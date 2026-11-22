//! ブラウザ (wasm32) 専用の入出力。
//!
//! - ネイティブの axum WebSocket サーバーの代わりに、JS から `send_command` / `on_response`
//!   を呼ぶか、親ページから `postMessage` でコマンドを受け取る。
//!   コマンドは実機と同じ `serde_json::from_str::<WsCommand>` でパースし、
//!   `server::setup_websocket_server` が作ったのと同じ mpsc に入れる
//!   (= `handle_websocket_commands` がそのまま処理する)。
//! - ebb (config.toml / presets/*.csv) は OPFS (Origin Private File System,
//!   `navigator.storage.getDirectory()`) から読む。構成は実機の `~/ebb` と同じ。
//!   無い・読めない・パースできないときは `example/` の内容で起動し、理由を console に出す。
//! - フォント (assets/fonts/ipag.ttf) は wasm に埋め込む。assets/ を HTTP で配信しなくても文字が出る。
//!
//! OPFS のパス規則 (OPFS のルートからの相対パス):
//!   `?device=X` あり: `X/ebb/config.toml`、`X/ebb/presets/<プリセット名>.csv`
//!   `?device` なし  : `ebb/config.toml`、  `ebb/presets/<プリセット名>.csv`
//!   (同一オリジンの iframe で台ごとに分けるため。`ebb_dir()` で現在のディレクトリを返す)
//!
//! URL クエリ:
//!   `?device=A`  台名 (親ページへの postMessage の `device` にも使う)
//!   `?scale=0.5` 描画解像度の倍率 (既定 0.5。1 / 0.5 / 0.333 など 0 < scale <= 1)
//!
//! 起動の流れ: OPFS の読込は非同期なので、`main` の 1 回目の呼び出しで `load_ebb` が読込を開始して
//! `None` を返し (main はそのまま return)、読み終わったら `crate::main()` をもう一度呼ぶ。
//! 2 回目は読んだ内容を返し、main はネイティブと同じ流れで App を組み立てて run する。

use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::sync::OnceLock;

use bevy::prelude::*;
use bevy::window::WindowResolution;
use tokio::sync::{broadcast, mpsc};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{FileSystemDirectoryHandle, FileSystemFileHandle};

use crate::loader::{Config, PresetManager, TextSource};
use crate::server::{CommandSender, ResponseBroadcaster, WsCommand, WsResponse};

const DEFAULT_CONFIG: &str = include_str!("../example/config.toml");
const DEFAULT_PRESETS: &[(&str, &str)] =
    &[("example", include_str!("../example/presets/example.csv"))];
const EMBEDDED_FONT: &[u8] = include_bytes!("../assets/fonts/ipag.ttf");

const DEFAULT_SCALE: f32 = 0.5;

static COMMAND_TX: OnceLock<mpsc::Sender<WsCommand>> = OnceLock::new();

thread_local! {
    static RESPONSE_CALLBACKS: RefCell<Vec<js_sys::Function>> = const { RefCell::new(Vec::new()) };
    /// 起動時に読んだ ebb の状況 (親ページへの ready 通知用)
    static EBB_STATUS: RefCell<String> = const { RefCell::new(String::new()) };
    /// OPFS から読み終えた ebb (main の 2 回目の呼び出しで取り出す)
    static LOADED_EBB: RefCell<Option<(PresetManager, Config)>> = const { RefCell::new(None) };
}

// ---------------------------------------------------------------------------
// JS 公開 API
// ---------------------------------------------------------------------------

/// 実機の WebSocket に送るのと同じ JSON 文字列を渡す。
/// パースに失敗した (= 実機では無視される) 場合は Err で理由を返す。
#[wasm_bindgen]
pub fn send_command(json: &str) -> Result<(), JsValue> {
    let command = serde_json::from_str::<WsCommand>(json).map_err(|e| {
        JsValue::from_str(&format!(
            "Failed to parse WebSocket message: {e} (実機ではこのコマンドは無視されます)"
        ))
    })?;
    let tx = COMMAND_TX
        .get()
        .ok_or_else(|| JsValue::from_str("nex-board がまだ起動していません"))?;
    tx.try_send(command)
        .map_err(|e| JsValue::from_str(&format!("Failed to send command to Bevy: {e}")))
}

/// 応答 (WsResponse) を JSON 文字列で受け取るコールバックを登録する。
#[wasm_bindgen]
pub fn on_response(cb: js_sys::Function) {
    RESPONSE_CALLBACKS.with(|c| c.borrow_mut().push(cb));
}

/// この台の台名 (`?device=`)。指定が無ければ空文字。
#[wasm_bindgen]
pub fn device() -> String {
    query_param("device").unwrap_or_default()
}

/// この台が ebb を読む OPFS のディレクトリ (`"A/ebb"` または `"ebb"`)。
#[wasm_bindgen]
pub fn ebb_dir() -> String {
    ebb_dir_parts().join("/")
}

// ---------------------------------------------------------------------------
// main.rs から呼ぶもの
// ---------------------------------------------------------------------------

/// 論理解像度 `window_width × text_size` (config 値) を `?scale=` 倍で描画する。
pub fn window_resolution(conf: &Config) -> WindowResolution {
    let mut resolution = WindowResolution::default();
    resolution.set_scale_factor_override(Some(scale()));
    resolution.set(conf.window_width, conf.text_size);
    resolution
}

/// OPFS から ebb を読む。
/// 1 回目は非同期の読込を始めて `None` を返し、読込後に `crate::main()` を呼び直す。
/// 2 回目はその結果を返す。
pub fn load_ebb() -> Option<(PresetManager, Config)> {
    if let Some(ebb) = LOADED_EBB.with(|c| c.borrow_mut().take()) {
        return Some(ebb);
    }
    wasm_bindgen_futures::spawn_local(async {
        let ebb = load_ebb_from_opfs().await;
        LOADED_EBB.with(|c| *c.borrow_mut() = Some(ebb));
        crate::main();
    });
    None
}

/// `server::setup_websocket_server` の後に呼ぶ。
/// 同じ mpsc / broadcast を JS 側につなぎ、埋め込みフォントを使うようにする。
pub fn setup(app: &mut App) {
    let command_tx = app.world().resource::<CommandSender>().sender.clone();
    let _ = COMMAND_TX.set(command_tx);
    let response_rx = app
        .world()
        .resource::<ResponseBroadcaster>()
        .sender
        .subscribe();
    app.insert_resource(WebResponseReceiver(response_rx));
    app.add_systems(PostUpdate, forward_responses);
    // main.rs の setup (Startup) が asset_server.load("fonts/ipag.ttf") で作ったハンドルを、
    // 埋め込みフォントのハンドルに差し替える
    app.add_systems(PostStartup, use_embedded_font);

    install_message_listener();
    let status = EBB_STATUS.with(|s| s.borrow().clone());
    post_to_parent("ready", &status, None);
}

// ---------------------------------------------------------------------------
// ebb (OPFS)
// ---------------------------------------------------------------------------

fn ebb_dir_parts() -> Vec<String> {
    match query_param("device").filter(|d| !d.is_empty()) {
        Some(d) => vec![d, "ebb".to_string()],
        None => vec!["ebb".to_string()],
    }
}

async fn load_ebb_from_opfs() -> (PresetManager, Config) {
    let dir_name = ebb_dir();
    let mut notes: Vec<String> = Vec::new();

    let ebb = match open_ebb_dir().await {
        Ok(d) => d,
        Err(e) => {
            notes.push(format!("OPFS の {dir_name}/ を開けません: {e}"));
            None
        }
    };
    if ebb.is_none() && notes.is_empty() {
        notes.push(format!("OPFS に {dir_name}/ がありません"));
    }

    // --- config.toml ---
    let stored_config = match &ebb {
        Some(dir) => match read_text(dir, "config.toml").await {
            Ok(Some(text)) => Some(text),
            Ok(None) => {
                notes.push(format!("{dir_name}/config.toml がありません"));
                None
            }
            Err(e) => {
                notes.push(format!("{dir_name}/config.toml を読めません: {e}"));
                None
            }
        },
        None => None,
    };
    let mut config_source = "example".to_string();
    let mut conf = None;
    if let Some(text) = stored_config {
        match toml::from_str::<Config>(&text) {
            Ok(c) => {
                conf = Some(c);
                config_source = format!("opfs:{dir_name}/config.toml");
            }
            Err(e) => notes.push(format!("{dir_name}/config.toml のパースに失敗: {e}")),
        }
    }
    let conf = conf.unwrap_or_else(|| {
        toml::from_str::<Config>(DEFAULT_CONFIG).unwrap_or(Config {
            // loader::unwrap_conf と同じフォールバック値
            text_size: 1080.0,
            window_width: 1920.0,
            camera_offset: 0.0,
        })
    });

    // --- presets/*.csv ---
    let mut csv_files: Vec<(String, String)> = Vec::new();
    if let Some(dir) = &ebb {
        match read_presets(dir).await {
            Ok(Some(files)) => {
                if files.is_empty() {
                    notes.push(format!("{dir_name}/presets/ に .csv がありません"));
                }
                for (name, result) in files {
                    match result {
                        Ok(text) => csv_files.push((name, text)),
                        Err(e) => notes.push(format!("{dir_name}/presets/{name}.csv を読めません: {e}")),
                    }
                }
            }
            Ok(None) => notes.push(format!("{dir_name}/presets/ がありません")),
            Err(e) => notes.push(format!("{dir_name}/presets/ を読めません: {e}")),
        }
    }
    let mut presets = parse_presets(&csv_files, &mut notes);
    let mut presets_source = format!("opfs:{dir_name}/presets");
    if presets.is_empty() {
        let defaults: Vec<(String, String)> = DEFAULT_PRESETS
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect();
        presets = parse_presets(&defaults, &mut notes);
        presets_source = "example".to_string();
    }
    // loader::load_all_presets と同じ: 1つも無ければ default を追加
    if presets.is_empty() {
        notes.push("有効なプリセットが無いため default を追加".to_string());
        presets.insert(
            "default".to_string(),
            vec![
                TextSource {
                    content: "Default Demo Text 1".to_string(),
                    duration: 5.0,
                    color: "#FDE047".to_string(),
                    interval: None,
                },
                TextSource {
                    content: "Default Demo Text 2".to_string(),
                    duration: 5.0,
                    color: "#FDE047".to_string(),
                    interval: None,
                },
            ],
        );
    }

    for n in &notes {
        console_warn(&format!("[nex-board] {n}"));
    }
    let mut preset_names: Vec<&String> = presets.keys().collect();
    preset_names.sort();
    console_log(&format!(
        "[nex-board] ebb: config={config_source} presets={presets_source} {preset_names:?}"
    ));

    let status = serde_json::json!({
        "ebb_dir": dir_name,
        "config_source": config_source,
        "presets_source": presets_source,
        "config": {
            "text_size": conf.text_size,
            "window_width": conf.window_width,
            "camera_offset": conf.camera_offset,
        },
        "presets": preset_names,
        "notes": notes,
        "scale": scale(),
    });
    EBB_STATUS.with(|s| *s.borrow_mut() = status.to_string());

    (PresetManager { presets }, conf)
}

fn parse_presets(
    files: &[(String, String)],
    notes: &mut Vec<String>,
) -> HashMap<String, Vec<TextSource>> {
    let mut presets = HashMap::new();
    for (name, text) in files {
        match parse_csv(text) {
            Ok(texts) => {
                presets.insert(name.clone(), texts);
            }
            Err(e) => notes.push(format!("プリセット '{name}' の読込に失敗: {e}")),
        }
    }
    presets
}

/// loader::load_csv と同じ読み方 (has_headers(true) + TextSource へ deserialize)。
fn parse_csv(text: &str) -> Result<Vec<TextSource>, Box<dyn Error>> {
    let rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());
    let result: Vec<TextSource> = rdr
        .into_deserialize()
        .collect::<Result<Vec<TextSource>, csv::Error>>()?;
    Ok(result)
}

/// OPFS のルートから ebb ディレクトリを開く。無ければ Ok(None)。
async fn open_ebb_dir() -> Result<Option<FileSystemDirectoryHandle>, String> {
    let window = web_sys::window().ok_or("window がありません")?;
    let root: FileSystemDirectoryHandle = JsFuture::from(window.navigator().storage().get_directory())
        .await
        .map_err(js_error)?
        .unchecked_into();
    let mut dir = root;
    for part in ebb_dir_parts() {
        match sub_dir(&dir, &part).await? {
            Some(d) => dir = d,
            None => return Ok(None),
        }
    }
    Ok(Some(dir))
}

async fn sub_dir(
    dir: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<Option<FileSystemDirectoryHandle>, String> {
    match JsFuture::from(dir.get_directory_handle(name)).await {
        Ok(h) => Ok(Some(h.unchecked_into())),
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(js_error(e)),
    }
}

/// ファイルを文字列で読む。無ければ Ok(None)。
async fn read_text(dir: &FileSystemDirectoryHandle, name: &str) -> Result<Option<String>, String> {
    let handle: FileSystemFileHandle = match JsFuture::from(dir.get_file_handle(name)).await {
        Ok(h) => h.unchecked_into(),
        Err(e) if is_not_found(&e) => return Ok(None),
        Err(e) => return Err(js_error(e)),
    };
    let file: web_sys::File = JsFuture::from(handle.get_file())
        .await
        .map_err(js_error)?
        .unchecked_into();
    let text = JsFuture::from(file.text()).await.map_err(js_error)?;
    Ok(Some(text.as_string().unwrap_or_default()))
}

/// presets/ の *.csv を読む。presets/ が無ければ Ok(None)。
async fn read_presets(
    ebb: &FileSystemDirectoryHandle,
) -> Result<Option<Vec<(String, Result<String, String>)>>, String> {
    let Some(dir) = sub_dir(ebb, "presets").await? else {
        return Ok(None);
    };
    // FileSystemDirectoryHandle.keys() は非同期イテレータ
    let iter = dir.keys();
    let mut names = Vec::new();
    loop {
        let step = JsFuture::from(iter.next().map_err(js_error)?)
            .await
            .map_err(js_error)?;
        let done = js_sys::Reflect::get(&step, &"done".into())
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if done {
            break;
        }
        if let Some(name) = js_sys::Reflect::get(&step, &"value".into())
            .ok()
            .and_then(|v| v.as_string())
        {
            if let Some(stem) = name.strip_suffix(".csv") {
                names.push(stem.to_string());
            }
        }
    }
    names.sort();
    let mut files = Vec::new();
    for name in names {
        let result = match read_text(&dir, &format!("{name}.csv")).await {
            Ok(Some(t)) => Ok(t),
            Ok(None) => Err("ファイルではありません".to_string()),
            Err(e) => Err(e),
        };
        files.push((name, result));
    }
    Ok(Some(files))
}

fn is_not_found(e: &JsValue) -> bool {
    // NotFoundError: 無い / TypeMismatchError: ファイルとディレクトリの取り違え
    matches!(
        js_sys::Reflect::get(e, &"name".into())
            .ok()
            .and_then(|v| v.as_string())
            .as_deref(),
        Some("NotFoundError") | Some("TypeMismatchError")
    )
}

fn js_error(e: JsValue) -> String {
    let name = js_sys::Reflect::get(&e, &"name".into())
        .ok()
        .and_then(|v| v.as_string());
    let message = js_sys::Reflect::get(&e, &"message".into())
        .ok()
        .and_then(|v| v.as_string());
    match (name, message) {
        (Some(n), Some(m)) => format!("{n}: {m}"),
        _ => e.as_string().unwrap_or_else(|| format!("{e:?}")),
    }
}

fn console_log(msg: &str) {
    web_sys::console::log_1(&JsValue::from_str(msg));
}

fn console_warn(msg: &str) {
    web_sys::console::warn_1(&JsValue::from_str(msg));
}

// ---------------------------------------------------------------------------
// 内部
// ---------------------------------------------------------------------------

/// 埋め込みフォントを Assets<Font> に入れ、Fonts のハンドルを差し替える。
/// (asset_server.load("fonts/ipag.ttf") の HTTP 読込は使われなくなる)
fn use_embedded_font(mut fonts: ResMut<crate::Fonts>, mut font_assets: ResMut<Assets<Font>>) {
    match Font::try_from_bytes(EMBEDDED_FONT.to_vec()) {
        Ok(font) => fonts.text_font.font = font_assets.add(font),
        Err(e) => console_warn(&format!("[nex-board] 埋め込みフォントを読めません: {e}")),
    }
}

#[derive(Resource)]
struct WebResponseReceiver(broadcast::Receiver<WsResponse>);

/// broadcast された WsResponse を JS コールバックと親ページへ流す。
fn forward_responses(mut rx: ResMut<WebResponseReceiver>) {
    loop {
        match rx.0.try_recv() {
            Ok(response) => {
                let Ok(json) = serde_json::to_string(&response) else {
                    continue;
                };
                RESPONSE_CALLBACKS.with(|cbs| {
                    let arg = JsValue::from_str(&json);
                    for cb in cbs.borrow().iter() {
                        let _ = cb.call1(&JsValue::NULL, &arg);
                    }
                });
                post_to_parent("response", &json, None);
            }
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
}

fn query_param(name: &str) -> Option<String> {
    let search = web_sys::window()?.location().search().ok()?;
    web_sys::UrlSearchParams::new_with_str(&search)
        .ok()?
        .get(name)
}

fn scale() -> f32 {
    query_param("scale")
        .and_then(|s| s.parse::<f32>().ok())
        .filter(|s| *s > 0.0 && *s <= 1.0)
        .unwrap_or(DEFAULT_SCALE)
}

fn own_origin() -> Option<String> {
    web_sys::window()?.location().origin().ok()
}

/// 親ページ (同一オリジン) へ `{source:"nexboard", device, kind, json, id?}` を送る。
fn post_to_parent(kind: &str, json: &str, id: Option<&JsValue>) {
    let Some(window) = web_sys::window() else {
        return;
    };
    let Ok(Some(parent)) = window.parent() else {
        return;
    };
    let Some(origin) = own_origin() else { return };
    let msg = js_sys::Object::new();
    let set = |k: &str, v: &JsValue| {
        let _ = js_sys::Reflect::set(&msg, &JsValue::from_str(k), v);
    };
    set("source", &JsValue::from_str("nexboard"));
    set("device", &JsValue::from_str(&device()));
    set("kind", &JsValue::from_str(kind));
    set("json", &JsValue::from_str(json));
    if let Some(id) = id {
        set("id", id);
    }
    let _ = parent.post_message(&msg, &origin);
}

/// 親ページからの `{type:"command", json, id?}` を受け付ける (同一オリジンのみ)。
fn install_message_listener() {
    let Some(window) = web_sys::window() else {
        return;
    };
    let closure = Closure::<dyn FnMut(web_sys::MessageEvent)>::new(|event: web_sys::MessageEvent| {
        if Some(event.origin()) != own_origin() {
            return;
        }
        let data = event.data();
        let get = |k: &str| js_sys::Reflect::get(&data, &JsValue::from_str(k)).ok();
        if get("type").and_then(|v| v.as_string()).as_deref() != Some("command") {
            return;
        }
        let id = get("id").filter(|v| !v.is_undefined());
        let Some(json) = get("json").and_then(|v| v.as_string()) else {
            post_to_parent("error", "json が文字列ではありません", id.as_ref());
            return;
        };
        match send_command(&json) {
            Ok(()) => post_to_parent("accepted", &json, id.as_ref()),
            Err(e) => {
                let reason = e.as_string().unwrap_or_else(|| format!("{e:?}"));
                post_to_parent("error", &reason, id.as_ref());
            }
        }
    });
    let _ = window
        .add_event_listener_with_callback("message", closure.as_ref().unchecked_ref());
    closure.forget();
}

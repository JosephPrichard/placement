use crate::backend::models::{DrawMsg, Input, ServiceError};
use crate::backend::models::TextOutput;
use crate::backend::database::External;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{ConnectInfo, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use axum::http::HeaderMap;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tracing::log::{error, info, warn};

pub type SocketReader = SplitStream<WebSocket>;
pub type SocketWriter = Arc<Mutex<SplitSink<WebSocket, Message>>>;

#[derive(Clone)]
pub struct ServerState<E: External> {
    pub broadcast_tx: broadcast::Sender<DrawMsg>,
    pub external: E
}

async fn handle_recv(external: impl External + 'static, mut rx: SocketReader, tx: SocketWriter, ip: IpAddr) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Starting the recv loop for ip={}", ip);
        while let Some(result) = rx.next().await {
            let in_msg = match result {
                Ok(msg) => msg,
                Err(err) => {
                    error!("Failed to recv a buffer from socket: {}", err);
                    continue;
                },
            };
            let in_str = match in_msg.to_text() {
                Ok(text) => text,
                Err(err) => {
                    error!("Failed to convert binary buffer to utf8 encoding: {}", err);
                    continue;
                },
            };
            let input: Input = match serde_json::from_str(in_str) {
                Ok(structure) => structure,
                Err(err) => {
                    error!("Failed to deserialize an input structure from recv buffer: {}", err);
                    continue;
                },
            };
            let result = handle_input(external.clone(), tx.clone(), ip, input).await;
            if let Err(err) = result {
                error!("Error occurred while processing an input: {:?}", err)
            }
        }
    })
}

async fn handle_input(external: impl External + Clone, tx: SocketWriter, ip: IpAddr, input: Input) -> Result<(), ServiceError> {
    match input {
        Input::DrawTile(draw) => {
            info!("Received a draw_msg={:?} input", draw);
            external.draw_tile(draw, ip).await
        },
        Input::GetGroup(key) => {
            info!("Received a get_group={:?} input", key);
            let buffer = external.get_group(key).await?;

            let mut tx = tx.lock().await;
            tx.send(Message::from(buffer)).await.unwrap();
            Ok(())
        },
        Input::GetTileInfo(point) => {
            info!("Received a get_tile_info={:?} input", point);
            let str = external.get_tile_info(point).await?;

            let mut tx = tx.lock().await;
            info!("Sending str={} over socket", str);
            tx.send(Message::from(str)).await.unwrap();

            Ok(())
        },
    }
}

async fn handle_send(broadcast_tx: broadcast::Sender<DrawMsg>, tx: Arc<Mutex<SplitSink<WebSocket, Message>>>, ip: IpAddr) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Starting the send loop for ip={}", ip);
        let mut rx = broadcast_tx.subscribe();
        loop {
            let draw = match rx.recv().await {
                Ok(draw) => draw,
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(count)) => {
                    warn!("Broadcast receiver lagged, missing {} messages", count);
                    continue;
                }
            };
            
            let output = TextOutput::DrawEvent(draw);
            let str = match serde_json::to_string(&output) {
                Ok(buffer) => buffer,
                Err(err) => {
                    error!("Failed to serialize a draw_msg={:?}: {}", draw, err);
                    continue;
                }
            };
            
            let mut tx = tx.lock().await;
            info!("Sending str={} over socket", str);
            if let Err(err) = tx.send(Message::from(str)).await {
                error!("Failed to send serialized draw msg over socket: {}", err);
            }
        }
    })
}

pub async fn ws_handler<E: External + 'static>(ws: WebSocketUpgrade, headers: HeaderMap, State(server): State<ServerState<E>>, ConnectInfo(addr): ConnectInfo<SocketAddr>) -> impl IntoResponse {
    let ip = match headers.get("X-Forwarded-For") {
        None => addr.ip(),
        Some(header) => {
            let ip_str = match header.to_str() {
                Ok(ip_str) => ip_str,
                Err(_) => return String::from("Failed to convert X-Forwarded-For header to string").into_response()
            };
            match IpAddr::from_str(ip_str) {
                Ok(ip) => ip,
                Err(_) => return String::from("Failed to parse ip address in X-Forwarded-For header").into_response()
            }
        }
    };
    ws.on_upgrade(move |socket| async move {
        info!("Websocket connection established with server for ip={}", ip);

        let (tx, rx) = socket.split();
        let tx = Arc::new(Mutex::new(tx));
        let mut listener = handle_recv(server.external.clone(), rx, tx.clone(), ip).await;
        let mut sender = handle_send(server.broadcast_tx.clone(), tx, ip).await;

        tokio::select! {
            _ = &mut listener => {
                info!("Listener task ended, aborting sender task for ip={}", ip);
                sender.abort()
            },
            _ = &mut sender => {
                error!("Sender task ended, this should not happen for ip={}", ip);
                listener.abort()
            }
        }
    })
}

mod test {
    use std::future::Future;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;
    use axum::Router;
    use axum::routing::get;
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::broadcast;
    use tokio::time::sleep;
    use tokio_tungstenite::tungstenite::Message;
    use tracing::log::info;
    use crate::backend::database::External;
    use crate::backend::models::{DrawMsg, GroupKey, ServiceError};
    use crate::backend::ws::{ws_handler, ServerState};

    // #[derive(Clone)]
    // struct MockExternal {}
    // 
    // impl External for MockExternal {
    //     fn draw_tile(&self, draw: DrawMsg, ip: IpAddr) -> impl Future<Output=Result<(), ServiceError>> + Send {
    //         async { Ok(()) }
    //     }
    // 
    //     fn get_group(&self, key: GroupKey) -> impl Future<Output=Result<Vec<u8>, ServiceError>> + Send {
    //         async move {
    //             if key == GroupKey(0, 0) {
    //                 Ok(vec![0, 0, 0])
    //             } else {
    //                panic!("No mock exists in get_group for key={:?}", key)
    //             }
    //         }
    //     }
    // 
    //     fn get_tile_info(&self, point: (i32, i32)) -> impl Future<Output=Result<String, ServiceError>> + Send {
    //         async move {
    //             if point == (0, 0) {
    //                 Ok(String::from("output"))
    //             } else {
    //                 panic!("No mock exists in get_tile_info for point={:?}", point)
    //             }
    //         }
    //     }
    // }
    // 
    // #[tokio::test]
    // async fn integration_test() {
    //     let listener = tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 412)))
    //         .await
    //         .unwrap();
    //     let addr = listener.local_addr().unwrap();
    // 
    //     let (tx, _) = broadcast::channel(100);
    //     let state = ServerState { broadcast_tx: tx, external: MockExternal{} };
    // 
    //     let router = Router::new()
    //         .route("/testable", get(ws_handler))
    //         .with_state(state);
    //     tokio::spawn(async move { axum::serve(listener, router) });
    // 
    //     tokio::spawn(async move {
    //         let pairs = vec![
    //             (Message::text("{ \"DrawTile\": { \"x\": 0, \"y\": 0, \"rgb\": [1, 2, 3] } }"), 
    //              Message::text("{ \"DrawEvent\": { \"x\": 0, \"y\": 0, \"rgb\": [1, 2, 3] } }")),
    //             (Message::text("{ \"GetTileInfo\": { \"x\": 0, \"y\": 0, \"rgb\": [1, 2, 3] } } } "),
    //              Message::text("{ \"TileInfo\": [0, 0] } ")),
    //             (Message::text("{ \"GetGroup\": [0, 0] } "),
    //              ),
    //         ];
    //         
    //         let (mut socket, _) =
    //             tokio_tungstenite::connect_async(format!("ws://{}/testable", addr))
    //                 .await
    //                 .unwrap();
    // 
    //         for input in pairs {
    //             socket.send(input).await.unwrap();
    //         }
    //         for output in outputs {
    //             let msg1 = socket.next().await.unwrap().unwrap();
    //         }
    //     });
    // }
}
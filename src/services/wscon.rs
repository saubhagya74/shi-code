use std::{ collections::HashMap, net::SocketAddr};

use axum::{body::Bytes, extract::{ConnectInfo, Query, State, WebSocketUpgrade, ws::{Message, WebSocket}}, http::{HeaderMap, StatusCode}, response::IntoResponse};
use serde::Deserialize;
use crate::my_states::{AppState, UserConnection};
use futures_util::{SinkExt, StreamExt};
use crate::ws_controllers::my_ws_router;

pub async fn ws_handler(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(master_state):State<AppState>,
    #[allow(unused_variables)]
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    ws:WebSocketUpgrade
)-> impl IntoResponse
{
    // let id=headers.get("userid")
    // .and_then(|value| value.to_str().ok())
    // .and_then(|value| value.parse::<u64>().ok())
    // .unwrap_or_else(||{
    //     0
    // });// id from jwt
    let id = (*params.get("userid").unwrap()).parse().unwrap();
    if id==0 {return StatusCode::BAD_REQUEST.into_response();}

    return ws.on_upgrade(move |socket|{
        handle_socket(socket,master_state,addr,id)
    });
}
pub async fn handle_socket(socket: WebSocket,mstate: AppState, addr: SocketAddr,id:u64) {

    println!("Client Connected: {:?} with id {}",addr,id);

    let (mut sender, mut receiver) = futures_util::StreamExt::split(socket);

    let (tx, mut rx) = tokio::sync::mpsc
    ::unbounded_channel::<Bytes>();
    
    let conn=UserConnection{
        socket_addr: addr,
        tx
    };

    {
        let lock_client = mstate.ws_clients.clients.write();
        lock_client.insert(id, conn);
    }
    // let s_state=state.clients.clone();
    
    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(Message::Binary(msg)).await.is_err() {
                break;
            }
        }
    });
    //there is sender and receiver for each user
    let r_state=mstate.clone();
    //use dashmap
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            // println!("received: {:?}", msg);
            match msg{
                Message::Binary(bin_data)=>{
                    println!("{:?}",bin_data);
                    if let Ok(ws_msg)=serde_json::from_slice::<WsEnvelope>(&bin_data){

                        let token_id_str = ws_msg.token.accesstoken;

                        if let Ok(extracted_id) = token_id_str.parse::<u64>() {
                            println!("Extracted ID from token: {}", extracted_id);
                            
                            my_ws_router::ws_router(
                                ws_msg.action, 
                                ws_msg.payload, 
                                r_state.clone(), 
                                extracted_id
                            ).await;
                        } else {
                            eprintln!("Failed to parse token '{}' into a u64", token_id_str);
                        }
                    }
                },
                Message::Text(text)=>{
                    
                    let bin_data=axum::body::Bytes::from(text);
                    println!("{:?}",bin_data);
                    if let Ok(ws_msg)=serde_json::from_slice::<WsEnvelope>(&bin_data){

                        let token_id_str = ws_msg.token.accesstoken;

                        if let Ok(extracted_id) = token_id_str.parse::<u64>() {
                            println!("Extracted ID from token: {}", extracted_id);
                            
                            my_ws_router::ws_router(
                                ws_msg.action, 
                                ws_msg.payload, 
                                r_state.clone(), 
                                extracted_id
                            ).await;
                        } else {
                            eprintln!("Failed to parse token '{}' into a u64", token_id_str);
                        }
                    }
                },
                Message::Ping(ping)=>{
                    println!("got ping, {:?}",ping);
                },
                Message::Pong(text)=>{
                    println!("got pong, {:?}",text);
                },
                Message::Close(close)=>{
                    println!("got closed, {:?}",Some(close));//some wtf??unwrap
                },
                #[allow(unreachable_patterns)]
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    };
}
#[derive(Deserialize,Debug)]
struct WsEnvelope<'a> {
    #[serde(borrow)]
    token: TokenData<'a>,
    action: &'a str,
    #[serde(borrow)]
    payload: &'a serde_json::value::RawValue,
}

#[derive(Deserialize,Debug)]
struct TokenData<'a> {
    accesstoken: &'a str,
}//payload lai ni same estai garne
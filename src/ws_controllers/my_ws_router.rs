use serde_json::value;

use crate::{my_states::AppState, ws_controllers::{self, group::{self, create_group::WSCreateGroupPayload, send_message::WSGroupMessagePayload}, private_com::u2u_com::WSMessagePrivatePayload, requests::handle_request::WSRequestPayload}};

pub async fn ws_router(
    action: &str, 
    raw_payload: &value::RawValue,
    state: AppState,
    user_id :u64
){
     match action {
        "sendMessagePrivate" => {
            send_message_private(raw_payload,state,user_id).await;
        },
        "createGroup" => {
            create_group_handler(raw_payload, state, user_id).await;
        },
        "sendRequest" => {
            request_handler(raw_payload, state, user_id).await;
        },
        "sendGroupMessage" => {
            group_message_handler(raw_payload, state, user_id).await;
        }
        _ => {
            println!("invald action: {}", action);
        }
    }
}
pub async fn group_message_handler(
    raw_payload: &serde_json::value::RawValue, 
    state: AppState, 
    user_id: u64
) {
    match serde_json::from_str::<WSGroupMessagePayload>(raw_payload.get()) {
        Ok(payload) => {
            ws_controllers::group::send_message::send_group_msg_to_tx(payload, state, user_id).await;
        }
        Err(e) => eprintln!("group message parsing failed: {:?}", e),
    }
}
pub async fn request_handler(raw_payload: &serde_json::value::RawValue, state: AppState, user_id: u64) {
    match serde_json::from_str::<WSRequestPayload>(raw_payload.get()) {
        Ok(payload) => {
            ws_controllers::requests::handle_request::send_request_to_tx(payload, state, user_id).await;
        }
        Err(e) => eprintln!("parsing request failed: {:?}", e),
    }
}
pub async fn send_message_private(
    raw_payload: &value::RawValue,
    state: AppState,
    user_id :u64
){
    println!("called sendMessagePrivate");
    match serde_json::from_str::<WSMessagePrivatePayload>(raw_payload.get()) {
        Ok(payload) => {
            println!("payloadis: {:?}",payload);
            ws_controllers::private_com::u2u_com::send_message_private(payload, state.clone(), user_id).await;
        }
        Err(e) => {
            println!("parsing failed: {:?}", e);
        }
    }
}
pub async fn create_group_handler(
    raw_payload: &value::RawValue,
    state: AppState,
    user_id: u64
) {
    println!("called createGroup");
    match serde_json::from_str::<WSCreateGroupPayload>(raw_payload.get()) {
        Ok(payload) => {
            // We pass user_id as the creator to ensure the person making the request is the owner
            group::create_group::create_group(
                payload, 
                state, 
                user_id
            ).await;
        }
        Err(e) => {
            println!("parsing createGroup failed: {:?}", e);
        }
    }
}
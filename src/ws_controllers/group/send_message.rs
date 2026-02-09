use serde::Deserialize;
use axum::body::Bytes;

use crate::{my_states::AppState, ws_controllers::{private_com::u2u_com::ContentLabel}};


pub async fn send_group_msg_to_tx(
    payload: WSGroupMessagePayload<'_>, 
    state: AppState, 
    user_id: u64
) {
    // Clean quotes from RawValue and convert to Bytes
    // let description_str = payload.description.get().trim_matches('"').to_string();
    // let des_bytes = Bytes::copy_from_slice(description_str.as_bytes());

    let chat_id = payload.chat_id.parse::<u64>().unwrap_or(0);
    if chat_id == 0 { return; }
    
    let m_id = {
        let mut idbuck = state.services.bucket_id.lock();
        idbuck.get_id() as i64
    };
    
    let my_timestamp=chrono::Utc::now().timestamp();
    
    let des_bytes=Bytes::copy_from_slice((payload.description).get().as_bytes());

    let db_rec = WSGroupMessageDB {
        sender_id: (user_id as i64).clone(),
        message_id: m_id .clone(),
        chat_id: (chat_id as i64).clone(),
        content_type: payload.content_type.clone(),
        description: des_bytes.clone(),
        created_at: my_timestamp
    };

    if let Err(e) = state.ws_txes.tx_db_g_message.send(db_rec) {
        eprintln!("failed to send group message to channel: {}", e);
    }

    let forward_msg = GroupForwardMessage {
        group_id: chat_id as i64,
        sender_id: user_id as i64,
        message_id: m_id, // Generated from bucket_id
        content_type: payload.content_type,
        description: des_bytes,
    };

    let _ = state.ws_txes.tx_db_fgm.send(forward_msg);
}

#[derive(Deserialize, Debug)]
pub struct WSGroupMessagePayload<'a> {
    pub chat_id: String,
    pub content_type: ContentLabel,
    #[serde(borrow)]
    pub description: &'a serde_json::value::RawValue,
}

#[derive(Clone, Debug)]
pub struct GroupForwardMessage{
    pub group_id: i64,
    pub sender_id: i64,
    pub message_id: i64,
    pub content_type: ContentLabel,
    pub description: Bytes
}

pub struct WSGroupMessageDB {
    pub sender_id: i64,
    pub message_id: i64,
    pub chat_id: i64,
    pub content_type: ContentLabel,
    pub description: Bytes,
    pub created_at: i64,
}
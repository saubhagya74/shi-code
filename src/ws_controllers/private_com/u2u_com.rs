use axum::body::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::value;

use crate::my_states::AppState;

pub async fn send_message_private<'a>(
    payload: WSMessagePrivatePayload<'a>,
    state: AppState,
    user_id: u64,
) {
    let receiver_id = payload.receiver_id.parse::<u64>().unwrap_or(0);
    if receiver_id == 0 { return; }

    let description_str = payload.description.get().trim_matches('"').to_string();
    let des_bytes = axum::body::Bytes::copy_from_slice(description_str.as_bytes());
    // let des_bytes=Bytes::copy_from_slice(payload.description.get().as_bytes());
    let forward_payload = serde_json::json!({
        "action": "newMessage",
        "payload": {
            "chat_id": payload.chat_id,
            "description": description_str,
            "messaged_at": chrono::Utc::now().to_rfc3339(),
            "sender_id": user_id.to_string()
        }
    });

    let response_bytes = Bytes::from(forward_payload.to_string());

    if let Some(uc) = state.ws_clients.clients.read().get(&receiver_id) {
        // Now you are sending a JSON string inside the binary message
        let _ = uc.tx.send(response_bytes);
    }

    let db_rec = MessagePrivateDB {
        sender_id: user_id,
        receiver_id: receiver_id,
        content_type: payload.content_type,
        description: des_bytes,
        created_at: chrono::Utc::now().timestamp(),
    };

    let _ = state.ws_txes.tx_db_batch_private.send(db_rec);
}
#[derive(Deserialize, Debug)]
pub struct WSMessagePrivatePayload<'a> {
    pub receiver_id: String,
    pub chat_id: Option<String>,
    pub content_type: ContentLabel,
    #[serde(borrow)]
    pub description: &'a value::RawValue,
}
pub struct MessagePrivateDB{
   pub sender_id : u64,
   pub receiver_id:u64,
   pub content_type: ContentLabel,
   pub description: Bytes,
   pub created_at: i64
}
#[allow(non_camel_case_types)]
#[derive(Debug, Serialize, Deserialize, sqlx::Type, Clone, Copy)]
#[sqlx(type_name = "content_label")]
pub enum ContentLabel {
    text,
    video,
    audio,
    image,
    file,
}
#[allow(unused)]
#[allow(non_camel_case_types)]
#[derive(Debug, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "compression_label")]
pub enum CompressionLabel {
    lz4,
    gzip,
    zstd,
    none
}
#[allow(unused)]
#[allow(non_camel_case_types)]
#[derive(Debug, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "encryption_label")]
pub enum EncryptionLabel {
    ecc,
    rsa,
    none
}

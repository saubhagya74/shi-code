use serde::{Deserialize, Serialize};

use crate::my_states::AppState;

pub async fn send_request_to_tx(payload: WSRequestPayload, state: AppState, user_id: u64) {
    let db_rec = WSRequestDB {
        sender_id: user_id as i64,
        receiver_id: payload.receiver_id as i64,
        status: payload.status,
        created_at: chrono::Utc::now(),
    };

    if let Err(e) = state.ws_txes.tx_db_request.send(db_rec) {
        eprintln!("failed to send request to batcher: {}", e);
    }
}
#[derive(Deserialize, Debug)]
pub struct WSRequestPayload {
    pub receiver_id: u64,
    pub status: RequestStatus,
}
#[allow(non_camel_case_types)]
#[derive(Debug, Serialize, Deserialize, sqlx::Type, Clone, Copy)]
#[sqlx(type_name = "request_status_type")]
pub enum RequestStatus {
    pending,
    accepted,
    declined,
}
pub struct WSRequestDB {
    pub sender_id: i64,
    pub receiver_id: i64,
    pub status: RequestStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
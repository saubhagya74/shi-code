use tokio::time::{Instant, timeout, Duration};
use crate::AppState; // Import AppState
use crate::ws_controllers::requests::handle_request::WSRequestDB; // Import WSRequestDB

pub async fn db_batcher_requests(
    state: AppState,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<WSRequestDB>,
) {
    loop {
        let start = Instant::now();
        let limit = Duration::from_millis(15000);

        let mut request_ids = Vec::new();
        let mut sender_ids = Vec::new();
        let mut receiver_ids = Vec::new();
        let mut statuses = Vec::new();
        let mut created_ats = Vec::new();
        let mut notification_ids = Vec::new();

        while start.elapsed() < limit {
            let time_remaining = limit.saturating_sub(start.elapsed());
            match timeout(time_remaining, rx.recv()).await {
                Ok(Some(db_rec)) => {
                    let mut idbuck = state.services.bucket_id.lock();
                    request_ids.push(idbuck.get_id() as i64);
                    notification_ids.push(idbuck.get_id() as i64);

                    sender_ids.push(db_rec.sender_id);
                    receiver_ids.push(db_rec.receiver_id);
                    
                    // Fix: Force lowercase to match typical Postgres Enum definitions
                    statuses.push(format!("{:?}", db_rec.status).to_lowercase());
                    created_ats.push(db_rec.created_at);
                }
                Ok(None) => return,
                Err(_) => break,
            }
        }

        if !request_ids.is_empty() {
            let res = sqlx::query!(
                r#"
                WITH raw_data AS (
                    SELECT * FROM unnest(
                        $1::int8[], $2::int8[], $3::int8[], 
                        $4::text[], $5::timestamptz[], $6::int8[]
                    ) AS t(req_id, s_id, r_id, stat_str, c_at, n_id)
                ),
                upsert_requests AS (
                    INSERT INTO requests (request_id_, sender_id_, receiver_id_, status, requested_at)
                    SELECT req_id, s_id, r_id, stat_str::request_status_type, c_at FROM raw_data
                    ON CONFLICT (sender_id_, receiver_id_) 
                    DO UPDATE SET 
                        status = EXCLUDED.status,
                        requested_at = EXCLUDED.requested_at
                    RETURNING request_id_, sender_id_, receiver_id_, status, requested_at
                )
                INSERT INTO home (notification_id_, user_id_, notification_object_, created_at_, is_seen_, is_deleted_)
                SELECT 
                    rd.n_id,
                    CASE 
                        WHEN ur.status = 'pending' THEN ur.receiver_id_ 
                        WHEN ur.status = 'accepted' THEN ur.sender_id_  
                    END,
                    jsonb_build_object(
                        'type', 'friend_request',
                        'request_id', ur.request_id_,
                        'status', ur.status,
                        'from_user', ur.sender_id_
                    ),
                    ur.requested_at,
                    false,
                    false
                FROM upsert_requests ur
                JOIN raw_data rd ON ur.request_id_ = rd.req_id
                WHERE ur.status IN ('pending', 'accepted');
                "#,
                &request_ids,
                &sender_ids,
                &receiver_ids,
                &statuses,
                &created_ats,
                &notification_ids
            ).execute(&state.services.db_pool).await;

            if let Err(e) = res {
                eprintln!("error batch inserting requests/notifications: {:?}", e);
            }
        }
    }
}
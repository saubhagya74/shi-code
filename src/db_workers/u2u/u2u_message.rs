use tokio::time::Instant;

use crate::{my_states::AppState, ws_controllers::private_com::u2u_com::{MessagePrivateDB}};

pub async fn db_batcher_private(
    state: AppState,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<MessagePrivateDB>,
) {
    loop {
        let start = Instant::now();
        let limit = tokio::time::Duration::from_millis(20000);
        
        let mut message_ids = Vec::new();
        let mut sender_ids = Vec::new();
        let mut receiver_ids = Vec::new();
        let mut user_a_ids = Vec::new();
        let mut user_b_ids = Vec::new();
        let mut content_types = Vec::new();
        let mut descriptions = Vec::new();
        let mut trimmed_descriptions = Vec::new();
        let mut messaged_ats = Vec::new();
        let mut new_chat_ids = Vec::new();

        while start.elapsed() < limit {
            let time_remaining = limit.saturating_sub(start.elapsed());
            match tokio::time::timeout(time_remaining, rx.recv()).await {
                Ok(Some(db_rec)) => {

                    let (u_a, u_b) = if db_rec.sender_id > db_rec.receiver_id {
                        (db_rec.sender_id as i64, db_rec.receiver_id as i64)
                    } else {
                        (db_rec.receiver_id as i64, db_rec.sender_id as i64)
                    };

                    let messaged_at = chrono::DateTime::from_timestamp(db_rec.created_at, 0)
                        .unwrap_or_else(|| chrono::Utc::now());

                    let mut id_gen = state.services.bucket_id.lock();
                    message_ids.push(id_gen.get_id() as i64);
                    new_chat_ids.push(id_gen.get_id() as i64);
                    drop(id_gen);

                    let desc = String::from_utf8_lossy(&db_rec.description).into_owned();
                    let tri_desc = if desc.len() > 30 {
                        format!("{}...", &desc.chars().take(27).collect::<String>())
                    } else {
                        desc.clone()
                    };

                    sender_ids.push(db_rec.sender_id as i64);
                    receiver_ids.push(db_rec.receiver_id as i64);
                    user_a_ids.push(u_a);
                    user_b_ids.push(u_b);
                    content_types.push(format!("{:?}", db_rec.content_type)); //implements sqlx type??
                    descriptions.push(desc);
                    trimmed_descriptions.push(tri_desc);
                    messaged_ats.push(messaged_at);
                }
                Ok(None) => return,
                Err(_) => break,
            }
        }

       if !message_ids.is_empty() {
            let result = sqlx::query!(
                r#"
                WITH raw_data AS (
                    SELECT 
                        msg_id, s_id, r_id, new_c_id, descrip, tri_desc, m_at, u_a, u_b,
                        c_type_raw::content_label as c_type -- Cast text back to Enum here
                    FROM UNNEST(
                        $1::int8[], $2::int8[], $3::int8[], $4::int8[], 
                        $5::text[], $6::text[], $7::timestamptz[], $8::int8[], $9::int8[],
                        $10::text[] -- Pass the enums as a text array
                    ) AS t(msg_id, s_id, r_id, new_c_id, descrip, tri_desc, m_at, u_a, u_b, c_type_raw)
                ),
                upsert_convo AS (
                    INSERT INTO conversations (chat_id, user_a_id, user_b_id, last_message, last_message_time)
                    SELECT DISTINCT ON (u_a, u_b) 
                        new_c_id, u_a, u_b, tri_desc, m_at 
                    FROM raw_data
                    ON CONFLICT (user_a_id, user_b_id) 
                    DO UPDATE SET 
                        last_message = EXCLUDED.last_message,
                        last_message_time = EXCLUDED.last_message_time
                    RETURNING chat_id, user_a_id, user_b_id
                )
                INSERT INTO messages (message_id, chat_id, sender_id, receiver_id, description, messaged_at, content_type)
                SELECT 
                    r.msg_id, u.chat_id, r.s_id, r.r_id, r.descrip, r.m_at, r.c_type
                FROM raw_data r
                JOIN upsert_convo u ON r.u_a = u.user_a_id AND r.u_b = u.user_b_id;
                "#,
                &message_ids,
                &sender_ids,
                &receiver_ids,
                &new_chat_ids,
                &descriptions,
                &trimmed_descriptions,
                &messaged_ats,
                &user_a_ids,
                &user_b_ids,
                &content_types // This is now Vec<String>
            ).execute(&state.services.db_pool).await;

            if let Err(e) = result {
                eprintln!("Batch insert failed: {:?}", e);
            }
        }
    }
}
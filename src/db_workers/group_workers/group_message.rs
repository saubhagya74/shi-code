use tokio::time::{Instant, timeout, Duration};

use crate::{my_states::AppState, ws_controllers::group::{ send_message::WSGroupMessageDB}};

pub async fn db_batcher_group_messages(
    state: AppState,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<WSGroupMessageDB>,
) {
    loop {
        let start = Instant::now();
        let limit = Duration::from_millis(15000);

        let mut msg_ids = Vec::new();
        let mut chat_ids = Vec::new();
        let mut sender_ids = Vec::new();
        let mut content_types = Vec::new();
        let mut descriptions = Vec::new();
        let mut messaged_ats = Vec::new();

        while start.elapsed() < limit {
            let time_remaining = limit.saturating_sub(start.elapsed());
            match timeout(time_remaining, rx.recv()).await {
                Ok(Some(db_rec)) => {
                    msg_ids.push(db_rec.message_id);
                    chat_ids.push(db_rec.chat_id);
                    sender_ids.push(db_rec.sender_id);
                    content_types.push(format!("{:?}", db_rec.content_type));
                    descriptions.push(String::from_utf8_lossy(&(db_rec.description.clone())).into_owned());
                    messaged_ats.push(chrono::DateTime::from_timestamp(db_rec.created_at, 0)
                    .unwrap_or_else(|| chrono::Utc::now()));
                }
                Ok(None) => return,
                Err(_) => break,
            }
        }

        if !msg_ids.is_empty() {
            let res = sqlx::query!(
                r#"
                with raw_data as (
                    select 
                        m_id, c_id, s_id, c_type, description_text, m_at
                    from unnest(
                        $1::int8[], $2::int8[], $3::int8[], 
                        $4::text[], $5::text[], $6::timestamptz[]
                    ) as t(m_id, c_id, s_id, c_type, description_text, m_at)
                ),
                validated_data as (
                    -- join with group_members to ensure sender is allowed to post
                    select 
                        r.m_id, r.c_id, r.s_id, r.c_type, r.description_text, r.m_at 
                    from raw_data r
                    inner join group_members gm 
                    on r.c_id = gm.group_id and r.s_id = gm.member_id
                ),
                insert_msgs as (
                    insert into group_messages (
                        message_id, 
                        chat_id, 
                        sender_id, 
                        content_type, 
                        description, 
                        messaged_at
                    )
                    select 
                        m_id, 
                        c_id, 
                        s_id, 
                        c_type::content_label, 
                        description_text, 
                        m_at 
                    from validated_data
                    returning chat_id, description, messaged_at
                )
                -- update group conversation preview
                update group_conversations gc
                set 
                    last_message = substring(im.description from 1 for 30),
                    last_message_time = im.messaged_at
                from insert_msgs im
                where gc.chat_id = im.chat_id;
                "#,
                &msg_ids,
                &chat_ids,
                &sender_ids,
                &content_types,
                &descriptions,
                &messaged_ats
            ).execute(&state.services.db_pool).await;

            if let Err(e) = res {
                eprintln!("group message batch failed: {:?}", e);
            }
        }
    }
}
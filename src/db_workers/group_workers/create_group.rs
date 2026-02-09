use tokio::time::{Instant, timeout};

use crate::{my_states::AppState, ws_controllers::group::create_group::WSCreateGroupDB};



pub async fn create_group(
    state: AppState,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<WSCreateGroupDB>
) {
    loop {
        let start = Instant::now();
        let limit = tokio::time::Duration::from_millis(15000);

        let mut group_ids = Vec::new();
        let mut group_names = Vec::new();
        let mut creator_ids = Vec::new();
        let mut created_at_v = Vec::new();
        let mut initial_members_json = Vec::new();

        while start.elapsed() < limit {
            let time_remaining = limit.saturating_sub(start.elapsed());
            match timeout(time_remaining, rx.recv()).await {
                Ok(Some(db_rec)) => {
                    let g_id = {
                        let mut idbuck = state.services.bucket_id.lock();
                        idbuck.get_id() as i64
                    };

                    // Parse the bytes into a serde_json::Value
                    let m_json: serde_json::Value = serde_json::from_slice(&db_rec.initial_members)
                        .unwrap_or_else(|_| serde_json::json!([]));

                    group_ids.push(g_id);
                    group_names.push(db_rec.group_name.to_string());
                    creator_ids.push(db_rec.creator_id);
                    created_at_v.push(db_rec.created_at);
                    initial_members_json.push(m_json);
                }
                Ok(None) => break, 
                Err(_) => break,
            }
        }

        if !group_ids.is_empty() {
            let res = sqlx::query!(
                r#"
                with raw_data as (
                    select * from unnest(
                        $1::int8[],          -- g_ids
                        $2::text[],          -- names
                        $3::int8[],          -- creators
                        $4::timestamptz[],   -- dates
                        $5::jsonb[]          -- members_json
                    ) as t (g_id, g_name, c_id, c_at, m_list)
                ),
                insert_conv as (
                    insert into group_conversations (
                        chat_id, 
                        group_name, 
                        creater_id, 
                        last_message, 
                        last_message_time, 
                        created_at
                    )
                    select 
                        g_id, 
                        g_name, 
                        c_id, 
                        'group created', 
                        c_at, 
                        c_at 
                    from raw_data
                    returning chat_id, creater_id, created_at
                )
                insert into group_members (group_id, member_id, joined_at)
                select distinct on (target.g_id, target.m_id) 
                    target.g_id, target.m_id, target.c_at
                from (
                    -- combine creator from the inserted conversations
                    select chat_id as g_id, creater_id as m_id, created_at as c_at 
                    from insert_conv
                    
                    union all
                    
                    -- flatten initial members from the raw data
                    select 
                        rd.g_id, 
                        (jsonb_array_elements_text(rd.m_list)::int8) as m_id, 
                        rd.c_at 
                    from raw_data rd
                ) as target
                inner join users u on u.user_id = target.m_id -- join with users table
                on conflict (group_id, member_id) do nothing;
                "#,
                &group_ids,
                &group_names,
                &creator_ids,
                &created_at_v,
                &initial_members_json
            ).execute(&state.services.db_pool).await;

            if let Err(e) = res {
                eprintln!("Error batch inserting groups: {:?}", e);
            }
        }
    }
}
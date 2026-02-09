use std::sync::Arc;

use axum::{Json, extract::State, http::{HeaderMap, StatusCode}, response::IntoResponse};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use crate::my_states::AppState;
use serde_with::{serde_as, DisplayFromStr}; // Add this import

#[serde_as] // Add this macro
#[derive(Serialize, sqlx::FromRow)]
    pub struct ChatListItem {
    #[serde_as(as = "DisplayFromStr")] 
    pub chat_id: i64,
    pub name: String,
    #[serde_as(as = "DisplayFromStr")] 
    pub user_a_id: i64,
    #[serde_as(as = "DisplayFromStr")] 
    pub user_b_id:i64,
    pub last_message: Option<String>,
    pub last_message_time: DateTime<Utc>,
    pub is_group: bool,
    pub profile_url: Option<String>,
}
#[derive(Deserialize)]
pub struct GetConversationPayload{
    pub request_time: Option<chrono::DateTime<chrono::Utc>>
}
pub async fn get_conversations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload):Json<GetConversationPayload>
) -> Result<impl IntoResponse, StatusCode> {
    
    let user_id = headers.get("userid")
    .and_then(|v| v.to_str().ok())
    .map(|s| s.trim().trim_matches('"'))
    .and_then(|s| s.parse::<i64>().ok())
    .ok_or(StatusCode::UNAUTHORIZED)?;

    let given_time=match payload.request_time{
        Some(value)=> value,
        None => return Err(StatusCode::BAD_REQUEST)
    };

    let chats = sqlx::query_as!(
        ChatListItem,
        r#"
        select 
            chat_id as "chat_id!", 
            name as "name!", 
            user_a_id as "user_a_id!", --force non null if struct requires it
            user_b_id as "user_b_id!",
            last_message, 
            last_message_time as "last_message_time!", 
            is_group as "is_group!", 
            profile_url 
        from (
            -- u2u (User to User)
            select 
                c.chat_id, 
                case 
                    when c.user_a_id = $1 then u_b.display_name 
                    else u_a.display_name 
                end as name,
                c.user_a_id,          --add these to innneer sub query
                c.user_b_id,
                c.last_message,
                c.last_message_time,
                false as is_group,
                case 
                    when c.user_a_id = $1 then u_b.profile_url 
                    else u_a.profile_url 
                end as profile_url
            from conversations c
            join users u_a on c.user_a_id = u_a.user_id
            join users u_b on c.user_b_id = u_b.user_id
            where (c.user_a_id = $1 or c.user_b_id = $1)
            and c.last_message_time < $2

            union all

            -- grpchat
            select 
                g.chat_id,
                g.group_name as name,
                0 as user_a_id,       --placeholder gorupdont have user a or b id
                0 as user_b_id,
                g.last_message,
                g.last_message_time,
                true as is_group,
                g.profile_url
            from group_conversations g
            join group_members m on g.chat_id = m.group_id
            where m.member_id = $1
            and g.last_message_time < $2
        ) combined_chat
        order by last_message_time desc
        limit 15
        "#,
        user_id,
        given_time
    )
    .fetch_all(&state.services.db_pool)
    .await
    .map_err(|e| {
        eprintln!("Database Error: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    for chat in &chats {
    if chat.is_group {
        let group_lock = state.ws_clients.group_map
            .entry(chat.chat_id)
            .or_insert_with(|| Arc::new(parking_lot::RwLock::new(Vec::new())))
            .clone();

        let mut members = group_lock.write();
        
        if !members.contains(&(user_id as i64)) {
            members.push(user_id as i64);
        }
    }
}
    println!("{:?}",state.ws_clients.group_map);
    Ok(Json(chats))
}
// select 
//             chat_id as "chat_id!", 
//             name as "name!", 
//             last_message, 
//             last_message_time as "last_message_time!", 
//             is_group as "is_group!", 
//             profile_url 
//         from (
//             --u2u
//             select 
//                 c.chat_id, 
//                 case 
//                     when c.user_a_id = $1 then u_b.display_name 
//                     else u_a.display_name 
//                 end as name,
//                 c.last_message,
//                 c.last_message_time,
//                 false as is_group,
//                 case 
//                     when c.user_a_id = $1 then u_b.profile_url 
//                     else u_a.profile_url 
//                 end as profile_url
//             from conversations c
//             join users u_a on c.user_a_id = u_a.user_id
//             join users u_b on c.user_b_id = u_b.user_id
//             where (c.user_a_id = $1 or c.user_b_id = $1)
//               and c.last_message_time < $2

//             union all
//             -- grpchat
//             select 
//                 g.chat_id,
//                 g.group_name as name,
//                 g.last_message,
//                 g.last_message_time,
//                 true as is_group,
//                 g.profile_url
//             from group_conversations g
//             join group_members m on g.chat_id = m.group_id
//             where m.member_id = $1
//               and g.last_message_time < $2
//         ) combined_chat
//         order by last_message_time desc
//         limit 15
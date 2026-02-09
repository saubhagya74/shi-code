use axum::{Json, extract::State, http::{HeaderMap, StatusCode}, response::IntoResponse};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use crate::my_states::AppState;

use serde_with::{serde_as, DisplayFromStr}; // Add this import

#[serde_as] // Add this macro
#[derive(Serialize, sqlx::FromRow)]
pub struct MessageResponse {
    #[serde_as(as = "DisplayFromStr")]
    pub message_id: i64,
    
    #[serde_as(as = "DisplayFromStr")] // Converts i64 -> String in JSON
    pub sender_id: i64,
    
    pub content_type: String,
    pub description: Option<String>,
    pub messaged_at: DateTime<Utc>,
    pub is_edited: bool,
    pub reaction_emoji: Option<String>,
}
#[derive(Deserialize, Debug)]
pub struct ChatHistoryPayload {
    pub other_id: Option<String>,  
    pub group_id: Option<String>,
    pub last_msg_time: Option<DateTime<Utc>>,
}
pub async fn get_messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ChatHistoryPayload>,
) -> Result<impl IntoResponse, StatusCode> {
    // println!("{:?}",payload);
    let my_id = headers.get("userid")
    .and_then(|v| v.to_str().ok())
    .map(|s| s.trim().trim_matches('"')) // Remove spaces and extra quotes
    .and_then(|s| s.parse::<i64>().ok())
    .ok_or(StatusCode::UNAUTHORIZED)?;

    let (other_id, last_msg_time) = match (&payload.other_id, payload.last_msg_time) {
        (Some(o_str), Some(time)) => {
            let parsed_id = o_str.parse::<i64>().map_err(|_| StatusCode::BAD_REQUEST)?;
            (parsed_id, time)
        },
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let (user_a, user_b) = if my_id > other_id { 
        (my_id, other_id) 
    } else { 
        (other_id, my_id) 
    };
    // println!("{},{}",user_a,user_b);
    let messages = sqlx::query_as!(
        MessageResponse,
        r#"
        select 
            m.message_id as "message_id!", 
            m.sender_id as "sender_id!", 
            m.content_type::text as "content_type!", 
            m.description, 
            m.messaged_at as "messaged_at!", 
            m.is_edited as "is_edited!", 
            r.emoji_id_ as "reaction_emoji?"
        from messages m
        join conversations c on m.chat_id = c.chat_id
        left join reactions_ r on m.message_id = r.message_id_ and r.user_id_ = $4
        where c.user_a_id = $1 and c.user_b_id = $2
          and m.messaged_at < $3
          and m.is_deleted = false
        order by m.messaged_at desc
        limit 30
        "#,
        user_a,
        user_b,
        last_msg_time,
        my_id
    )
    .fetch_all(&state.services.db_pool)
    .await
    .map_err(|e| {
        eprintln!("fetch error: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(messages))
}

pub async fn get_group_messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ChatHistoryPayload>
) -> Result<impl IntoResponse, StatusCode> {
    let user_id = headers.get("userid")
        .and_then(|v| v.to_str().ok()?.parse::<i64>().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let (g_chat_id, last_msg_time) = match (&payload.group_id, payload.last_msg_time) {
        (Some(g_str), Some(time)) => {
            let parsed_id = g_str.parse::<i64>().map_err(|_| StatusCode::BAD_REQUEST)?;
            (parsed_id, time)
        },
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let is_member = sqlx::query!(
        "select 1 as exists_ from group_members where group_id = $1 and member_id = $2",
        g_chat_id,
        user_id
    )
    .fetch_optional(&state.services.db_pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if is_member.is_none() {
        return Err(StatusCode::FORBIDDEN);
    }

    let messages = sqlx::query_as!(
        MessageResponse,
        r#"
        select 
            m.message_id as "message_id!", 
            m.sender_id as "sender_id!", 
            m.content_type::text as "content_type!", 
            m.description, 
            m.messaged_at as "messaged_at!", 
            m.is_edited as "is_edited!", 
            r.emoji_id_ as "reaction_emoji?"
        from group_messages m
        left join reactions_ r on m.message_id = r.message_id_ and r.user_id_ = $1
        where m.chat_id = $2
          and m.messaged_at < $3
          and m.is_deleted = false
        order by m.messaged_at desc
        limit 30
        "#,
        user_id,
        g_chat_id,
        last_msg_time
    )
    .fetch_all(&state.services.db_pool)
    .await
    .map_err(|e| {
        eprintln!("group fetch error: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(messages))
}

use axum::{Json, extract::State, http::{HeaderMap, StatusCode}, response::IntoResponse};
use chrono::{DateTime, Utc, Duration};
use serde::{Deserialize, Serialize};
use crate::my_states::AppState;

use serde_with::{serde_as, DisplayFromStr}; // Add this import
#[serde_as] // Add this macro
#[derive(Serialize)]
pub struct StoryResponse {
    #[serde_as(as = "DisplayFromStr")] 
    pub story_id: i64,
    #[serde_as(as = "DisplayFromStr")] 
    pub creator_id: i64,
    pub story_created_at: DateTime<Utc>,
}
#[derive(Deserialize)]
pub struct GetStoriesPayload{
    pub request_time: Option<chrono::DateTime<chrono::Utc>>
}
pub async fn get_stories(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<GetStoriesPayload>
) -> Result<impl IntoResponse, StatusCode> {

    let user_id = headers.get("userid")
    .and_then(|v| v.to_str().ok())
    .map(|s| s.trim().trim_matches('"')) // Remove spaces and extra quotes
    .and_then(|s| s.parse::<i64>().ok())
    .ok_or(StatusCode::UNAUTHORIZED)?;

    let request_time = match payload.request_time {
        Some(value) => value,
        None => return Err(StatusCode::BAD_REQUEST),
    };
    
    let day_ago = Utc::now() - Duration::days(1);
    let limit = 20;

    let mut stories = sqlx::query_as!(
        StoryResponse,
        r#"
        select 
            story_id as "story_id!", 
            creator_id as "creator_id!", 
            story_created_at as "story_created_at!"
        from story_notifications 
        where user_id = $1 
          and story_created_at > $2 -- within last 24hr
          and story_created_at < $3 -- older than the requested time
        order by story_created_at desc 
        limit $4
        "#,
        user_id,
        day_ago,
        request_time,
        limit as i64
    )
    .fetch_all(&state.services.db_pool)
    .await
    .map_err(|e| {
        println!("noti fetch error: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if stories.len() < limit {
        let remaining = (limit - stories.len()) as i64;

        let heavy_stories = sqlx::query_as!(
            StoryResponse,
            r#"
            select 
                s.story_id as "story_id!", 
                s.user_id as "creator_id!", 
                s.created_at as "story_created_at!"
            from stories s
            inner join followed_following ff on s.user_id = ff.following_id
            where ff.follower_id = $1 
              and ff.is_followed_to_heavy = true
              and ff.is_active = true
              and s.created_at > $2
              and s.created_at < $3
            order by s.created_at desc
            limit $4
            "#,
            user_id,
            day_ago,
            request_time,
            remaining
        )
        .fetch_all(&state.services.db_pool)
        .await
        .map_err(|e| {
            println!("heavyfolowingto fetch error: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        stories.extend(heavy_stories);
    }

    Ok(Json(stories))
}
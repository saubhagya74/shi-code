use axum::{Router, http::StatusCode, routing::{get, post}};

use crate::my_states::AppState;

pub fn get_router()->Router<AppState>{
    Router::new()
    .route("/getHealth", get(async||{StatusCode::OK}))
    .route("/signup", post(super::user_controllers::create_user))
    .route("/signin", post(super::user_controllers::login_user))
    .route("/getStories", post(super::home::get_stories))
    .route("/getConversations", post(super::conversations::get_conversations))
    .route("/getMessages", post(super::messages::get_messages))
    .route("/getGroupMessages", post(super::messages::get_group_messages))
}
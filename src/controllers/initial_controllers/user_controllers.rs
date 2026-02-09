use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize};
use serde_json::{json};
use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier, 
    password_hash::{SaltString, rand_core::OsRng}
};
use crate::AppState;

#[derive(Deserialize)] 
pub struct CreateUserPayload {
    pub username: String,
    pub display_name: String,
    pub email: String,
    pub password: String
}

pub async fn create_user(
    State(state): State<AppState>, 
    Json(payload): Json<CreateUserPayload>
) -> impl IntoResponse {
    
    let new_id = state.services.bucket_id.lock().get_id();
    
    let password_bytes = payload.password.as_bytes();
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();

    let password_hash = match argon2.hash_password(password_bytes, &salt) {
        Ok(p) => p.to_string(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Error hashing password").into_response(),
    };
    
    let query_result = sqlx::query!(
        r#"
        INSERT INTO users (user_id, username, display_name, email, pass_hash) 
        VALUES ($1, $2, $3, $4, $5)
        "#,
        new_id as i64,
        payload.username,
        payload.display_name,
        payload.email,
        password_hash
    )
    .execute(&state.services.db_pool)
    .await;
    
    match query_result {
        Ok(_) => {
            (StatusCode::CREATED, Json(json!({
                "message": "User created",
                "user_id": new_id,
                "username": payload.username
            }))).into_response()
        }
        Err(e) => {
            eprintln!("Database error: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create user").into_response()
        }
    }
}

pub async fn login_user(
    State(state): State<AppState>,
    Json(payload): Json<LoginPayload>
) -> impl IntoResponse {

    // 1. Fetch BOTH the id and the pass_hash
    let row = sqlx::query!("SELECT user_id, pass_hash FROM users WHERE username = $1", payload.username)
        .fetch_optional(&state.services.db_pool)
        .await;

    // Optional: Anti-enumeration delay
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let db_user = match row {
        Ok(Some(user)) => user,
        Ok(None) => return (StatusCode::UNAUTHORIZED, "Invalid email or password").into_response(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response(),
    };

    let parsed_hash = match PasswordHash::new(&db_user.pass_hash) {
        Ok(hash) => hash,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Invalid hash format").into_response(),
    };

    // 2. Verify Password
    match Argon2::default().verify_password(payload.password.as_bytes(), &parsed_hash) {
        Ok(_) => {
            // 3. Return the user ID in the response
            // If you aren't using JWTs yet, we send the ID as a string or number
            (StatusCode::OK, Json(json!({ 
                "token": db_user.user_id.to_string(), // Using ID as a placeholder for token
            }))).into_response()
        },
        Err(_) => (StatusCode::UNAUTHORIZED, "Invalid email or password").into_response(),
    }
}

#[derive(Deserialize)]
pub struct LoginPayload {
    pub username: String,
    pub password: String,
}
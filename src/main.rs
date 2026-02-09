use axum::{Router, http::{HeaderName, HeaderValue, Method, StatusCode, header}, routing::get};
use tower_http::cors::CorsLayer;
use crate::{my_states::AppState};
mod controllers;
mod services;
mod my_states;
mod ws_controllers;
mod db_workers;
mod test;
#[tokio::main]
async fn main() {
    let cors = CorsLayer::new()
    .allow_origin("http://localhost:4200".parse::<HeaderValue>().unwrap())
    .allow_origin("http://localhost:8080".parse::<HeaderValue>().unwrap())
    .allow_methods([Method::GET, Method::POST])
    .allow_headers([header::CONTENT_TYPE, HeaderName::from_static("userid")]); 
    
    let master_state=my_states::initialize_state().await;

    let router1 = Router::<AppState>::new()
    .route("/health", get(|| async { StatusCode::OK }))
    .route("/ws", get(services::wscon::ws_handler));

    let final_router = Router::<AppState>::new()
        .merge(controllers::initial_controllers::routerfile::get_router()) // Added .await
        .merge(router1)
        .with_state(master_state)
        .layer(cors);
    //add cors
    let my_addr="0.0.0.0:6745";
    let my_listener=tokio::net::TcpListener::bind(my_addr).await.unwrap();
    axum::serve(my_listener,
    final_router.into_make_service_with_connect_info::<std::net::SocketAddr>()).await.unwrap();
}

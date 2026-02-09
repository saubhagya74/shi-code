use std::{sync::Arc};
use axum::{body::Bytes, extract::FromRef};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use snowflake::SnowflakeIdBucket;
use sqlx::{Pool, Postgres};
use crate::{db_workers, services, ws_controllers::{group::{self, create_group::{WSCreateGroupDB}, send_message::{GroupForwardMessage, WSGroupMessageDB}}, private_com::u2u_com::MessagePrivateDB, requests::handle_request::WSRequestDB}};

pub async fn initialize_state()->AppState{

    let my_db_pool=services::dbcon::pgcon().await.unwrap();
    let my_clients=Arc::new(RwLock::new(DashMap::<u64,UserConnection>::new()));
    let my_group_map = Arc::new(DashMap::new());

    let my_bucket_id=Arc::new(Mutex::new(
        snowflake::SnowflakeIdBucket::new(1, 1)));

    let (db_tx_p,db_rx_p)=tokio::sync::mpsc
    ::unbounded_channel::<MessagePrivateDB>();

    let (db_tx_cg,db_rx_cg)=tokio::sync::mpsc
    ::unbounded_channel::<WSCreateGroupDB>();

    let (db_tx_r,db_rx_r)=tokio::sync::mpsc
    ::unbounded_channel::<WSRequestDB>();

    let (db_tx_gm,db_rx_gm)=tokio::sync::mpsc
    ::unbounded_channel::<WSGroupMessageDB>();

    let (db_tx_fgm,db_rx_fgm)=tokio::sync::mpsc
    ::unbounded_channel::<GroupForwardMessage>();

    let master_state= AppState{
        services: ServiceState { db_pool: my_db_pool.clone(), bucket_id: my_bucket_id},
        ws_clients: WSClientState {
            clients:my_clients.clone(),
            group_map: my_group_map
            },
        ws_txes: WSTxState {
            tx_db_batch_private :db_tx_p,
            tx_db_create_group :db_tx_cg,
            tx_db_request: db_tx_r,
            tx_db_g_message: db_tx_gm,
            tx_db_fgm: db_tx_fgm,
            }
    };

    let s1=master_state.clone();
    tokio::spawn(async move{
        db_workers::u2u::u2u_message::db_batcher_private(s1, db_rx_p).await;
    });
    let s2=master_state.clone();
    tokio::spawn(async move{
        db_workers::group_workers::create_group::create_group(s2, db_rx_cg).await;
    });
    let s3=master_state.clone();
    tokio::spawn(async move{
        db_workers::request_workers::process_requests::db_batcher_requests(s3, db_rx_r).await;
    });
    let s4=master_state.clone();
    tokio::spawn(async move{
        group::forwader::ws_forwader_group(s4, db_rx_fgm).await;
    });
    let s5=master_state.clone();
    tokio::spawn(async move{
        db_workers::group_workers::group_message::db_batcher_group_messages(s5, db_rx_gm).await;
    });
    master_state
}
// The Master State

#[derive(Clone)]
pub struct AppState {
    pub services: ServiceState,
    pub ws_clients: WSClientState,
    pub ws_txes: WSTxState,
}

#[derive(Clone)]
pub struct WSTxState {
    pub tx_db_batch_private: tokio::sync::mpsc::UnboundedSender<MessagePrivateDB>,
    pub tx_db_create_group: tokio::sync::mpsc::UnboundedSender<WSCreateGroupDB>,
    pub tx_db_request: tokio::sync::mpsc::UnboundedSender<WSRequestDB>,
    pub tx_db_g_message: tokio::sync::mpsc::UnboundedSender<WSGroupMessageDB>,
    pub tx_db_fgm: tokio::sync::mpsc::UnboundedSender<GroupForwardMessage>,
}
#[allow(dead_code)]//////////////////////////////////////////////////////////////////////////
#[derive(Clone)]
pub struct WSClientState {
    pub clients: Arc<RwLock<DashMap<u64, UserConnection>>>,
    pub group_map: Arc<DashMap<i64, Arc<RwLock<Vec<i64>>>>>,
}

pub struct UserConnection {
    #[allow(unused)]
    pub socket_addr: std::net::SocketAddr,
    pub tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
}
#[derive(Clone)]
pub struct ServiceState {
    pub db_pool: Pool<Postgres>,
    pub bucket_id:Arc<Mutex<SnowflakeIdBucket>>,
}


impl FromRef<AppState> for ServiceState {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.services.clone()
    }
}

impl FromRef<AppState> for WSClientState {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.ws_clients.clone()
    }
}

impl FromRef<AppState> for WSTxState {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.ws_txes.clone()
    }
}
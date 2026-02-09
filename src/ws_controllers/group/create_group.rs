use axum::body::Bytes;
use serde::Deserialize;

use crate::AppState;

pub async fn create_group<'a>(
    payload:WSCreateGroupPayload<'a>,
    state: AppState,
    user_id: u64
){ //validate name, creator
    let initial_members_bytes=Bytes::copy_from_slice(payload.initial_members.
        get().as_bytes());
    //check group map here , if true then only send , if not do a db query 
    let db_rec=WSCreateGroupDB{
        group_name: payload.group_name.into_boxed_str(),
        creator_id: user_id as i64,
        initial_members: initial_members_bytes,
        created_at: chrono::Utc::now()
    };
    match state.ws_txes.tx_db_create_group.send(db_rec){
        Ok(_)=>{},
        Err(e)=>{
            println!("faile to send WSCreateGroupDB:{:?}",e);
        }
    }
}
#[derive(Deserialize,Debug)]
pub struct WSCreateGroupPayload<'a>{
    pub group_name: String,
    #[serde(borrow)]
    pub initial_members: &'a serde_json::value::RawValue
}
#[derive(Debug)]
pub struct WSCreateGroupDB{
    pub group_name: Box<str>,
    pub creator_id:i64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub initial_members: Bytes,
}
// #[derive(Debug)]
// pub struct WSInitialMemberDB{
//     pub group_id: i64,
//     pub member_id: i64
// }
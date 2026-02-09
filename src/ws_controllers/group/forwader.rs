use std::{collections::HashMap};

use axum::body::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::value;
use tokio::time::{Instant, timeout};

use crate::{my_states::AppState, ws_controllers::{group::send_message::GroupForwardMessage, private_com::u2u_com::ContentLabel}};

 
pub async fn ws_forwader_group(
    state: AppState,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<GroupForwardMessage>
){
    loop{
        let start=Instant::now();
        let limit=tokio::time::Duration::from_millis(20000);

        let mut messages= HashMap::<i64,GroupForwardMessage>::new();
        let mut grp_w_mid=HashMap::<i64,Vec<i64>>::new();

        while start.elapsed()<limit{
            let time_remaining=limit.saturating_sub(start.elapsed());
            match timeout(time_remaining, rx.recv()).await{
                Ok(Some(msg_payload))=>{

                    let msg_id=msg_payload.message_id;
                    let grp_id=msg_payload.group_id.clone();

                    messages.insert(msg_id.clone(), msg_payload);
                    grp_w_mid.entry(grp_id).or_default().push(msg_id);

                },
                Ok(None)=>{
                    println!("why no msg in ws forwadergorup ?");
                },
                Err(_)=>{
                    break;
                }
            }
        }
        
        if !grp_w_mid.is_empty() {
            let s1=state.clone();
            tokio::spawn(async move{
                finalize_bytes(s1, messages, grp_w_mid).await;
            });
        }
    }
}

pub async fn finalize_bytes(
    state:AppState,
    messages: HashMap<i64, GroupForwardMessage>,
    grp_w_mid: HashMap<i64, Vec<i64>>
){
    
    // This is the final map of grpid : finalJsonArrayInBytes
    let mut grp_w_final_bytes = HashMap::with_capacity(grp_w_mid.len());

    for (group_id, m_ids) in grp_w_mid {

        let mut bm_of_1_grp = Vec::with_capacity(m_ids.len());

        for m_id in m_ids {

            if let Some(msg) = messages.get(&m_id) {

                if let Ok(desc_raw) = serde_json::from_slice::<&value::RawValue>(&msg.description) {
                    bm_of_1_grp.push(ForwardMessage {
                        message_id: msg.message_id.to_string(),
                        sender_id: msg.sender_id.to_string(),
                        content_type: msg.content_type.clone(),
                        group_id: group_id.to_string(),
                        description: desc_raw, 
                    });
                }
            }
        }
        //searializATION must happen in this loop while references are alive
        if !bm_of_1_grp.is_empty() {
            // Serialize once per group
            if let Ok(json_vec) = serde_json::to_vec(&bm_of_1_grp) {
                //vec8 into bytes , o1
                grp_w_final_bytes.insert(group_id, Bytes::from(json_vec));
            }
        }//for big groups spaawn task inside, for now do at last
    }
    fanout(grp_w_final_bytes,state).await;
}
async fn fanout(
    grp_final_bytes: HashMap<i64, Bytes>,
    state: AppState
) {
    for (group_id, final_payload) in grp_final_bytes {
        //locks that group bucket not whole gorups
        if let Some(members_entry) = state.ws_clients.group_map.get(&group_id) {// for group
            //read lock matra
            let member_ids = members_entry.value().read();
            //read lock matra
            let clients_guard = state.ws_clients.clients.read();// for user s
            //borrow not taking
            for member_id in member_ids.iter() {

                if let Some(user_conn) = clients_guard.get(&(*member_id as u64)) {
                    // clone the handle . not the whole json
                    if let Err(e) = user_conn.tx.send(final_payload.clone()) {
                        eprintln!("Failed to send to user {}: {}", member_id, e);
                    }
                }
            }
        }//dashmap entry is drop automatically
    }
}
#[derive(Debug,Deserialize,Serialize)]
pub struct ForwardMessage<'a>{
    pub group_id: String,
    pub sender_id: String,
    pub message_id: String,
    pub content_type: ContentLabel,
    #[serde(borrow)]
    pub description: &'a value::RawValue
}
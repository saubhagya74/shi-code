

use std::env;
use dotenv::dotenv;
use sqlx::{Pool, Postgres};

pub async fn pgcon()-> Option<Pool<Postgres>>{

    dotenv().ok();
    let db_url = env::var("DATABASE_URL").expect("url not found");
    
    let conn_pool =sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await;

    match conn_pool{
            Ok(value)=>{
                println!("connected to database");
                return Some(value);
            },
            Err(e)=>{
                 panic!("error connecting database {:?}",e);
            }
    }
}
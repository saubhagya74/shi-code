use std::{env, time::Duration};
use aws_config;
use aws_sdk_s3::{self, Client, presigning::PresigningConfig};
use dotenv::dotenv;

#[allow(dead_code)]
pub async fn get_client()-> aws_sdk_s3::Client{
    dotenv().ok();
    let account_id=env::var("CF_ACCOUNT_ID").expect("CloudFare account id not found");
    let access_key=env::var("CF_ACCESS_KEY").expect("CloudFare access key not found");
    let secret_key=env::var("CF_SECRET_KEY").expect("CloudFare secret key not found");
    let endpoint_url = format!("https://{}.r2.cloudflarestorage.com", account_id);
    let credentials = aws_sdk_s3::config::Credentials::new(
        access_key, 
        secret_key,
        None,
        None,
        "static"
    );
    
    let config = aws_config::from_env()
        .region(aws_config::Region::new("auto")) //r2 uses auto
        .endpoint_url(endpoint_url)
        .credentials_provider(credentials)
        .load()
        .await;
    Client::new(&config)
}
#[allow(dead_code)]
#[allow(unused_variables)]
pub async fn main_work(){
    let client=get_client().await;
}
#[allow(dead_code)]
pub async fn upload_file(client: &Client, bucket: &str, key: &str, body: Vec<u8>) -> Result<(), aws_sdk_s3::Error> {
    client.put_object()
    .bucket(bucket)
    .key(key)
    .body(body.into())
    .content_type("application/octet-stream")
    .send()
    .await?;
Ok(())
}
#[allow(dead_code)]
pub async fn get_presigned_url(client: &Client, bucket: &str, key: &str) -> String {
    let expires_in = Duration::from_secs(3600);
    let presigned = client.put_object()
        .bucket(bucket)
        .key(key)
        .presigned(PresigningConfig::expires_in(expires_in).unwrap())
        .await
        .unwrap();
    
    presigned.uri().to_string()
}
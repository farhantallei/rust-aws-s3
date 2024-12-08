use std::error::Error;
use std::fs::File;
use std::io::Write;
use aws_sdk_s3::{Client};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::create_bucket::{CreateBucketError, CreateBucketOutput};
use aws_sdk_s3::operation::delete_bucket::{DeleteBucketError, DeleteBucketOutput};
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::list_buckets::{ListBucketsError, ListBucketsOutput};
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  dotenv::dotenv().ok();

  let access_key = std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
  let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY must be set");
  let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
  let profile = std::env::var("AWS_PROFILE").unwrap_or_else(|_| "default".to_string());
  let endpoint_url = std::env::var("AWS_ENDPOINT_URL").ok();

  let profile_str: &'static str = Box::leak(profile.into_boxed_str());

  let creds = Credentials::new(
    access_key,
    secret_key,
    None,
    None,
    profile_str,
  );

  let mut s3_config = aws_sdk_s3::config::Builder::new()
    .region(Region::new(region))
    .credentials_provider(creds);

  if endpoint_url.is_some() {
    s3_config = s3_config.endpoint_url(endpoint_url.unwrap());
    s3_config = s3_config.force_path_style(true);
  }

  let client = Client::from_conf(s3_config.build());

  let list_buckets_result = list_buckets(&client).await;

  match list_buckets_result {
    Ok(output) => {
      println!("Buckets: {:?}", output.buckets);
    }
    Err(error) => {
      println!("Error: {:?}", error);
    }
  }

  let bucket_name = "test-bucket";
  let file_path = "Cargo.toml";
  let key = "test/test2";

  let delete_bucket_result = delete_bucket(
    &client,
    bucket_name,
  ).await;

  match delete_bucket_result {
    Ok(output) => {
      println!("Bucket: {:?}", output);
    }
    Err(error) => {
      println!("Error: {:?}", error);
    }
  }

  let create_bucket_result = create_bucket(
    &client,
    bucket_name,
  ).await;

  match create_bucket_result {
    Ok(output) => {
      println!("Bucket: {:?}", output);
    }
    Err(error) => {
      println!("Error: {:?}", error);
    }
  }

  let upload_result = upload(
    &client,
    bucket_name,
    file_path,
    key,
  ).await;

  match upload_result {
    Ok(output) => {
      println!("Upload: {:?}", output);
    }
    Err(error) => {
      println!("Error: {:?}", error);
    }
  }

  let download_result = download(
    &client,
    bucket_name,
    key,
  ).await;

  match download_result {
    Ok(mut output) => {
      let mut file = File::create("./dump.txt").unwrap();

      while let Some(chunk) = output.body.try_next().await.unwrap() {
        file.write_all(&chunk).unwrap();
      }

      println!("Download: {:?}", output);
    }
    Err(error) => {
      println!("Error: {:?}", error);
    }
  }

  Ok(())
}

async fn upload(
  client: &Client,
  bucket_name: &str,
  file_path: &str,
  key: &str,
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
  let body = aws_sdk_s3::primitives::ByteStream::from_path(
    std::path::Path::new(file_path)
  ).await;

  client
    .put_object()
    .bucket(bucket_name)
    .key(key)
    .body(body.unwrap())
    .send()
    .await
}

async fn download(
  client: &Client,
  bucket_name: &str,
  key: &str,
) -> Result<GetObjectOutput, SdkError<GetObjectError>> {
  client
    .get_object()
    .bucket(bucket_name)
    .key(key)
    .send()
    .await
}

async fn list_buckets(
  client: &Client,
) -> Result<ListBucketsOutput, SdkError<ListBucketsError>> {
  client
    .list_buckets()
    .send()
    .await
}

async fn create_bucket(
  client: &Client,
  bucket_name: &str,
) -> Result<CreateBucketOutput, SdkError<CreateBucketError>> {
  client
    .create_bucket()
    .bucket(bucket_name)
    .send()
    .await
}

async fn delete_bucket(
  client: &Client,
  bucket_name: &str,
) -> Result<DeleteBucketOutput, SdkError<DeleteBucketError>> {
  client
    .delete_bucket()
    .bucket(bucket_name)
    .send()
    .await
}

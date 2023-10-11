use actix_cors::Cors;
use actix_files as fs;
use actix_web::{web, App, HttpResponse, HttpServer};
use futures_util::stream::StreamExt;
use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use std::env;
use std::time::Instant;
use uuid::Uuid;
// use std::fs::File;
// use tokio::io::AsyncReadExt;

async fn upload(mut payload: web::Payload) -> Result<HttpResponse, actix_web::Error> {
    let mut bytes = web::BytesMut::new();

    while let Some(chunk) = payload.next().await {
        bytes.extend_from_slice(&chunk?);
    }

    // Gere uma chave única no S3 com UUID
    let s3_key = format!("uploads/{}", Uuid::new_v4()); // uploads/uuid na pasta da raiz do bucket

    // Começa a cronometrar
    let start = Instant::now();

    // Fazendo upload para o S3
    let bucket_name = "nome_do_bucket"; // Nome do bucket
    let upload_result = upload_to_s3(&bytes, bucket_name, &s3_key).await;

    // Calcula a duração do upload
    let duration = start.elapsed();

    match upload_result {
        Ok(_) => Ok(HttpResponse::Ok().body(format!(
            "Arquivo enviado com sucesso para o S3 no caminho: {}. Tempo de envio: {:?}",
            s3_key, duration
        ))),
        Err(e) => Ok(HttpResponse::InternalServerError().body(format!(
            "Erro ao enviar para o S3: {:?}. Tempo de tentativa: {:?}",
            e, duration
        ))),
    }
}

async fn upload_to_s3(
    bytes: &web::BytesMut,
    bucket: &str,
    key: &str,
) -> Result<(), rusoto_core::RusotoError<rusoto_s3::PutObjectError>> {
    let s3 = S3Client::new(Region::SaEast1); //Região de São Paulo
    let req = PutObjectRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        body: Some(bytes.to_vec().into()),
        ..Default::default()
    };
    s3.put_object(req).await?;

    Ok(())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let bind_address = env::var("BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8088".to_string()); // porta padrão 8081

    HttpServer::new(|| {
        App::new()
            .wrap(
                Cors::permissive(), // permitindo cors para testes
            )
            .route("/upload", web::post().to(upload))
            .service(fs::Files::new("/download", "uploads/"))
    })
    .bind(&bind_address)?
    .run()
    .await
}

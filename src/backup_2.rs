use actix_cors::Cors;
use actix_files as fs;
use actix_web::{web, App, Error, HttpResponse, HttpServer};
use futures::future;
use futures::stream::StreamExt;
use rusoto_core::Region;
use rusoto_s3::{
    CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadRequest, S3Client, UploadPartRequest, S3,
};
use std::env;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
// use tokio_multipart::Multipart;
use log::{error, info, warn};
use uuid::Uuid;

const PART_SIZE: usize = 100 * 1024 * 1024; // 100 MB

async fn upload(mut payload: web::Payload) -> Result<HttpResponse, Error> {
    let max_concurrent_uploads = 5; // ou qualquer outro número que você quiser testar
    let semaphore = Arc::new(Semaphore::new(max_concurrent_uploads));

    info!("Iniciando o upload...");

    let s3_key = format!("uploads/{}", Uuid::new_v4());
    let s3_client = S3Client::new(Region::SaEast1);
    info!("Usando o S3 Key: {}", s3_key);

    let create_req = CreateMultipartUploadRequest {
        bucket: "rustarchive".to_string(),
        key: s3_key.clone(),
        ..Default::default()
    };

    let create_resp = s3_client
        .create_multipart_upload(create_req)
        .await
        .map_err(|e| {
            error!("Erro ao iniciar o upload multipart: {}", e);
            actix_web::error::ErrorInternalServerError(e)
        })?;

    let upload_id = create_resp.upload_id.unwrap();
    info!("Upload ID recebido: {}", upload_id);

    let completed_parts: Arc<Mutex<Vec<CompletedPart>>> = Arc::new(Mutex::new(Vec::new()));
    let mut upload_futures = Vec::new();
    let mut buffer = Vec::new();
    let mut part_number = 1;

    info!("Processando os chunks...");
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        buffer.extend_from_slice(&chunk);

        while buffer.len() >= PART_SIZE {
            let upload_part = buffer.split_off(PART_SIZE);

            info!("Preparando para enviar parte número {}", part_number);

            let parts = completed_parts.clone();
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    error!("Failed to acquire semaphore permit.");
                    return Err(actix_web::error::ErrorInternalServerError(
                        "Failed to acquire semaphore permit.",
                    ));
                }
            };

            let future = upload_single_part(
                s3_client.clone(),
                upload_part,
                "rustarchive",
                &s3_key,
                &upload_id,
                part_number,
                parts,
                permit, // passando o permit aqui
            );

            upload_futures.push(future);
            part_number += 1;
        }
    }

    info!("Finalizando envio de chunks restantes...");
    if !buffer.is_empty() {
        let parts = completed_parts.clone();

        let permit = match semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                error!("Failed to acquire semaphore permit.");
                return Err(actix_web::error::ErrorInternalServerError(
                    "Failed to acquire semaphore permit.",
                ));
            }
        };

        let future = upload_single_part(
            s3_client.clone(),
            buffer,
            "rustarchive",
            &s3_key,
            &upload_id,
            part_number,
            parts,
            permit, // passando o permit aqui
        );

        upload_futures.push(future);
    }

    info!("Aguardando todas as tarefas de upload concluírem...");
    future::join_all(upload_futures).await;

    let complete_req = CompleteMultipartUploadRequest {
        bucket: "rustarchive".to_string(),
        key: s3_key.clone(),
        upload_id: upload_id,
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(completed_parts.lock().await.clone()),
        }),
        ..Default::default()
    };

    s3_client
        .complete_multipart_upload(complete_req)
        .await
        .map_err(|e| {
            error!("Erro ao completar upload multipart: {}", e);
            actix_web::error::ErrorInternalServerError(e)
        })?;

    info!(
        "Upload concluído com sucesso para o S3 no caminho: {}",
        s3_key
    );
    Ok(HttpResponse::Ok().body(format!(
        "Arquivo enviado com sucesso para o S3 no caminho: {}",
        s3_key
    )))
}

async fn upload_single_part(
    s3_client: S3Client,
    part: Vec<u8>,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i64,
    completed_parts: Arc<Mutex<Vec<CompletedPart>>>,
    _permit: OwnedSemaphorePermit,
) -> Result<(), String> {
    info!("Enviando a parte número {}", part_number);

    let upload_part_req = UploadPartRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id: upload_id.to_string(),
        body: Some(part.into()),
        part_number,
        ..Default::default()
    };

    let upload_part_resp = s3_client.upload_part(upload_part_req).await.map_err(|e| {
        error!("Erro ao fazer upload da parte {}: {}", part_number, e);
        format!("Erro ao fazer upload da parte {}: {}", part_number, e)
    })?;

    completed_parts.lock().await.push(CompletedPart {
        e_tag: upload_part_resp.e_tag,
        part_number: Some(part_number),
    });

    info!("Parte número {} enviada com sucesso", part_number);

    Ok(())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    info!("Servidor inicializando...");

    let bind_address = env::var("BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8088".to_string());

    HttpServer::new(|| {
        App::new()
            .wrap(Cors::permissive()) // permitindo cors para testes
            .route("/upload", web::post().to(upload))
            .service(fs::Files::new("/download", "uploads/"))
    })
    .bind(&bind_address)?
    .run()
    .await
}

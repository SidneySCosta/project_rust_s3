use actix_cors::Cors;
use actix_files as fs;
use actix_web::{web, App, HttpResponse, HttpServer};
use futures_util::stream::StreamExt;
use rusoto_core::Region;
use rusoto_s3::CreateMultipartUploadRequest;
use rusoto_s3::{
    CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart, S3Client,
    UploadPartRequest, S3,
};
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

    let s3_key = format!("uploads/{}", Uuid::new_v4()); // Gere uma chave única no S3 com UUID
    let start = Instant::now(); // Começa a cronometrar

    match upload_to_s3(&bytes, "rustarchive", &s3_key).await {
        Ok(_) => {
            let duration = start.elapsed();
            Ok(HttpResponse::Ok().body(format!(
                "Arquivo enviado com sucesso para o S3 no caminho: {}. Tempo de envio: {:?}",
                s3_key, duration
            )))
        }
        Err(e) => {
            let duration = start.elapsed();
            Ok(HttpResponse::InternalServerError().body(format!(
                "Erro ao enviar para o S3: {}. Tempo de tentativa: {:?}",
                e, duration
            )))
        }
    }
}
async fn upload_to_s3(bytes: &web::BytesMut, bucket: &str, key: &str) -> Result<(), String> {
    // Mudei o tipo de erro de retorno para String para simplificar
    let s3_client = S3Client::new(Region::SaEast1);

    // Inicie o upload multipart
    let create_req = CreateMultipartUploadRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        ..Default::default()
    };
    let create_resp = s3_client
        .create_multipart_upload(create_req)
        .await
        .map_err(|e| format!("Erro ao criar upload multipart: {}", e))?;
    let upload_id = create_resp.upload_id.unwrap();

    // Suponhamos que cada parte tenha 5 MB
    let part_size = 5 * 1024 * 1024;
    let total_parts = (bytes.len() as f64 / part_size as f64).ceil() as usize;

    let mut completed_parts: Vec<CompletedPart> = vec![];

    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = if part_number == total_parts {
            bytes.len()
        } else {
            part_number * part_size
        };

        let part_data = &bytes[start..end];

        let upload_part_req = UploadPartRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.clone(),
            body: Some(part_data.to_vec().into()),
            part_number: part_number as i64,
            ..Default::default()
        };

        let upload_part_resp = s3_client
            .upload_part(upload_part_req)
            .await
            .map_err(|e| format!("Falha ao enviar parte {}: {}", part_number, e))?;

        completed_parts.push(CompletedPart {
            e_tag: upload_part_resp.e_tag,
            part_number: Some(part_number as i64),
        });
    }

    let complete_req = CompleteMultipartUploadRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id: upload_id,
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(completed_parts),
        }),
        ..Default::default()
    };

    s3_client
        .complete_multipart_upload(complete_req)
        .await
        .map_err(|e| format!("Erro ao completar upload multipart: {}", e))?;

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

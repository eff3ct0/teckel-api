//! Teckel gRPC Worker — receives pipeline execution requests from remote clients.
//!
//! Each session gets its own DataFusion SessionContext. The worker manages
//! session lifecycle, DataFrame caching, and pipeline execution.

mod service;

use std::net::SocketAddr;
use teckel_remote::PipelineServiceServer;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let host = std::env::var("TECKEL_WORKER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("TECKEL_WORKER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(50051);

    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    let worker = service::TeckelWorker::new();

    tracing::info!(%addr, "teckel-worker starting (gRPC)");

    Server::builder()
        .add_service(PipelineServiceServer::new(worker))
        .serve(addr)
        .await?;

    Ok(())
}

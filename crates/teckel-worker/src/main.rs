//! Teckel gRPC Server — unified API for the UI and programmatic clients.
//!
//! Serves both the high-level API (validate, explain, jobs) and the
//! low-level Spark Connect-style API (sessions, per-op execution).
//!
//! Enables gRPC-Web via tonic-web for direct browser connectivity.

mod service;

use std::net::SocketAddr;
use teckel_remote::TeckelServiceServer;
use tonic::transport::Server;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let host = std::env::var("TECKEL_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("TECKEL_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(50051);
    let max_concurrency: usize = std::env::var("TECKEL_MAX_CONCURRENCY")
        .ok()
        .and_then(|c| c.parse().ok())
        .unwrap_or_else(num_cpus::get);

    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    let worker = service::TeckelWorker::new(max_concurrency);

    // gRPC-Web + CORS for browser access.
    // TECKEL_CORS_ORIGINS: comma-separated list of allowed origins, or "*" for any.
    let cors_origins = std::env::var("TECKEL_CORS_ORIGINS").unwrap_or_else(|_| "*".to_string());
    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_headers(Any)
        .expose_headers(Any);
    let cors = if cors_origins.trim() == "*" {
        tracing::warn!("CORS allow_origin = * (any). Set TECKEL_CORS_ORIGINS to restrict.");
        cors.allow_origin(Any)
    } else {
        let origins: Vec<_> = cors_origins
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.parse().expect("invalid origin in TECKEL_CORS_ORIGINS"))
            .collect();
        tracing::info!(?origins, "CORS allow_origin restricted");
        cors.allow_origin(AllowOrigin::list(origins))
    };

    let grpc_web = tonic_web::GrpcWebLayer::new();

    tracing::info!(%addr, max_concurrency, "teckel gRPC server starting (gRPC-Web enabled)");

    Server::builder()
        .accept_http1(true) // Required for gRPC-Web
        .layer(cors)
        .layer(grpc_web)
        .add_service(TeckelServiceServer::new(worker))
        .serve(addr)
        .await?;

    Ok(())
}

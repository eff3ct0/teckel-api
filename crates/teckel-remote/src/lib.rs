mod backend;

pub mod proto {
    tonic::include_proto!("teckel.v1");
}

pub use backend::RemoteBackend;
pub use proto::teckel_service_client::TeckelServiceClient;
pub use proto::teckel_service_server::{TeckelService, TeckelServiceServer};

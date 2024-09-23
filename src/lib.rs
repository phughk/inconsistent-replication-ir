mod server;
mod client;
mod io;
pub mod types;

pub use server::InconsistentReplicationServer;
pub use client::InconsistentReplicationClient;
pub use io::{IRNetwork, IRStorage, ClientStorage};


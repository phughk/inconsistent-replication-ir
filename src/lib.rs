mod client;
mod io;
mod server;
pub mod types;
pub(crate) mod utils;

pub use client::InconsistentReplicationClient;
#[cfg(any(test, feature = "test"))]
pub use io::test_utils;
pub use io::{IRNetwork, IRStorage};
pub use server::InconsistentReplicationServer;

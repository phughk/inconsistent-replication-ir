#![feature(specialization)]
#![feature(async_iterator)]

mod client;
pub(crate) mod debug;
mod io;
mod server;
pub mod types;
pub(crate) mod utils;

pub use client::InconsistentReplicationClient;
#[cfg(any(test, feature = "test"))]
pub use io::test_utils;
pub use io::{IRNetwork, IRStorage};
pub use server::InconsistentReplicationServer;

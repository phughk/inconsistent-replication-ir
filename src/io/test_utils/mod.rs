mod fake_network;
mod fake_storage;
pub mod mock_computers;
mod mock_record_store;
mod mock_storage;

pub use fake_network::FakeIRNetwork;
pub use fake_storage::FakeIRStorage;
pub use mock_computers::MockOperationHandler;
pub use mock_storage::MockStorage;
pub use mock_storage::StorageMethod;

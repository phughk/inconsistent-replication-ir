use crate::server::{IRServerError, View, ViewState};
use crate::test_utils::mock_computers::NoopComputer;
use crate::test_utils::{FakeIRNetwork, FakeIRStorage};
use crate::InconsistentReplicationServer;
use std::sync::Arc;

#[tokio::test]
pub async fn inconsistent_requests_rejected_if_not_normal() {
    let network = FakeIRNetwork::<_, _, FakeIRStorage<String, String, NoopComputer<String>>>::new();
    let members: Vec<_> = vec!["1", "2", "3"]
        .iter()
        .map(ToString::to_string)
        .collect();
    let storage = FakeIRStorage::new(members.clone(), NoopComputer::new());

    let server = InconsistentReplicationServer::new(network, storage, "1".to_string()).await;

    let resp = server
        .propose_inconsistent("client-id".to_string(), 1, "message".to_string(), None)
        .await;
    match resp {
        Ok(_) => {
            panic!("Should fail")
        }
        Err(e) => match e {
            IRServerError::Recovering(e_view) => {
                assert_eq!(
                    e_view,
                    View {
                        view: 1,
                        members,
                        state: ViewState::Recovery,
                    }
                )
            }
            _ => {
                panic!("Unexpected error")
            }
        },
    }
}

#[tokio::test]
pub async fn inconsistent_changes_view_if_receives_higher() {
    let network = FakeIRNetwork::<String, String, FakeIRStorage<_, _, NoopComputer<String>>>::new();
    let members = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    let storage = FakeIRStorage::new(members.clone(), NoopComputer::new());
    storage
        .set_current_view(View {
            view: 3,
            members: members.clone(),
            state: ViewState::Normal,
        })
        .await;

    let server =
        InconsistentReplicationServer::new(network.clone(), storage, "1".to_string()).await;
    let new_view = View::<Arc<String>> {
        view: 4,
        members: vec![],
        state: ViewState::Normal,
    };
    server.propose_inconsistent("1".to_string(), 1, "msg".to_string(), None);
}

#[tokio::test]
pub async fn propose_already_finalised() {
    todo!()
}

pub async fn finalize_already_finalised() {
    todo!()
}

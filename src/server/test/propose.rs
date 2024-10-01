use crate::server::{IRServerError, View, ViewState};
use crate::test_utils::mock_computers::NoopComputer;
use crate::test_utils::{FakeIRNetwork, FakeIRStorage, MockStorage, StorageMethod};
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
pub async fn consistent_requests_rejected_if_not_normal() {
    todo!()
}

#[tokio::test]
pub async fn inconsistent_changes_view_if_receives_higher() {
    let network = FakeIRNetwork::<
        Arc<String>,
        Arc<String>,
        FakeIRStorage<_, _, NoopComputer<Arc<String>>>,
    >::new();
    let members = vec![
        Arc::new("1".to_string()),
        Arc::new("2".to_string()),
        Arc::new("3".to_string()),
    ];
    let storage = FakeIRStorage::new(members.clone(), NoopComputer::new());
    storage
        .set_current_view(View {
            view: 3,
            members: members.clone(),
            state: ViewState::Normal,
        })
        .await;

    let server =
        InconsistentReplicationServer::new(network.clone(), storage, Arc::new("1".to_string()))
            .await;
    let new_view = View::<Arc<String>> {
        view: 4,
        members: vec![],
        state: ViewState::Normal,
    };
    server.propose_inconsistent(
        Arc::new("1".to_string()),
        1,
        Arc::new("msg".to_string()),
        None,
    );
}

#[tokio::test]
pub async fn consistent_changes_view_if_receives_higher() {
    todo!()
}

#[tokio::test]
pub async fn propose_consistent() {
    let network = FakeIRNetwork::<String, String, MockStorage<_, _>>::new();
    let members: Vec<String> = vec!["1", "2", "3"].iter().map(|x| x.to_string()).collect();
    let view = View {
        view: 1,
        members: members.clone(),
        state: ViewState::Normal,
    };
    let storage = MockStorage::new(view.clone());
    let server =
        InconsistentReplicationServer::new(network.clone(), storage.clone(), "1".to_string()).await;

    // and
    storage.mock_record_tentative_consistent(Box::new(
        |client, seq, view, msg| -> Option<String> { Some(msg) },
    ));

    // when
    let val = server
        .propose_consistent("client-id".to_string(), 3, String::from("msg"))
        .await;

    // then only necessary calls
    assert!(storage.get_invocations_current_view() == vec![view.clone()]);
    assert!(
        storage.get_invocations_record_tentative_consistent()
            == vec![("client-id".to_string(), 3, view, "msg".to_string())]
    );

    // and no other calls
    storage.assert_invocations_no_calls(&[
        StorageMethod::ProposeInconsistent,
        StorageMethod::FinalizeConsistent,
        StorageMethod::FinalizeInconsistent,
    ])
}

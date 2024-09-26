use crate::server::View;
use crate::types::{IRMessage, NodeID};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};

/// Derive f (number of tolerable failures) from the number of nodes in the cluster
pub fn f(nodes: usize) -> Result<usize, ()> {
    if nodes < 3 {
        Err(())
    } else {
        Ok(((nodes - 1) as f32 / 2.0f32).ceil() as usize)
    }
}

/// Derive the fast quorum size from the number of nodes in the cluster
pub fn fast_quorum(nodes: usize) -> Result<usize, ()> {
    Ok(((3 * f(nodes)? + 1) as f32 / 2f32 + 1f32).floor() as usize)
}

/// Derive the normal quorum size from the number of nodes in the cluster
pub fn slow_quorum(nodes: usize) -> Result<usize, ()> {
    Ok(f(nodes)? + 1)
}

pub struct QuorumVote<'a, ID: NodeID, MSG: IRMessage> {
    node: &'a ID,
    message: &'a MSG,
    view: &'a View<ID>,
}

#[derive(Eq, PartialEq)]
struct Quorum<ID: NodeID, MSG: IRMessage> {
    count: usize,
    message: MSG,
    nodes_with: Vec<ID>,
    nodes_without: Vec<ID>,
    view: View<ID>,
}

impl<ID: NodeID, MSG: IRMessage> Debug for Quorum<ID, MSG>
where
    ID: Debug,
    MSG: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Quorum")
            .field("count", &self.count)
            .field("message", &self.message)
            .field("nodes_with", &self.nodes_with)
            .field("nodes_without", &self.nodes_without)
            .field("view", &self.view)
            .finish() // Concludes the formatting
    }
}

/// Find a quorum where the set agrees to a value
/// All views must match for quorum
/// Quorum can only be from largest view; Membership comes from largest quorum.
/// Err if no quorum
/// TODO change to return borrows not clones
pub fn find_quorum<
    'a,
    ID: NodeID,
    MSG: IRMessage,
    ITER: Iterator<Item = &'a QuorumVote<'a, ID, MSG>>,
>(
    iterable: ITER,
    quorum_type: QuorumType,
) -> Result<Quorum<ID, MSG>, ()> {
    let votes: BTreeMap<View<ID>, BTreeMap<MSG, Vec<ID>>> = BTreeMap::new();
    let _ = votes;
    let mut total_size = 0;
    for item in iterable {
        total_size += 1;
        let _ = item;
    }
    Err(())
}

pub enum QuorumType {
    FastQuorum,
    NormalQuorum,
}

#[cfg(test)]
mod test {
    use crate::server::ViewState;
    use crate::utils::{Quorum, QuorumType, QuorumVote};

    #[test]
    fn test_f() {
        assert!(super::f(0).is_err());
        assert!(super::f(1).is_err());
        assert!(super::f(2).is_err());
        assert_eq!(super::f(3), Ok(1));
        assert_eq!(super::f(4), Ok(2));
        assert_eq!(super::f(5), Ok(2));
        assert_eq!(super::f(6), Ok(3));
        assert_eq!(super::f(7), Ok(3));
    }

    #[test]
    fn test_fast_quorum() {
        assert!(super::fast_quorum(0).is_err());
        assert!(super::fast_quorum(1).is_err());
        assert!(super::fast_quorum(2).is_err());
        // f = 1; 3f/2+1 = 3/2+1 = 1.5+1 = 2.5 = 3
        assert_eq!(super::fast_quorum(3), Ok(3));
        // f = 2; 3f/2+1 = 6/2+1 = 3+1 = 4
        assert_eq!(super::fast_quorum(4), Ok(4));
        // f = 2; 3f/2+1 = 6/2+1 = 3+1 = 4
        assert_eq!(super::fast_quorum(5), Ok(4));
        // f = 3; 3f/2+1 = 9/2+1 = 4.5+1 = 5.5 = 6
        assert_eq!(super::fast_quorum(6), Ok(6));
        // f = 3; 3f/2+1 = 9/2+1 = 4.5+1 = 5.5 = 6
        assert_eq!(super::fast_quorum(7), Ok(6));
        // f = 4; 3f/2+1 = 12/2+1 = 6+1 = 7
        assert_eq!(super::fast_quorum(8), Ok(7));
        // f = 4; 3f/2+1 = 12/2+1 = 6+1 = 7
        assert_eq!(super::fast_quorum(9), Ok(7));
    }

    #[test]
    fn test_slow_quorum() {
        assert!(super::slow_quorum(0).is_err());
        assert!(super::slow_quorum(1).is_err());
        assert!(super::slow_quorum(2).is_err());
        assert_eq!(super::slow_quorum(3), Ok(2));
        assert_eq!(super::slow_quorum(4), Ok(3));
        assert_eq!(super::slow_quorum(5), Ok(3));
        assert_eq!(super::slow_quorum(6), Ok(4));
        assert_eq!(super::slow_quorum(7), Ok(4));
        assert_eq!(super::slow_quorum(8), Ok(5));
        assert_eq!(super::slow_quorum(9), Ok(5));
    }

    #[test]
    fn test_quorum() {
        struct TestCase<'a> {
            name: &'a str,
            votes: Vec<QuorumVote<'a, String, String>>,
            quorum_type: QuorumType,
            expected: Result<Quorum<String, String>, ()>,
        }

        fn view(num: u64, members: &[&'static str], state: ViewState) -> super::View<String> {
            super::View {
                view: num,
                members: members.iter().map(|s| s.to_string()).collect(),
                state,
            }
        }

        // Because of lifetimes we need to do some horrific shit
        let one = "1".to_string();
        let two = "2".to_string();
        let three = "3".to_string();
        let four = "4".to_string();

        let msg_a = "A".to_string();
        let msg_b = "B".to_string();

        // view_<number_of_nodes>_<view_number>_state
        let view_3_1_normal = view(1, &["1", "2", "3"], ViewState::Normal);
        let view_3_2_normal = view(2, &["1", "2", "3"], ViewState::Normal);
        let view_4_1_normal = view(1, &["1", "2", "3", "4"], ViewState::Normal);

        let cases: Vec<TestCase> = vec![
            // TODO quorum not achieved if no votes
            // TODO 4 nodes (f=1) and 2=A, 2=B
            TestCase {
                name: "Quorum is achieved if all votes are the same",
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                ],
                quorum_type: QuorumType::FastQuorum,
                expected: Ok(Quorum {
                    count: 3,
                    message: msg_a.clone(),
                    nodes_with: vec![one.clone(), two.clone(), three.clone()],
                    nodes_without: vec![],
                    view: view_3_1_normal.clone(),
                }),
            },
            TestCase {
                name: "Quorum is achieved if one value is different",
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_b,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Ok(Quorum {
                    count: 2,
                    message: msg_a.clone(),
                    nodes_with: vec![two.clone(), three.clone()],
                    nodes_without: vec![one.clone()],
                    view: view_3_1_normal.clone(),
                }),
            },
            TestCase {
                name: "Quorum is achieved if one value is missing",
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Ok(Quorum {
                    count: 2,
                    message: "A".to_string(),
                    nodes_with: vec![one.clone(), two.clone()],
                    nodes_without: vec![three.clone()],
                    view: view_3_1_normal.clone(),
                }),
            },
            TestCase {
                name: "Quorum is not achieved is one value has a larger view",
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_a,
                        view: &view_3_2_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Err(()),
            },
            TestCase {
                name: "Quorum is achieved if one value has a smaller view",
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_2_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_2_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Ok(Quorum {
                    count: 2,
                    message: "A".to_string(),
                    nodes_with: vec!["1".to_string(), "2".to_string()],
                    nodes_without: vec!["3".to_string()],
                    view: view(2, &["1", "2", "3"], ViewState::Normal),
                }),
            },
            TestCase {
                name: "Quorum is not achieved if equal split votes in 4 node cluster",
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_4_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_4_1_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_a,
                        view: &view_4_1_normal,
                    },
                    QuorumVote {
                        node: &four,
                        message: &msg_b,
                        view: &view_4_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Err(()),
            },
        ];

        for case in cases {
            let result = super::find_quorum(case.votes.iter(), case.quorum_type);
            assert_eq!(result, case.expected, "{}", case.name);
        }
    }
}

use rlp::RlpStream;
use sha3::{Digest, Keccak256};

// --- 1. Nibble Helper ---
// Handles the "Hex Prefix" (Compact) Encoding required by Ethereum
#[derive(Debug, Clone, PartialEq)]
struct Nibbles {
    data: Vec<u8>,
}

impl Nibbles {
    fn from_raw(key: &[u8]) -> Self {
        let mut data = Vec::with_capacity(key.len() * 2);
        for &b in key {
            data.push(b >> 4);
            data.push(b & 0x0F);
        }
        Nibbles { data }
    }

    fn common_prefix(&self, other: &Nibbles) -> usize {
        self.data
            .iter()
            .zip(other.data.iter())
            .take_while(|(a, b)| a == b)
            .count()
    }

    fn split_at(&self, idx: usize) -> (Nibbles, Nibbles) {
        (
            Nibbles {
                data: self.data[..idx].to_vec(),
            },
            Nibbles {
                data: self.data[idx..].to_vec(),
            },
        )
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    // Implements the Compact Encoding (Hex Prefix)
    // flag: 2 for Leaf, 0 for Extension (before parity check)
    fn encode_compact(&self, is_leaf: bool) -> Vec<u8> {
        let mut output = Vec::new();
        let term = if is_leaf { 2 } else { 0 };
        let odd = self.data.len() % 2 != 0;

        let flags = if odd { 1 } else { 0 } | term;

        if odd {
            output.push((flags << 4) | self.data[0]);
            for chunk in self.data[1..].chunks(2) {
                output.push((chunk[0] << 4) | chunk[1]);
            }
        } else {
            output.push(flags << 4);
            for chunk in self.data.chunks(2) {
                output.push((chunk[0] << 4) | chunk[1]);
            }
        }
        output
    }
}

// --- 2. Node Definition ---

#[derive(Clone, Debug)]
enum Node {
    Leaf {
        path: Nibbles,
        value: Vec<u8>,
    },
    Extension {
        path: Nibbles,
        child: Box<Node>,
    },
    Branch {
        children: [Option<Box<Node>>; 16],
        value: Option<Vec<u8>>,
    },
}

impl Node {
    // This is the core logic function you asked for.
    // It encodes the node. If strictly < 32 bytes, it returns the raw RLP.
    // If >= 32 bytes, it hashes the RLP and returns the RLP-encoded Hash.
    fn rlp_for_parent(&self) -> Vec<u8> {
        let raw_rlp = self.rlp_canonical();

        if raw_rlp.len() < 32 {
            // RULE: If RLP < 32 bytes, inline the node directly.
            raw_rlp
        } else {
            // RULE: If RLP >= 32 bytes, hash it and store the reference.
            let mut hasher = Keccak256::new();
            hasher.update(&raw_rlp);
            let hash = hasher.finalize(); // 32 bytes

            // We return the RLP encoding of the hash (which is 0xa0 + hash)
            let mut s = RlpStream::new();
            s.append(&hash.as_slice());
            s.out().to_vec()
        }
    }

    // Calculates the standard RLP of the node content
    fn rlp_canonical(&self) -> Vec<u8> {
        let mut s = RlpStream::new();
        match self {
            Node::Leaf { path, value } => {
                s.begin_list(2);
                s.append(&path.encode_compact(true));
                s.append(value);
            }
            Node::Extension { path, child } => {
                s.begin_list(2);
                s.append(&path.encode_compact(false));
                // Recurse: Ensure we apply the <32 byte check to the child pointer
                s.append_raw(&child.rlp_for_parent(), 1);
            }
            Node::Branch { children, value } => {
                s.begin_list(17);
                for child in children {
                    match child {
                        Some(node) => {
                            // Recurse: Apply rule to every child reference
                            s.append_raw(&node.rlp_for_parent(), 1);
                        }
                        None => {
                            s.append_empty_data();
                        }
                    }
                }
                match value {
                    Some(v) => s.append(v),
                    None => s.append_empty_data(),
                };
            }
        }
        s.out().to_vec()
    }

    // Use this to get the final Root Hash of the trie
    fn hash(&self) -> Vec<u8> {
        let rlp = self.rlp_canonical();
        let mut hasher = Keccak256::new();
        hasher.update(&rlp);
        hasher.finalize().to_vec()
    }
}

// --- 3. Trie Wrapper ---

pub struct MPT {
    root: Option<Node>,
}

impl MPT {
    pub fn new() -> Self {
        MPT { root: None }
    }

    pub fn root_hash(&self) -> Vec<u8> {
        match &self.root {
            // Ethereum defines the hash of an empty trie as the hash of empty RLP
            // Keccak256(RLP("")) = 56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421
            None => hex::decode("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
                .unwrap(),
            Some(node) => node.hash(),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        let nibbles = Nibbles::from_raw(key);
        self.root = Some(Self::insert_rec(self.root.take(), nibbles, value.to_vec()));
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        let nibbles = Nibbles::from_raw(key);
        let (new_root, removed) = Self::delete_rec(self.root.take(), nibbles);
        self.root = new_root;
        removed
    }

    fn insert_rec(node: Option<Node>, path: Nibbles, value: Vec<u8>) -> Node {
        match node {
            None => Node::Leaf { path, value },
            Some(Node::Leaf {
                path: leaf_path,
                value: leaf_val,
            }) => {
                let common = path.common_prefix(&leaf_path);

                // Exact match (update)
                if common == leaf_path.data.len() && common == path.data.len() {
                    return Node::Leaf {
                        path: leaf_path,
                        value,
                    };
                }

                // Split leaf into Branch
                let (_, suffix_old) = leaf_path.split_at(common);
                let (_, suffix_new) = path.split_at(common);

                let mut children: [Option<Box<Node>>; 16] = Default::default();
                let mut branch_val = None;

                // Place old leaf
                if suffix_old.is_empty() {
                    branch_val = Some(leaf_val);
                } else {
                    let idx = suffix_old.data[0] as usize;
                    let (_, rem) = suffix_old.split_at(1);
                    children[idx] = Some(Box::new(Node::Leaf {
                        path: rem,
                        value: leaf_val,
                    }));
                }

                // Place new leaf
                if suffix_new.is_empty() {
                    branch_val = Some(value);
                } else {
                    let idx = suffix_new.data[0] as usize;
                    let (_, rem) = suffix_new.split_at(1);
                    children[idx] = Some(Box::new(Node::Leaf { path: rem, value }));
                }

                let branch = Node::Branch {
                    children,
                    value: branch_val,
                };

                if common > 0 {
                    let (prefix, _) = path.split_at(common);
                    Node::Extension {
                        path: prefix,
                        child: Box::new(branch),
                    }
                } else {
                    branch
                }
            }
            Some(Node::Extension {
                path: ext_path,
                child,
            }) => {
                let common = path.common_prefix(&ext_path);
                if common == ext_path.data.len() {
                    // Pass through extension
                    let (_, rem) = path.split_at(common);
                    let new_child = Self::insert_rec(Some(*child), rem, value);
                    return Node::Extension {
                        path: ext_path,
                        child: Box::new(new_child),
                    };
                }

                // Split Extension
                let (_, suffix_ext) = ext_path.split_at(common);
                let (_, suffix_new) = path.split_at(common);

                let idx_ext = suffix_ext.data[0] as usize;
                let idx_new = suffix_new.data[0] as usize;

                let mut children: [Option<Box<Node>>; 16] = Default::default();

                // Old child branch
                let (_, rem_ext) = suffix_ext.split_at(1);
                if rem_ext.is_empty() {
                    children[idx_ext] = Some(child);
                } else {
                    children[idx_ext] = Some(Box::new(Node::Extension {
                        path: rem_ext,
                        child,
                    }));
                }

                // New child branch
                let (_, rem_new) = suffix_new.split_at(1);
                children[idx_new] = Some(Box::new(Node::Leaf {
                    path: rem_new,
                    value,
                }));

                let branch = Node::Branch {
                    children,
                    value: None,
                };

                if common > 0 {
                    let (prefix, _) = path.split_at(common);
                    Node::Extension {
                        path: prefix,
                        child: Box::new(branch),
                    }
                } else {
                    branch
                }
            }
            Some(Node::Branch {
                mut children,
                value: branch_val,
            }) => {
                if path.is_empty() {
                    return Node::Branch {
                        children,
                        value: Some(value),
                    };
                }
                let idx = path.data[0] as usize;
                let (_, rem) = path.split_at(1);

                let child = children[idx].take();
                let new_child = Self::insert_rec(child.map(|x| *x), rem, value);
                children[idx] = Some(Box::new(new_child));

                Node::Branch {
                    children,
                    value: branch_val,
                }
            }
        }
    }

    fn delete_rec(node: Option<Node>, path: Nibbles) -> (Option<Node>, bool) {
        match node {
            None => (None, false),
            Some(Node::Leaf {
                path: leaf_path,
                value,
            }) => {
                if leaf_path.data == path.data {
                    (None, true)
                } else {
                    (
                        Some(Node::Leaf {
                            path: leaf_path,
                            value,
                        }),
                        false,
                    )
                }
            }
            Some(Node::Extension {
                path: ext_path,
                child,
            }) => {
                let common = path.common_prefix(&ext_path);
                if common != ext_path.data.len() {
                    return (
                        Some(Node::Extension {
                            path: ext_path,
                            child,
                        }),
                        false,
                    );
                }
                let (_, rem) = path.split_at(common);
                let child_node = *child;
                let (new_child, removed) = Self::delete_rec(Some(child_node), rem);
                if !removed {
                    return (
                        Some(Node::Extension {
                            path: ext_path,
                            child: Box::new(
                                new_child.expect("child must exist when delete is a no-op"),
                            ),
                        }),
                        false,
                    );
                }
                let Some(new_child) = new_child else {
                    return (None, true);
                };

                // Compress extension + (extension|leaf) by concatenating paths.
                match new_child {
                    Node::Extension {
                        path: child_path,
                        child: grand_child,
                    } => {
                        let mut merged = ext_path.data.clone();
                        merged.extend_from_slice(&child_path.data);
                        (
                            Some(Node::Extension {
                                path: Nibbles { data: merged },
                                child: grand_child,
                            }),
                            true,
                        )
                    }
                    Node::Leaf {
                        path: child_path,
                        value: leaf_val,
                    } => {
                        let mut merged = ext_path.data.clone();
                        merged.extend_from_slice(&child_path.data);
                        (
                            Some(Node::Leaf {
                                path: Nibbles { data: merged },
                                value: leaf_val,
                            }),
                            true,
                        )
                    }
                    Node::Branch { .. } => (
                        Some(Node::Extension {
                            path: ext_path,
                            child: Box::new(new_child),
                        }),
                        true,
                    ),
                }
            }
            Some(Node::Branch {
                mut children,
                value,
            }) => {
                if path.is_empty() {
                    if value.is_none() {
                        return (Some(Node::Branch { children, value }), false);
                    }
                    let new_branch = Node::Branch {
                        children,
                        value: None,
                    };
                    return (Self::compress(new_branch), true);
                }

                let idx = path.data[0] as usize;
                let (_, rem) = path.split_at(1);

                let (new_child, removed) = Self::delete_rec(children[idx].take().map(|b| *b), rem);
                if !removed {
                    children[idx] = new_child.map(|n| Box::new(n));
                    return (Some(Node::Branch { children, value }), false);
                }
                children[idx] = new_child.map(|n| Box::new(n));
                (Self::compress(Node::Branch { children, value }), true)
            }
        }
    }

    fn compress(node: Node) -> Option<Node> {
        match node {
            Node::Branch {
                mut children,
                value,
            } => {
                let mut present: Vec<usize> = Vec::new();
                for i in 0..16 {
                    if children[i].is_some() {
                        present.push(i);
                    }
                }
                match (present.len(), value.clone()) {
                    (0, None) => None,
                    (0, Some(v)) => Some(Node::Leaf {
                        path: Nibbles { data: Vec::new() },
                        value: v,
                    }),
                    (1, None) => {
                        let idx = present[0];
                        let child = children[idx].take().unwrap();
                        match *child {
                            Node::Leaf { path, value } => {
                                let mut merged = vec![idx as u8];
                                merged.extend_from_slice(&path.data);
                                Some(Node::Leaf {
                                    path: Nibbles { data: merged },
                                    value,
                                })
                            }
                            Node::Extension { path, child } => {
                                let mut merged = vec![idx as u8];
                                merged.extend_from_slice(&path.data);
                                Some(Node::Extension {
                                    path: Nibbles { data: merged },
                                    child,
                                })
                            }
                            Node::Branch { .. } => Some(Node::Extension {
                                path: Nibbles {
                                    data: vec![idx as u8],
                                },
                                child,
                            }),
                        }
                    }
                    _ => Some(Node::Branch { children, value }),
                }
            }
            Node::Extension { path, child } => {
                // If the extension points to another short node, merge.
                match *child {
                    Node::Extension {
                        path: child_path,
                        child: grand,
                    } => {
                        let mut merged = path.data.clone();
                        merged.extend_from_slice(&child_path.data);
                        Some(Node::Extension {
                            path: Nibbles { data: merged },
                            child: grand,
                        })
                    }
                    Node::Leaf {
                        path: child_path,
                        value,
                    } => {
                        let mut merged = path.data.clone();
                        merged.extend_from_slice(&child_path.data);
                        Some(Node::Leaf {
                            path: Nibbles { data: merged },
                            value,
                        })
                    }
                    other => Some(Node::Extension {
                        path,
                        child: Box::new(other),
                    }),
                }
            }
            other => Some(other),
        }
    }
}

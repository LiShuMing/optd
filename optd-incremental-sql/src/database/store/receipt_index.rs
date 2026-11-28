//! Immutable receipt shards shared by successive database manifests.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{DatabaseStore, Payload, Receipt};
use crate::error::invalid;
use crate::store::{component, json_bytes, verified_file, write_bytes, MAX_JSON_BYTES};
use crate::Result;

const VERSION: u8 = 1;
const LEAF_TARGET_BYTES: usize = 64 * 1024;
const ROUTE_LEN: usize = 64;
const FILE_PREFIX: &str = "receipt-node-";

type Entries = BTreeMap<String, Receipt>;

/// Each committed manifest references exactly one versioned index root.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReceiptRoot {
    version: u8,
    node: Payload,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Node<T = Entries> {
    Branch(BTreeMap<u8, Payload>),
    Leaf(T),
}

impl ReceiptRoot {
    /// Validate the manifest version separately from reading the routing path.
    pub(super) fn validate_version(&self) -> Result<()> {
        if self.version != VERSION {
            return Err(invalid("unsupported database receipt index version"));
        }
        Ok(())
    }

    /// Detect a missing or corrupt root on open without loading every receipt.
    pub(super) fn validate_root(&self, store: &DatabaseStore) -> Result<()> {
        read_node(store, &self.node, &[])?;
        Ok(())
    }

    /// Read the exact request receipt along one hash route.
    pub(super) fn lookup(&self, store: &DatabaseStore, id: &str) -> Result<Option<Receipt>> {
        let route = route(id);
        let mut payload = self.node.clone();
        for depth in 0..=ROUTE_LEN {
            match read_node(store, &payload, &route[..depth])? {
                Node::Leaf(mut entries) => return Ok(entries.remove(id)),
                Node::Branch(mut children) => match children.remove(&route[depth]) {
                    Some(child) => payload = child,
                    None => return Ok(None),
                },
            }
        }
        Err(invalid("database receipt index route exceeds digest width"))
    }
}

/// Writes only nodes reached by an insertion, or builds a legacy index once.
pub(super) struct Writer<'a> {
    store: &'a DatabaseStore,
    directory: &'a Path,
    name: &'a str,
    files: usize,
}

impl<'a> Writer<'a> {
    pub(super) fn new(store: &'a DatabaseStore, directory: &'a Path, name: &'a str) -> Self {
        Self {
            store,
            directory,
            name,
            files: 0,
        }
    }

    pub(super) fn stage(
        &mut self,
        current: Option<&ReceiptRoot>,
        legacy: &Entries,
        receipt: Receipt,
    ) -> Result<ReceiptRoot> {
        let node = if let Some(root) = current {
            let route = route(&receipt.id);
            self.insert(&root.node, receipt, &route, 0)?
        } else {
            let mut entries = legacy.clone();
            insert_entry(&mut entries, receipt)?;
            let node = self.write_entries(entries, 0)?;
            self.store.work.receipt_migration(legacy.len())?;
            node
        };
        Ok(ReceiptRoot {
            version: VERSION,
            node,
        })
    }

    fn insert(
        &mut self,
        payload: &Payload,
        receipt: Receipt,
        route: &[u8; ROUTE_LEN],
        depth: usize,
    ) -> Result<Payload> {
        match read_node(self.store, payload, &route[..depth])? {
            Node::Leaf(mut entries) => {
                insert_entry(&mut entries, receipt)?;
                self.write_entries(entries, depth)
            }
            Node::Branch(mut children) => {
                let digit = route[depth];
                let child = match children.get(&digit) {
                    Some(old) => self.insert(old, receipt, route, depth + 1)?,
                    None => self.write_entries(
                        BTreeMap::from([(receipt.id.clone(), receipt)]),
                        depth + 1,
                    )?,
                };
                children.insert(digit, child);
                self.write_file(&json_bytes(&Node::<Entries>::Branch(children))?)
            }
        }
    }

    fn write_entries(&mut self, entries: Entries, depth: usize) -> Result<Payload> {
        let bytes = serde_json::to_vec(&Node::Leaf(&entries))?;
        if bytes.len() <= LEAF_TARGET_BYTES || entries.len() == 1 || depth == ROUTE_LEN {
            return self.write_file(&bytes);
        }

        let mut buckets = BTreeMap::<u8, Entries>::new();
        for (id, receipt) in entries {
            buckets
                .entry(route(&id)[depth])
                .or_default()
                .insert(id, receipt);
        }
        let children = buckets
            .into_iter()
            .map(|(digit, entries)| Ok((digit, self.write_entries(entries, depth + 1)?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        self.write_file(&json_bytes(&Node::<Entries>::Branch(children))?)
    }

    fn write_file(&mut self, bytes: &[u8]) -> Result<Payload> {
        if bytes.len() as u64 > MAX_JSON_BYTES {
            return Err(invalid(
                "individual receipt or colliding receipt shard exceeds byte budget",
            ));
        }
        let name = format!("{FILE_PREFIX}{}.json", self.files);
        self.files = self.files.checked_add(1).ok_or(crate::Error::Overflow)?;
        let file = write_bytes(self.directory, &name, bytes)?;
        self.store.work.receipt_write(file.bytes)?;
        Ok(Payload {
            offset: None,
            directory: self.name.to_owned(),
            file,
        })
    }
}

/// Shared receipt nodes are visited once across all retained manifest roots.
#[derive(Default)]
pub(super) struct Reachability {
    visited: BTreeMap<(String, String), VisitedNode>,
}

struct VisitedNode {
    bytes: u64,
    digest: String,
    prefix: Vec<u8>,
}

impl Reachability {
    pub(super) fn mark(
        &mut self,
        store: &DatabaseStore,
        root: &ReceiptRoot,
        reachable: &mut BTreeMap<String, BTreeSet<String>>,
    ) -> Result<()> {
        self.mark_node(store, &root.node, &mut Vec::new(), reachable)
    }

    fn mark_node(
        &mut self,
        store: &DatabaseStore,
        payload: &Payload,
        prefix: &mut Vec<u8>,
        reachable: &mut BTreeMap<String, BTreeSet<String>>,
    ) -> Result<()> {
        let key = (payload.directory.clone(), payload.file.name.clone());
        if let Some(seen) = self.visited.get(&key) {
            if seen.bytes != payload.file.bytes
                || seen.digest != payload.file.sha256
                || seen.prefix.as_slice() != prefix
            {
                return Err(invalid("inconsistent shared receipt node descriptor"));
            }
            return Ok(());
        }
        let node = read_node(store, payload, prefix)?;
        self.visited.insert(
            key,
            VisitedNode {
                bytes: payload.file.bytes,
                digest: payload.file.sha256.clone(),
                prefix: prefix.clone(),
            },
        );
        reachable
            .entry(payload.directory.clone())
            .or_default()
            .insert(payload.file.name.clone());
        if let Node::Branch(children) = node {
            for (digit, child) in children {
                prefix.push(digit);
                self.mark_node(store, &child, prefix, reachable)?;
                prefix.pop();
            }
        }
        Ok(())
    }
}

fn read_node(store: &DatabaseStore, payload: &Payload, prefix: &[u8]) -> Result<Node> {
    if !component(&payload.file.name)
        || !payload.file.name.starts_with(FILE_PREFIX)
        || payload.file.bytes > MAX_JSON_BYTES
    {
        return Err(invalid("invalid database receipt node descriptor"));
    }
    let file = verified_file(&store.directory(&payload.directory)?, &payload.file)?;
    let node: Node = serde_json::from_reader(file)?;
    match &node {
        Node::Leaf(entries) => {
            if entries.is_empty()
                || entries.iter().any(|(id, receipt): (&String, &Receipt)| {
                    id != &receipt.id
                        || id.is_empty()
                        || id.len() > 256
                        || !route(id).starts_with(prefix)
                })
            {
                return Err(invalid(
                    "database receipt is outside its shard or has an invalid ID",
                ));
            }
        }
        Node::Branch(children) => {
            if prefix.len() == ROUTE_LEN
                || children.is_empty()
                || children.keys().any(|digit| *digit >= 16)
            {
                return Err(invalid("invalid routing branch in database receipt index"));
            }
        }
    }
    store.work.receipt_read(payload.file.bytes)?;
    Ok(node)
}

fn insert_entry(entries: &mut Entries, receipt: Receipt) -> Result<()> {
    if entries.insert(receipt.id.clone(), receipt).is_some() {
        return Err(invalid("request identifier already committed"));
    }
    Ok(())
}

fn route(id: &str) -> [u8; ROUTE_LEN] {
    let digest = Sha256::digest(id.as_bytes());
    std::array::from_fn(|index| {
        let byte = digest[index / 2];
        if index % 2 == 0 {
            byte >> 4
        } else {
            byte & 0xf
        }
    })
}

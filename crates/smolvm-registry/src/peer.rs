//! Brokered peer-to-peer layer-blob fetch over node mTLS.

#[path = "peer_impl.rs"]
mod migrated;

pub(crate) use migrated::{fetch_blob_from_peers, peer_client, PeerClient};

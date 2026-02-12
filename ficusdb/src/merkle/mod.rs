mod aha;
mod backend;
mod merkle;
mod node;
mod store;
#[cfg(test)]
mod tests;
mod utils;

#[cfg(feature = "stats")]
mod stats;

type DirtyPtr = usize;
pub type CleanPtr = u64;

const NBRANCH: usize = 16;

pub use aha::AggregatedHashArray;
pub use backend::Backend;
pub use merkle::Merkle;
pub use node::Value;
pub use store::NodeStore;

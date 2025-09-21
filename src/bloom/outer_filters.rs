use std::{sync::{RwLock}};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};

use crate::bloom::{hash::{array_sharding_hash, bloom_check, bloom_hash, bloom_insert}, io::outer_insert_disk_io_cache};

pub const OUTER_ARRAY_SHARDS: u32 = 4096;

pub const OUTER_BLOOM_STARTING_MULT: u32 = 13;
pub const OUTER_BLOOM_STARTING_LENGTH: u64 = 1u64 << OUTER_BLOOM_STARTING_MULT;


pub struct OuterBlooms {
    pub(crate) filters: Vec<RwLock<BitVec>>,
}

impl Default for OuterBlooms {
    fn default() -> Self {
        let empty_bitvec = bitvec![0; OUTER_BLOOM_STARTING_LENGTH as usize];
        let filters = (0..OUTER_ARRAY_SHARDS)
            .map(|_| RwLock::new(empty_bitvec.clone()))
            .collect();
        OuterBlooms { filters }
    }
}

impl OuterBlooms {
    pub fn contains_and_insert(&self, key: &str) -> Result<bool> {
        let shards = array_sharding_hash(key, crate::FilterType::Outer)?;
        let map = bloom_hash(&shards, key, crate::FilterType::Outer)?;
        let result = bloom_check(&map, crate::FilterType::Outer)?;

        let exists = match result {
            crate::CollisionResult::Zero => {
                bloom_insert(&map, crate::FilterType::Outer);
                outer_insert_disk_io_cache(key, &shards);
                false
            },
            crate::CollisionResult::Partial(_) => {
                bloom_insert(&map, crate::FilterType::Outer);
                outer_insert_disk_io_cache(key, &shards);
                false
            }
            crate::CollisionResult::Complete(_, _) => {
                true
            }
            crate::CollisionResult::Error => {
                tracing::warn!("unexpected issue with collision result for key: {key}");
                return Err(anyhow!("Failed to match collision rsult of Outer bloom"))
            }
        };
        Ok(exists)

    }
}
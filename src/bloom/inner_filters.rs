use std::{collections::HashMap, sync::RwLock};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};

use crate::bloom::hash::{array_sharding_hash, bloom_check, bloom_hash, bloom_insert};

pub const INNER_ARRAY_SHARDS: u32 = 8192;

pub const INNER_BLOOM_STARTING_MULT: u32 = 12;
//pub const INNER_BLOOM_STARTING_LENGTH: u64 = 2_u64.pow( INNER_BLOOM_STARTING_MULT);
pub const INNER_BLOOM_STARTING_LENGTH: u64 = 1 << INNER_BLOOM_STARTING_MULT;


pub struct InnerBlooms {
    pub(crate) filters: Vec<RwLock<BitVec>>,
}

impl Default for InnerBlooms {
    fn default() -> Self {
        let empty_bitvec = bitvec![0; INNER_BLOOM_STARTING_LENGTH as usize];
        //let filters = std::array::from_fn(|_| RwLock::new(empty_bitvec.clone()));
        let filters = (0..INNER_ARRAY_SHARDS)
            .map(|_| RwLock::new(empty_bitvec.clone()))
            .collect();
        InnerBlooms { filters }
    }

}

impl InnerBlooms {
    pub fn contains_and_insert(key: &str) -> Result<bool> {
        let shards = array_sharding_hash(key, crate::FilterType::Inner)?;
        let map = bloom_hash(&shards, key, crate::FilterType::Inner)?;
        let result = bloom_check(&map, crate::FilterType::Inner)?;

        let exists = match result {
            crate::CollisionResult::Zero => {
                bloom_insert(&map, crate::FilterType::Inner);
                false
            },
            crate::CollisionResult::Partial(_) => {
                bloom_insert(&map, crate::FilterType::Inner);
                false
            }
            crate::CollisionResult::Complete(_, _) => {
                true
            }
            crate::CollisionResult::Error => {
                tracing::warn!("unexpected issue with collision result for key: {key}");
                return Err(anyhow!("Failed to match collision rsult of Inner bloom"))
            }
        };
        Ok(exists)

    }
}
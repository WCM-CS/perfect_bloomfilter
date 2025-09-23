use std::{collections::HashMap, io::Cursor, sync::{atomic::{AtomicU32, AtomicU64, Ordering}, Arc, RwLock}};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};
use once_cell::sync::Lazy;

use crate::{hash::{array_sharding_hash, bloom_check, bloom_hash, bloom_insert, HASH_SEED_SELECTION, INNER_ARRAY_SHARDS, INNER_BLOOM_HASH_FAMILY_SIZE, INNER_BLOOM_STARTING_LENGTH, INNER_BLOOM_STARTING_MULT, OUTER_ARRAY_SHARDS, OUTER_BLOOM_HASH_FAMILY_SIZE, OUTER_BLOOM_STARTING_LENGTH, OUTER_BLOOM_STARTING_MULT}, utils::{hash_remainder, CollisionResult, FilterType}};


pub static GLOBAL_PBF: Lazy<PerfectBloomFilter> = Lazy::new(|| {
    PerfectBloomFilter::default()
});


pub struct PerfectBloomFilter {
    pub(crate) outer_filter: Arc<Vec<ShardData>>,
    pub(crate) inner_filter: Arc<Vec<ShardData>>,

}

impl Default for PerfectBloomFilter {
    fn default() -> Self {
        let outer_filter = (0..OUTER_ARRAY_SHARDS)
            .map(|_| ShardData::new(FilterType::Outer))
            .collect::<Vec<_>>();
        let inner_filter = (0..INNER_ARRAY_SHARDS)
            .map(|_| ShardData::new(FilterType::Inner))
            .collect::<Vec<_>>();

        PerfectBloomFilter {
            outer_filter: Arc::new(outer_filter),
            inner_filter: Arc::new(inner_filter),
        }
    }
}

impl PerfectBloomFilter {
    pub fn system() -> &'static Self {
        &*GLOBAL_PBF
    }

    pub fn contains_and_insert(&self, key: &str) -> Result<bool> {
        let outer = Self::contains_insert(key, &FilterType::Outer)?;
        let inner = Self::contains_insert(key, &FilterType::Inner)?;
      
        Ok(inner & outer)
    }

    

    fn contains_insert(key: &str, filter_type: &FilterType) -> Result<bool>{
        let shards = array_sharding_hash(key, filter_type)?;
        let shards_hashes = bloom_hash(&shards, key, filter_type)?;
        
        let collision_results = bloom_check(&shards_hashes, filter_type)?;

        let exists = match collision_results {
            CollisionResult::Zero => {
                bloom_insert(&shards_hashes, filter_type);
                false
            },
            CollisionResult::Partial(_) => {
                bloom_insert(&shards_hashes, filter_type);
                false
            }
            CollisionResult::Complete(_, _) => {
                true
            }
            CollisionResult::Error => {
                tracing::warn!("unexpected issue with collision result for key: {key}");
                return Err(anyhow!("Failed to match collision rsult of Outer bloom"))
            }
        };
        
        Ok(exists)
        
    }

}

// Shard data grouped in a vector index
pub struct ShardData {
    //
    pub(crate) filter: RwLock<BitVec>,

    //
    pub(crate) key_count: RwLock<u64>,
    pub(crate) bloom_length: RwLock<u64>,
    pub(crate) bloom_length_mult: RwLock<u32>,

    /*
    pub(crate) key_count: AtomicU64,
    pub(crate) bloom_length: AtomicU64,
    pub(crate) bloom_length_mult: AtomicU32,
     */


}

impl ShardData {
    fn new(filter_type: FilterType) -> Self {
        match filter_type {
            FilterType::Outer => {
                let filter = RwLock::new(bitvec![0; OUTER_BLOOM_STARTING_LENGTH as usize]);
                let key_count = RwLock::new(0);
                let bloom_length = RwLock::new(OUTER_BLOOM_STARTING_LENGTH);
                let bloom_length_mult = RwLock::new(OUTER_BLOOM_STARTING_MULT);

                ShardData { filter, key_count, bloom_length, bloom_length_mult }
            },
            FilterType::Inner => {
                let filter = RwLock::new(bitvec![0; INNER_BLOOM_STARTING_LENGTH as usize]);
                let key_count = RwLock::new(0);
                let bloom_length = RwLock::new(INNER_BLOOM_STARTING_LENGTH);
                let bloom_length_mult = RwLock::new(INNER_BLOOM_STARTING_MULT);

                ShardData { filter, key_count, bloom_length, bloom_length_mult }
            },
        }
    }

}

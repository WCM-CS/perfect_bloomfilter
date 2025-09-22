use std::{collections::HashMap, io::Cursor, sync::{atomic::{AtomicU32, AtomicU64, Ordering}, Arc, RwLock}};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};
use once_cell::sync::Lazy;

use crate::{hash::{array_sharding_hash, bloom_check, bloom_hash, bloom_insert, contains_and_insert, INNER_ARRAY_SHARDS, INNER_BLOOM_STARTING_LENGTH, INNER_BLOOM_STARTING_MULT, OUTER_ARRAY_SHARDS, OUTER_BLOOM_STARTING_LENGTH, OUTER_BLOOM_STARTING_MULT}, utils::{CollisionResult, FilterType}};


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
        let outer = contains_and_insert(key, &FilterType::Outer)?;
        let inner = contains_and_insert(key, &FilterType::Inner)?;
      
        Ok(inner & outer)
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

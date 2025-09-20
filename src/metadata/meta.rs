use std::{collections::HashMap, sync::{Arc, RwLock}};

use once_cell::sync::Lazy;

use crate::bloom::{
    inner_filters::{INNER_ARRAY_SHARDS, INNER_BLOOM_STARTING_LENGTH, INNER_BLOOM_STARTING_MULT}, 
    outer_filters::{OUTER_ARRAY_SHARDS, OUTER_BLOOM_STARTING_LENGTH, OUTER_BLOOM_STARTING_MULT}
};

pub static GLOBAL_METADATA: Lazy<MetaData> = Lazy::new(|| {
    MetaData::new()
});


pub struct MetaData {
    pub(crate) outer_metadata: Arc<Meta>,
    pub(crate) inner_metadata: Arc<Meta>,
}

impl Default for MetaData {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaData {
    pub fn new() -> Self {
        let outer_metadata = Arc::new(Meta::new_outer());
        let inner_metadata = Arc::new(Meta::new_inner());

        MetaData {
            outer_metadata,
            inner_metadata,
        }
    }
}

pub struct Meta {
    blooms_key_count: Arc<RwLock<HashMap<u32, u64>>>,
    pub(crate) bloom_bit_length: Arc<RwLock<HashMap<u32, u64>>>,
    bloom_bit_length_mult: Arc<RwLock<HashMap<u32, u32>>>,
    array_shards: Arc<u32>,
}

impl Meta {
    fn new_outer() -> Self {
        let mut key_count: HashMap<u32, u64> = HashMap::with_capacity(OUTER_ARRAY_SHARDS as usize);
        let mut bit_length: HashMap<u32, u64> = HashMap::with_capacity(OUTER_ARRAY_SHARDS as usize);
        let mut bit_length_mult: HashMap<u32, u32> = HashMap::with_capacity(OUTER_ARRAY_SHARDS as usize);

        for partition in 0..OUTER_ARRAY_SHARDS{
            key_count.insert(partition, 0);
            bit_length.insert(partition, OUTER_BLOOM_STARTING_LENGTH); 
            bit_length_mult.insert(partition, OUTER_BLOOM_STARTING_MULT);
        }

        Self {
            blooms_key_count: Arc::new(RwLock::new(key_count)),
            bloom_bit_length: Arc::new(RwLock::new(bit_length)),
            bloom_bit_length_mult: Arc::new(RwLock::new(bit_length_mult)),
            array_shards: Arc::new(OUTER_ARRAY_SHARDS)
        }
    }

    fn new_inner() -> Self {
        let mut key_count: HashMap<u32, u64> = HashMap::with_capacity(INNER_ARRAY_SHARDS as usize);
        let mut bit_length: HashMap<u32, u64> = HashMap::with_capacity(INNER_ARRAY_SHARDS as usize);
        let mut bit_length_mult: HashMap<u32, u32> = HashMap::with_capacity(INNER_ARRAY_SHARDS as usize);

        for partition in 0..INNER_ARRAY_SHARDS{
            key_count.insert(partition, 0);
            bit_length.insert(partition, INNER_BLOOM_STARTING_LENGTH); 
            bit_length_mult.insert(partition, INNER_BLOOM_STARTING_MULT);
        }

        Self {
            blooms_key_count: Arc::new(RwLock::new(key_count)),
            bloom_bit_length: Arc::new(RwLock::new(bit_length)),
            bloom_bit_length_mult: Arc::new(RwLock::new(bit_length_mult)),
            array_shards: Arc::new(INNER_ARRAY_SHARDS)
        }
    }
}
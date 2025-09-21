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
    fn new() -> Self {
        let outer_metadata = Arc::new(Meta::new_outer());
        let inner_metadata = Arc::new(Meta::new_inner());

        MetaData {
            outer_metadata,
            inner_metadata,
        }
    }
}

pub struct Meta {
    pub(crate) shards_metadata: Arc<Vec<ShardMeta>>,
    pub(crate) array_shards: Arc<u32>,
}

pub struct ShardMeta {
    pub blooms_key_count: RwLock<u64>,
    pub bloom_bit_length: RwLock<u64>,
    pub bloom_bit_length_mult: RwLock<u32>,
}

impl Meta {
    fn new_outer() -> Self {
        let mut shards_metadata = Vec::with_capacity(OUTER_ARRAY_SHARDS as usize);

        for _ in 0..OUTER_ARRAY_SHARDS {
            shards_metadata.push(ShardMeta {
                blooms_key_count: RwLock::new(0),
                bloom_bit_length: RwLock::new(OUTER_BLOOM_STARTING_LENGTH),
                bloom_bit_length_mult: RwLock::new(OUTER_BLOOM_STARTING_MULT),
            });
        }

        Self {
            shards_metadata: Arc::new(shards_metadata),
            array_shards: Arc::new(OUTER_ARRAY_SHARDS),
        }
    }

    fn new_inner() -> Self {
        let mut shards_metadata = Vec::with_capacity(INNER_ARRAY_SHARDS as usize);

        for _ in 0..INNER_ARRAY_SHARDS {
            shards_metadata.push(ShardMeta {
                blooms_key_count: RwLock::new(0),
                bloom_bit_length: RwLock::new(INNER_BLOOM_STARTING_LENGTH),
                bloom_bit_length_mult: RwLock::new(INNER_BLOOM_STARTING_MULT),
            });
        }

        Self {
            shards_metadata: Arc::new(shards_metadata),
            array_shards: Arc::new(INNER_ARRAY_SHARDS),
        }
    }
}


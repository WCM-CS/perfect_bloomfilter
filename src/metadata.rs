use std::{collections::HashMap, sync::{Arc, RwLock}};

use crate::{
    inner_filters::{INNER_ARRAY_SHARDS, INNER_BLOOM_STARTING_LENGTH, INNER_BLOOM_STARTING_MULT}, 
    outer_filters::{OUTER_ARRAY_SHARDS, OUTER_BLOOM_STARTING_LENGTH, OUTER_BLOOM_STARTING_MULT}
};


pub struct MetaData {
    outer_metadata: Arc<RwLock<Meta>>,
    inner_metadata: Arc<RwLock<Meta>>,
}

impl Default for MetaData {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaData {
    pub fn new() -> Self {
        let outer_metadata = Arc::new(RwLock::new(Meta::new()));
        let inner_metadata = Arc::new(RwLock::new(Meta::new()));

        MetaData {
            outer_metadata,
            inner_metadata,
        }
    }
}

struct Meta {
    blooms_key_count: HashMap<u32, u64>,
    bloom_bit_length: HashMap<u32, u64>,
    bloom_bit_length_mult: HashMap<u32, u32>,
   
}

impl Meta {
    fn new() -> Self {
        Meta {
            blooms_key_count: HashMap::new(),
            bloom_bit_length: HashMap::new(),
            bloom_bit_length_mult: HashMap::new(),
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
            blooms_key_count: key_count,
            bloom_bit_length: bit_length,
            bloom_bit_length_mult: bit_length_mult,
        }
    }

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
            blooms_key_count: key_count,
            bloom_bit_length: bit_length,
            bloom_bit_length_mult: bit_length_mult,
        }
    }
}
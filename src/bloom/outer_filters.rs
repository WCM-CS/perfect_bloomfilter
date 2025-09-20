use std::{sync::{RwLock}};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};


pub const OUTERBLOOM_HASH_FAMILY_SIZE: u32 = 7;
pub const OUTER_ARRAY_SHARDS: u32 = 4096;

pub const OUTER_BLOOM_STARTING_MULT: u32 = 13;
pub const OUTER_BLOOM_STARTING_LENGTH: u64 = 2_u64.pow( OUTER_BLOOM_STARTING_MULT);


pub struct OuterBlooms {
    filters: [RwLock<BitVec>; OUTER_ARRAY_SHARDS as usize],
}

impl Default for OuterBlooms {
    fn default() -> Self {
        let empty_bitvec = bitvec![0; OUTER_ARRAY_SHARDS as usize];
        let filters = std::array::from_fn(|_| RwLock::new(empty_bitvec.clone()));
        OuterBlooms { filters }
    }
}

impl OuterBlooms {
    pub fn contains_and_insert(&self, key: &str) -> Result<()> {
        Ok(())

    }
}
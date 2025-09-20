use std::{sync::{RwLock}};
use bitvec::vec::BitVec;
use bitvec::bitvec;
use anyhow::{Result, anyhow};

pub const INNER_ARRAY_SHARDS: u32 = 8192;

pub const INNER_BLOOM_STARTING_MULT: u32 = 12;
pub const INNER_BLOOM_STARTING_LENGTH: u64 = 2_u64.pow( INNER_BLOOM_STARTING_MULT);


pub struct InnerBlooms {
    filters: [RwLock<BitVec>; INNER_ARRAY_SHARDS as usize],
}

impl Default for InnerBlooms {
    fn default() -> Self {
        let empty_bitvec = bitvec![0; INNER_ARRAY_SHARDS as usize];
        let filters = std::array::from_fn(|_| RwLock::new(empty_bitvec.clone()));
        InnerBlooms { filters }
    }

}

impl InnerBlooms {
    pub fn contains_and_insert(&self, key: &str) -> Result<()> {
        Ok(())

    }
}
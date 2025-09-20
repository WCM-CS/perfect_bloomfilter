use std::{collections::HashMap, io::Cursor};
use anyhow::{Result, anyhow};

use crate::{bloom::{inner_filters::INNER_ARRAY_SHARDS, outer_filters::OUTER_ARRAY_SHARDS, utils::jump_hash_partition}, metadata::meta::GLOBAL_METADATA, CollisionResult, FilterType};

pub const OUTER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;
pub const INNER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;

pub const HASH_SEED_SELECTION: [u32; 6] = [
    0x8badf00d,
    0xdeadbabe,
    0xabad1dea,
    0xdeadbeef,
    0xcafebabe,
    0xfeedface,
];


fn array_sharding_hash(key: &str, filter_type: FilterType) -> Result<Vec<u32>> {
    let (mask, hash_seed) = match filter_type {
        FilterType::Outer => {
            (OUTER_ARRAY_SHARDS - 1, HASH_SEED_SELECTION[0])
        },
        FilterType::Inner => {
            (OUTER_ARRAY_SHARDS - 1, HASH_SEED_SELECTION[1])
        }
    };

    let h1 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), hash_seed)?;

    let high = (h1 >> 64) as u64;
    let low = h1 as u64;
    let shard_size = mask / 2;

    let p1 = jump_hash_partition(high ^ low, &(mask + 1))?;
    let p2 = (p1 + shard_size) & mask;

    debug_assert!(
        p1 != p2,
        "Partitions must be unique"
    );

    Ok(vec![p1, p2])


}

// Kirsch-Mitzenmacher optimization 
fn bloom_hash(shards: &[u32], key: &str, filter_type: FilterType) -> Result<HashMap<u32, Vec<u64>>> {
    let mut hash_list = HashMap::new();
    
    // get the shards bit lengths 
    let (bitvec_lens, hash_seeds, hash_family_size) = match filter_type {
        FilterType::Outer => {
            let mut bitvec_lens: Vec<u64> = vec![];
            let locked_meta = GLOBAL_METADATA.outer_metadata.bloom_bit_length.read().unwrap();
            for shard in shards {
                let bit_len = locked_meta.get(shard).unwrap();
                bitvec_lens.push(*bit_len);
            }
            (bitvec_lens, [HASH_SEED_SELECTION[2], HASH_SEED_SELECTION[3]], OUTER_BLOOM_HASH_FAMILY_SIZE)
        },
        FilterType::Inner => {
            let mut bitvec_lens: Vec<u64> = vec![];
            let locked_meta = GLOBAL_METADATA.inner_metadata.bloom_bit_length.read().unwrap();
            for shard in shards {
                let bit_len = locked_meta.get(shard).unwrap();
                bitvec_lens.push(*bit_len);
            }
            (bitvec_lens, [HASH_SEED_SELECTION[4], HASH_SEED_SELECTION[5]], INNER_BLOOM_HASH_FAMILY_SIZE)
        }
    };

    for shard in shards {

        let mut key_hashes = Vec::with_capacity(OUTER_BLOOM_HASH_FAMILY_SIZE as usize);
        let h1 = murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), hash_seeds[0])?;
        let h2 = murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), hash_seeds[1])?;

        for idx in 0..hash_family_size {
            let idx_u128 = idx as u128;
            let mask = bitvec_lens.get(idx as usize).unwrap() - 1;
            let index = (h1.wrapping_add(idx_u128.wrapping_mul(h2))) & mask as u128;

            key_hashes.push(index as u64)
        }

        hash_list.insert(*shard, key_hashes);

            
    }

    Ok(hash_list)

}

fn bloom_insert(outer_hashed: &HashMap<u32, Vec<u64>>, active_rehashing_shards: Option<Vec<u32>>, key: &str) -> Result<()> {
    return Err(anyhow!("Failed to match collision rsult of Outer bloom"))
}

fn bloom_collision_check(map: &HashMap<u32, Vec<u64>>, active_rehashing_shards: &Option<Vec<u32>>) -> Result<CollisionResult> {
    return Err(anyhow!("Failed to match collision rsult of Outer bloom"))
}
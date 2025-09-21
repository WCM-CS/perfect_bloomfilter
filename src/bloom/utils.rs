use anyhow::{Result, anyhow};

use crate::{bloom::rehash::{BLOOM_LENGTH_PER_KEY}, metadata::meta::GLOBAL_METADATA, FilterType};

const JUMP_HASH_SHIFT: i32 = 33;
const JUMP_HASH_CONSTANT: u64 = 2862933555777941757;
const JUMP: f64 = (1u64 << 31) as f64;

// BitOps

//Left Shift
pub fn power_of_two(x: u32) -> u64{
   1_u64 << x
}

// Right Shift
pub fn shift_right_bits(x: u128) -> u128{
    x >> 64
}

// Bitwise XOR
pub fn compare_high_low(high: u64, low: u64) -> u64{
    high ^ low
}

// Bitwise AND
pub fn partition_remainder(base: u32, mask: u32) -> u32{
    base & mask
}

pub fn hash_remainder(base: u128, mask: u128) -> u64{
    (base & mask) as u64
}


// JumpConsistentHash function, rust port from the jave implementation
// Link to the repo: https://github.com/ssedano/jump-consistent-hash/blob/master/src/main/java/com/github/ssedano/hash/JumpConsistentHash.java
// Original algorithm founded in 2014 by google Lamping & Veach
pub fn jump_hash_partition(key: u64, buckets: &u32) -> Result<u32> {
    if *buckets == 0 {
        return Err(anyhow!("Number of buckets must be positive"));
    }

    let mut b: i64 = -1;
    let mut j: i64 = 0;
    let mut mut_key = key;

    while j < *buckets as i64 {
        b = j;
        mut_key = mut_key.wrapping_mul(JUMP_HASH_CONSTANT).wrapping_add(1);

        let shifted = (mut_key >> JUMP_HASH_SHIFT).wrapping_add(1);
        let exp = shifted.max(1) as f64; 

        j = ((b as f64 + 1.0) * (JUMP / exp)).floor() as i64;
    }

    Ok(b as u32)
}

// concurrency configuration
pub fn concurrecy_init() -> Result<usize> {
    let system_threads = match std::thread::available_parallelism() {
        Ok(c) => c.get(),
        Err(e) => {
            tracing::warn!("Failed to get the core count:{e}");
            return Err(anyhow!("Failed to get the systems thread count"));
        }
    };

    let numerator = system_threads * 8;
    let denominator = 10;
    let usable_system_threads = (numerator + denominator - 1) / denominator;

    Ok(usable_system_threads)
}

// metadata computation

/*
pub fn metadata_computation(shards: Vec<u32>, filter_type: FilterType) {
    match filter_type {
        FilterType::Outer => {
            let mut rehash_shards = vec![];
            for shard in shards {
                let locked_shard_key_count = GLOBAL_METADATA.outer_metadata.shards_metadata[shard as usize].blooms_key_count.read().unwrap();
                let locked_shard_bloom_len = GLOBAL_METADATA.outer_metadata.shards_metadata[shard as usize].bloom_bit_length.read().unwrap();
                
                let bit_len_per_key = (*locked_shard_bloom_len as f64) / (*locked_shard_key_count as f64);

                if bit_len_per_key <= BLOOM_LENGTH_PER_KEY {
                    rehash_shards.push(shard);
                }
            }
            outer_insert_rehash_queue(&rehash_shards);
        },
        FilterType::Inner => {
            let mut rehash_shards = vec![];
            for shard in shards {
                let locked_shard_key_count = GLOBAL_METADATA.inner_metadata.shards_metadata[shard as usize].blooms_key_count.read().unwrap();
                let locked_shard_bloom_len = GLOBAL_METADATA.inner_metadata.shards_metadata[shard as usize].bloom_bit_length.read().unwrap();
                
                let bit_len_per_key = (*locked_shard_bloom_len as f64) / (*locked_shard_key_count as f64);

                if bit_len_per_key <= BLOOM_LENGTH_PER_KEY {
                    rehash_shards.push(shard);
                }
            }
            inner_insert_rehash_queue(&rehash_shards);
        },
    }
}
 */




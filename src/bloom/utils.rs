use anyhow::{Result, anyhow};

const JUMP_HASH_SHIFT: i32 = 33;
const JUMP_HASH_CONSTANT: u64 = 2862933555777941757;
const JUMP: f64 = (1u64 << 31) as f64;

pub fn raise_to_the_power_of_x(x: u32) -> u64{
    2_u64.pow(x)
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
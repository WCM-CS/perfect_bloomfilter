use bitvec::{bitvec, vec::BitVec};
use tokio::{fs, io::AsyncWriteExt, sync::{Mutex, RwLock}, time::sleep};
use std::{collections::HashMap, io::Cursor, sync::Arc, time::Duration};
use anyhow::{anyhow, Result};


const INNER_BLOOM_HASH_FAMILY_SIZE: u32 = 13;

const MIN_OUTER_BITS_PER_KEY: f32 = 9.2;
const MIN_INNER_BITS_PER_KEY: f32 = 19.2;

static HASH_SEED_SELECTION: [u32; 5] = [9, 223, 372, 630, 854];

// default construct for the bloom filters length
const NUM_U64_ELEMENTS: usize = ((BLOOM_DEFAULT_LENGTH + 63) / 64) as usize; //ceiling div

//const STATIC_VECTOR_LENGTH_COMPACT: u32 = 2048; // n < 100 million
const STATIC_VECTOR_LENGTH_STANDARD: usize = 4096; // n > 100 million && n < 800 million
//const STATIC_VECTOR_LENGTH_BIOG_DATA: u32 = 8192; // n > 800 million

//14 start at 14 which faislonce you get to 650k then we ned to resize te bloomfilters
static BLOOM_DEFAULT_LENGTH: usize = (2_u32.pow(14)) as usize;

enum DiskCache {
    C1,
    C2,
    C3
}

// Define the struct for the array of blooms
pub struct PerfectBloomFilterInner {
    shard_array1: Arc<RwLock<ShardArray>>,
    metadata: Arc<RwLock<ShardArrMeta>>,
    disk_cache_array1: Arc<Mutex<HashMap<u32, Vec<String>>>>
}

impl PerfectBloomFilterInner {
    async fn rehash_system (&self) -> Result<()> {
        let bits_per_key_min: f64 = 15.5;

        loop {
            sleep(Duration::from_secs(5)).await;
        
            let metadata = self.metadata.read().await;
            let bloom_key_count = &metadata.blooms_key_count;
            let bloom_bit_length = &metadata.bloom_bit_length;

            let mut shards_to_resize: Vec<u32> = vec![];

            for (shard, count)  in  bloom_key_count {
                // get bit vec len per the shard
                let bitvec_len = bloom_bit_length.get(shard).copied().unwrap_or(0);

                if *count > 0 {
                    let bits_per_key = bitvec_len as f64 / *count as f64;

                    if bits_per_key < bits_per_key_min {
                        shards_to_resize.push(*shard);
                    }
                }
            }

            drop(metadata);

            if !shards_to_resize.is_empty() {

                for shard in shards_to_resize {
                    tracing::warn!("need to resize for shard: {shard}");
                }
                
            }
        }
    }
}

async fn write_disk_io_cache(data: HashMap<u32, Vec<String>>, cache_type: DiskCache) -> Result<()> {
    let node = match cache_type {
        DiskCache::C1 => "node1",
        DiskCache::C2 => "node2",
        DiskCache::C3 => "node3",
    };


    for (shard, keys) in data.into_iter() {
        if let Some(parent) = std::path::Path::new("./pbf_data/init.txt").parent() {
            fs::create_dir_all(parent).await?;  // Create directory if it does not exist
        }
        let file_name = format!("./pbf_data/{}_{}.txt", node, shard);
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_name)
            .await?;

        let mut writer = tokio::io::BufWriter::new(file);

        for k in keys {
            writer.write_all(k.as_bytes()).await?;
            writer.write_all(b"\n").await?;
        }

        writer.flush().await?;
    };


    Ok(())

}


impl PerfectBloomFilterInner {
    async fn drain_cache(&mut self) -> Result<HashMap<u32, Vec<String>>> {
        let mut locked_cache = self.disk_cache_array1.lock().await;
        let data = std::mem::take(&mut *locked_cache); // deref the mutex guaard to take ownership and drain cache
        Ok(data)
    }
}

pub struct PerfectBloomFilter {
    node1: Arc<PerfectBloomFilterInner>,
    node2: Option<Arc<Mutex<PerfectBloomFilterInner>>>,
    node3: Option<Arc<Mutex<PerfectBloomFilterInner>>>,
}

impl PerfectBloomFilter {
    pub fn new() -> Result<Self>{

        let user_filter = Arc::new(PerfectBloomFilterInner {
            shard_array1: Arc::new(RwLock::new(ShardArray::default())),
            metadata: Arc::new(RwLock::new(ShardArrMeta::default())),
            disk_cache_array1: Arc::new(Mutex::new(HashMap::new())),
        });    

        let drain_filter = Arc::clone(&user_filter);
        let rehash_filter = Arc::clone(&user_filter);

        tokio::spawn(async move {
            sleep(Duration::from_secs(5)).await;

            let mut locked_cache = drain_filter.disk_cache_array1.lock().await;
            let data = std::mem::take(&mut *locked_cache);
            drop(locked_cache);

            write_disk_io_cache(data, DiskCache::C1).await.unwrap();
            
        });

        tokio::spawn(async move {
            let _ = rehash_filter.rehash_system().await;
        });

        Ok(Self {node1: user_filter, node2: None, node3: None})
    }


    

    pub async fn contains_insert(&mut self, key: &str) -> Result<bool> {

        // returns two shard idx/partitions/bloom filters
        let shards_idx = self.array_partition_hash(key).await?;
        // gets the keys hashes for the bloom filters 
        let inner_bloom_key_idx = self.bloom_hash(&shards_idx, key).await?;
        // checks if there is a collision and to what degree
        let collision_result = self.inner_bloom_check(&inner_bloom_key_idx).await?;

        let key_exists = match &collision_result {
            CollisionResult::Zero => {
                self.bloom_insert(&inner_bloom_key_idx).await;
                false
            }
             CollisionResult::PartialMinor(_) => {
                self.bloom_insert(&inner_bloom_key_idx).await;
                false
            }
            CollisionResult::PartialMajor(s1, s2) => {
                tracing::info!("Major collision for the bloom filter at shards: {:?} & {:?}", s1, s2);
                self.bloom_insert(&inner_bloom_key_idx).await;
                false
            }
            CollisionResult::Complete(_,_, _) => {
                true
            }
            CollisionResult::Error => {
                panic!("unexpected issue with collision result for key: {key}");
            }
        };

        if let Err(e) = self.insert_disk_io_cache(key, shards_idx) .await{
            tracing::warn!("Failed to insert data into the cache: {e}")
        }


        Ok(key_exists)

    }

    async fn insert_disk_io_cache(&mut self, key: &str, shards_idx: Vec<u32>) -> Result<()> {
        let mut inner = self.node1.disk_cache_array1.lock().await;
     
        for idx in shards_idx {
            inner
                .entry(idx)
                .or_default()
                .push(key.to_string());
        }

        Ok(())
    }

   

    async fn array_partition_hash(&self, key: &str) -> Result<Vec<u32>> {
        let h1 =
            murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[3])?;

        let high = (h1 >> 64) as u64;
        let low = h1 as u64;

        let p1 = self.jump_hash_partition(high, STATIC_VECTOR_LENGTH_STANDARD as u32)?;
        let mut p2 = self.jump_hash_partition(low, STATIC_VECTOR_LENGTH_STANDARD as u32)?;
        let mut p3 = self.jump_hash_partition(high ^ low, STATIC_VECTOR_LENGTH_STANDARD as u32)?;
        
        
        // utilize deterministic rehashing with xor principle
        if p1 == p2 {
            let rehashed = self.jump_hash_partition(high ^ low, STATIC_VECTOR_LENGTH_STANDARD as u32)?;

            p2 = if rehashed != p1 {
                rehashed
            } else {
                (p1 + ((low ^ high) % (STATIC_VECTOR_LENGTH_STANDARD as u64 - 1) + 1) as u32)
                    % STATIC_VECTOR_LENGTH_STANDARD as u32
            }
        }

        if p3 == p1 || p3 == p2 {
            p3 = (p3 +1) % STATIC_VECTOR_LENGTH_STANDARD as u32;
        }

        Ok(vec![p1, p2, p3])
    }

    // JumpConsistentHash function, rewritten from the jave implementation
    // Link to the repo: https://github.com/ssedano/jump-consistent-hash/blob/master/src/main/java/com/github/ssedano/hash/JumpConsistentHash.java
    fn jump_hash_partition(&self, key: u64, buckets: u32) -> Result<u32> {
        // Values based on the original java implementation
        let mut b: i128 = -1;
        let mut j: i128 = 0;

        const JUMP: f64 = (1u64 << 31) as f64;
        const CONSTANT: u64 = 2862933555777941757;

        let mut mut_key = key;

        while j < buckets as i128 {
            b = j;
            mut_key = mut_key.wrapping_mul(CONSTANT).wrapping_add(1);

            let exp = ((mut_key >> 33) + 1).max(1) as f64;
            j = ((b as f64 + 1.0) * (JUMP / exp)).floor() as i128;
        }

        Ok(b as u32)
    }

    async fn bloom_hash(
        &self,
        vector_partitions: &Vec<u32>,
        key: &str,
    ) -> Result<HashMap<u32, Vec<u64>>> {
        let mut inner_hash_list = HashMap::new();
        let inner = self.node1.metadata.read().await;

        for &vector_slot in vector_partitions {
            let bloom_length = match inner.bloom_bit_length.get(&vector_slot) {
                Some(&len) => len,
                None => return Err(anyhow!("Missing metadata for bloommmie {}", vector_slot)),
            };

            let mut key_hashes = Vec::with_capacity(INNER_BLOOM_HASH_FAMILY_SIZE as usize);
            let h1 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[3])?;
            let h2 =
                murmur3::murmur3_x64_128(&mut Cursor::new(key.as_bytes()), HASH_SEED_SELECTION[4])?;

            for idx in 0..INNER_BLOOM_HASH_FAMILY_SIZE {
                let idx_u128 = idx as u128;
                let index = (h1.wrapping_add(idx_u128.wrapping_mul(h2))) % bloom_length as u128;

                key_hashes.push(index as u64)
            }

            inner_hash_list.insert(vector_slot, key_hashes);
        }
        Ok(inner_hash_list)
    }


    async fn inner_bloom_check(&self, map: &HashMap<u32, Vec<u64>>) -> Result<CollisionResult> {
        let mut collision_map: HashMap<u32, bool> = HashMap::new();
        let inner = self.node1.shard_array1.read().await;

        for (index, map) in map.iter() {
            let key_exists = map
                .iter()
                .all(|&key| inner.data[*index as usize][key as usize]);
            collision_map.insert(*index, key_exists);
        }

        let collision_result = Self::check_collision(&collision_map)?;

        Ok(collision_result)
    }

    fn check_collision(map: &HashMap<u32, bool>) -> Result<CollisionResult> {
        let mut collided = vec![];

        map.iter().for_each(|(vector_indx, boolean)| {
            if *boolean {
                collided.push(vector_indx)
            }
        });

        let collision_result = match collided.as_slice() {
            [] => CollisionResult::Zero,
            [one] => CollisionResult::PartialMinor(**one),
            [one, two] => CollisionResult::PartialMajor(**one, **two),
            [one, two, three] => CollisionResult::Complete(**one, **two, **three),
            _ => CollisionResult::Error,
        };

        Ok(collision_result)
    }

    async fn bloom_insert(&mut self, inner_hashed: &HashMap<u32, Vec<u64>>) {
        let mut inner = self.node1.shard_array1.write().await;
        let mut inner_met = self.node1.metadata.write().await;
        for (vector_index, hashes) in inner_hashed {
            hashes.iter().for_each(|&bloom_index| {
                inner.data[*vector_index as usize].set(bloom_index as usize, true)
            });

            inner_met
                .blooms_key_count
                .entry(*vector_index)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    }



    

}

#[derive(Debug)]
struct ShardArray {
    data: [BitVec; STATIC_VECTOR_LENGTH_STANDARD as usize],
}

impl Default for ShardArray {
    fn default() -> Self {
        let empty_bitvec = bitvec![0; BLOOM_DEFAULT_LENGTH];
        let data = std::array::from_fn(|_| empty_bitvec.clone());
        ShardArray { data }
    }
}


#[derive(Debug)]
struct ShardArrMeta {
    blooms_key_count: HashMap<u32, u64>,
    bloom_bit_length: HashMap<u32, u64>,
}

impl Default for ShardArrMeta {
    fn default() -> Self {
         let mut key_count: HashMap<u32, u64> =
            HashMap::with_capacity(STATIC_VECTOR_LENGTH_STANDARD as usize);
        let mut bit_length: HashMap<u32, u64> =
            HashMap::with_capacity(STATIC_VECTOR_LENGTH_STANDARD as usize);

        for partition in 0..STATIC_VECTOR_LENGTH_STANDARD as u32 {
            key_count.insert(partition , 0);
            bit_length.insert(partition, BLOOM_DEFAULT_LENGTH as u64);
        }

        Self {
            blooms_key_count: key_count,
            bloom_bit_length: bit_length,
        }
    }
}

// make it a minor and major partial collision detection mechanism
enum CollisionResult {
    Zero,
    PartialMinor(u32),
    PartialMajor(u32, u32),
    Complete(u32, u32, u32),
    Error,
}



#[cfg(test)]
mod tests {
    use crate::shard_vec::PerfectBloomFilter;
    use std::{fs::OpenOptions};
    use std::io::Write;

    #[tokio::test]
    async fn test_filterb() {
        let temp_key1 = "First_Value".to_string(); // false
        let temp_key2 = "First_Value".to_string(); // true
        let temp_key3 = "First_Value_new".to_string(); // false
        let temp_key4 = "First_Value".to_string(); // true
        let temp_key5 = "First_Value_new".to_string(); // true
        let temp_key6 = "First_Value_newa".to_string(); // false

        let mut pf = PerfectBloomFilter::new().unwrap();

        let result_a = pf.contains_insert(&temp_key1).await.unwrap();
        let result_b = pf.contains_insert(&temp_key2).await.unwrap();
        let result_c = pf.contains_insert(&temp_key3).await.unwrap();
        let result_d = pf.contains_insert(&temp_key4).await.unwrap();
        let result_e = pf.contains_insert(&temp_key5).await.unwrap();
        let result_f = pf.contains_insert(&temp_key6).await.unwrap();

        assert_eq!(result_a, false);
        assert_eq!(result_b, true);
        assert_eq!(result_c, false);
        assert_eq!(result_d, true);
        assert_eq!(result_e, true);
        assert_eq!(result_f, false);
    }

    // inner blooms sized ar 2^20 returns a false negative at 22 million
    // rasie this to 2^22 and we get false positive a 550 million
    #[tokio::test]
    async fn test_filter_loop_a()  {
        tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .init();
        let count = 1_000_000;
        let mut pf = PerfectBloomFilter::new().unwrap();
        

        tracing::info!("Starting Insert phase 1");
        for i in 0..count {
            let key = i.to_string();

            let was_present = {
                pf.contains_insert(&key).await.expect("Insert failed unexpectedly")
            };

            /*
             */
            if was_present {
                tracing::error!("Value was a false positive: {key}, Jikes buster, you suck");
                //locked_pf.dump_inner_bloom_data();
                let key_copy = key.to_owned();

                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("false_positives.txt")
                    .expect("Failed to open log file");

                writeln!(file, "{key_copy}").expect("Failed to write key to file");
            }


            assert_eq!(was_present, false);
        }
        tracing::info!("Completed Insert phase 1");
        //let mut locked_pf = pf.lock().await;

        tracing::info!("Starting confirmation phase 1");
        for i in 0..count {
            let key = i.to_string();
            let was_present = pf
                .contains_insert(&key)
                .await
                .expect("Insert failed unexpectedly");

            assert_eq!(was_present, true);
        }
        tracing::info!("Completed confirmation phase 1");

        tracing::info!("Starting insertion phase 2");
      

    }
}

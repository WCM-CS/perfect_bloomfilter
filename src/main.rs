use anyhow::{Error, Result, anyhow};
use bitvec::bitvec;
use bitvec::ptr::BitRef;
use bitvec::vec::BitVec;
use core::hash;
use std::collections::{HashMap, HashSet};
use murmur3::murmur3_32;
use once_cell::sync::Lazy;
use std::default;
use std::io::Cursor;
use std::sync::Mutex;

type BigBloomType = BitVec;
type PartitionedBloomsType = Vec<BitVec>;
const STATIC_VECTOR_LENGTH: u32 = 4096;
const STATIC_VECTOR_PARTITIONS: usize = 2;

const STATIC_OUTER_BLOOM_SIZE: u32 = 32;
const INITIAL_INNER_BLOOM_SIZE: u32 = 20;

const STATIC_OUTER_BLOOM_FULL_LENGTH: u64 = 2_u64.pow(STATIC_OUTER_BLOOM_SIZE);
const STATIC_INNER_BLOOM_FULL_LENGTH: u64 = 2_u64.pow(INITIAL_INNER_BLOOM_SIZE);

const OUTER_BLOOM_HASH_FAMILY_SIZE: u32 = 7;
const INNER_BLOOM_HASH_FAMILY_SIZE: u32 = 13;

const MIN_INNER_BITS_PER_KEY: f32 = 19.2;

static BIG_BLOOM: Lazy<Mutex<BigBloom>> = Lazy::new(|| Mutex::new(BigBloom::default()));
static PARTITIONED_BLOOMS: Lazy<Mutex<Dissolver>> = Lazy::new(|| Mutex::new(Dissolver::default()));
static META_DATA: Lazy<Mutex<MetaDataBlooms>> = Lazy::new(|| Mutex::new(MetaDataBlooms::default()));

fn main() {


    let temp_key1 = "First_Value".to_string(); // false
    let temp_key2 = "First_Value".to_string(); // true 
    let temp_key3 = "First_Value_new".to_string(); // false

    match process(temp_key1) {
        Ok(key_exists) => println!("Key exists in the filter: {key_exists}"),
        Err(err) => println!("Error with the perfect bloom system: {err}"),
    }
    match process(temp_key2) {
        Ok(key_exists) => println!("Key exists in the filter: {key_exists}"),
        Err(err) => println!("Error with the perfect bloom system: {err}"),
    }
    match process(temp_key3) {
        Ok(key_exists) => println!("Key exists in the filter: {key_exists}"),
        Err(err) => println!("Error with the perfect bloom system: {err}"),
    }
}

fn process(key: String) -> Result<bool>{
    // first bloom filter check done
    // phase 1 of filtering process
    //let key_exists_in_filter_a = false;
    let key_exists = big_bloom_check(&key)?; // filter !

    // if the filter says no we stop and dont do anything else
 
    // hash into dissolver
    let collision_result = dissolver_check(&key)?;

    Ok(key_exists && collision_result)
}

fn big_bloom_check(key: &str) -> Result<bool> {
    // lock access to the big bloom filter
    let mut locked_big_bloom = BIG_BLOOM.lock().unwrap();

    // hash key into the
    let key_partitions = locked_big_bloom.hash(&key)?;

    let key_exists: bool = locked_big_bloom.check(&key_partitions)?;

    if !key_exists { 
        locked_big_bloom.insert(&key_partitions);

        return Ok(false)
    } else {
        return Ok(true)
    }
}
enum CollisionResult {
    Zero, 
    Partial(u32),
    Complete(u32, u32),
    Error
}

fn dissolver_check(key: &str) -> Result<bool> {
    let mut locked_partitioned_blooms = PARTITIONED_BLOOMS.lock().unwrap();

    // find it's two vector partitions
    let vector_partitions = locked_partitioned_blooms.vector_partition_hash(&key)?;

    // use vector partitions to find its blooms,, check the blooms
    let inner_bloom_hashes = locked_partitioned_blooms.inner_bloom_hash(&vector_partitions, &key)?;

    // if one or no collisions its not here
    let collision_map = locked_partitioned_blooms.inner_bloom_check(&inner_bloom_hashes)?;
    let collision_result: CollisionResult = check_collision(collision_map)?;


    // insert when zero and partial collide, return false for zero and partial, return true for complete, return eror for error
    // rehash on partial
    match collision_result {
        CollisionResult::Zero => {
            // spawn a green background tokio thread to handle the inserts non blocking and return the bool first
            locked_partitioned_blooms.inner_bloom_insert(&inner_bloom_hashes); // Element does not exist in filter
            return Ok(false);
        }
        CollisionResult::Partial(partition) => { // partial collision, need to rehash bloom at vector index
            locked_partitioned_blooms.inner_bloom_insert(&inner_bloom_hashes);
            // ctrigger rehash on the gicen partition
            return Ok(false);
        }
        CollisionResult::Complete(partition_a,partition_b) => {  // colplete collision, meanig the key exists in the set
            // check if eitehr partition requeres a rehash or resizing
            return Ok(true);
        },
        CollisionResult::Error => {// failure to properly interpret results something went wrong, im in a teapot
            return Err(anyhow!("Unexpected collision error"))
        }        
    }
    
}

fn check_collision(map: HashMap<u32, bool>) -> Result<CollisionResult>{
    // if both collide it is there
    let mut collided = vec![];

    map.iter().for_each(|(vector_indx, boolean)| {
        if *boolean {
            collided.push(vector_indx)
        }
    });

    let collision_result = match collided.as_slice() {
        [] => CollisionResult::Zero,
        [one] => CollisionResult::Partial(**one),
        [one, two] => CollisionResult::Complete(**one, **two),
        _ => CollisionResult::Error
    };

    Ok(collision_result)
}

struct MetaDataBlooms {
    big_bloom_key_count: u64, // keys inserted
    inner_blooms_key_count: HashMap<u32, u64>, // vector partition, keys inserted

    big_bloom_bit_length: u64, // bit vec length
    inner_bloom_bit_length: HashMap<u32, u64>, // vector partition, bit vec length
}

impl Default for MetaDataBlooms {
    fn default() -> Self {
        // craft meta data for inner blooms
        let mut key_count: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH as usize);
        let mut bit_length: HashMap<u32, u64> = HashMap::with_capacity(STATIC_VECTOR_LENGTH as usize);

        for partition in 0..STATIC_VECTOR_LENGTH {
            key_count.insert(partition, 0);
            bit_length.insert(partition, STATIC_INNER_BLOOM_FULL_LENGTH);
        }

        Self {
            big_bloom_key_count: 0,
            inner_blooms_key_count: key_count,

            big_bloom_bit_length: STATIC_OUTER_BLOOM_FULL_LENGTH,
            inner_bloom_bit_length: bit_length,
        }
    }
}

struct BigBloom {
    body: BitVec,
}

impl Default for BigBloom {
    fn default() -> Self {
        Self {
            body: bitvec![0; STATIC_OUTER_BLOOM_FULL_LENGTH as usize],
        }
    }
}

impl BigBloom {
    fn hash(&self, key: &str) -> Result<Vec<u64>> {
        let mut hash_index_list: Vec<u64> =
            Vec::with_capacity(OUTER_BLOOM_HASH_FAMILY_SIZE as usize);

        for seed in 0..OUTER_BLOOM_HASH_FAMILY_SIZE {
            let hash_result = murmur3_32(&mut Cursor::new(&key.as_bytes()), seed).unwrap();
            hash_index_list.push(hash_result as u64 % STATIC_OUTER_BLOOM_FULL_LENGTH);
        }

        Ok(hash_index_list)
    }

    fn check(&self, key_partitions: &Vec<u64>) -> Result<bool> {
        let key_exists = key_partitions
            .iter()
            .all(|&key| self.body[key as usize]);

        Ok(key_exists)
    }

    fn insert(&mut self, key_partitions: &Vec<u64>) {
        key_partitions.iter()
            .for_each(|&bloom_index| {
                self.body.set(bloom_index as usize, true)
            }
        );
    }
}

#[derive(Debug)]
struct Dissolver {
    body: Vec<BitVec>,
}

impl Default for Dissolver {
    fn default() -> Self {
        Self {
            body: vec![bitvec![0; STATIC_INNER_BLOOM_FULL_LENGTH as usize]; STATIC_VECTOR_LENGTH as usize],
        }
    }
}

impl Dissolver {
    fn vector_partition_hash(&self, key: &str) -> Result<Vec<u32>> {
        let mut partitions = HashSet::with_capacity(STATIC_VECTOR_PARTITIONS);

        for seed in 0..STATIC_VECTOR_PARTITIONS {
            let hash_result = murmur3_32(&mut Cursor::new(key.as_bytes()), seed as u32)?;
            partitions.insert(hash_result % STATIC_VECTOR_LENGTH);
        }

        /*let mut seed = 0;
        while partitions.len() < STATIC_VECTOR_PARTITIONS {
            let hash_result = murmur3_32(&mut Cursor::new(key.as_bytes()), seed)?;
            partitions.insert(hash_result % STATIC_VECTOR_LENGTH);
            seed += 1;
        } */
        
        
        Ok(partitions.into_iter().collect())
    }

    /*

impl Dissolver {
    fn vector_partition_hash(&self, key: &str) -> Result<Vec<u32>> {
        let mut partitions = HashSet::with_capacity(2);

        let mut seed = 0;
        while partitions.len() < 2 {
            let hash_result = murmur3_32(&mut Cursor::new(key.as_bytes()), seed)?;
            partitions.insert(hash_result % STATIC_VECTOR_LENGTH);
            seed += 1;
        }
        
        Ok(partitions.into_iter().collect())
    }

    // returns map of vec index to inner blooms hash indexes in the vector slot 
    fn inner_bloom_hash(&self, vector_partitions: &Vec<u32>, key: &str) -> Result<HashMap<u32, Vec<u64>>> {
        let mut inner_hash_list: HashMap<u32, Vec<u64>> = HashMap::new();

        let locked_meta_data = META_DATA.lock().unwrap();

        // for each vector partition get the correlating inner bloom bit length
        let bit_lengths: Vec<u64> = vector_partitions.iter().filter_map(|&vector_slot| 
           locked_meta_data.inner_bloom_bit_length.get(&vector_slot).copied()
        ).collect();

        // oiterate through the vector partitions getting the hashes for the inner blooms for that partition
        for index in 0..=1 { // maybe dont hardcode vector slots bit for now its fine
            let vector_slot = vector_partitions[index];
            let bloom_length = bit_lengths[index];

            let mut key_hashes = Vec::with_capacity(INNER_BLOOM_HASH_FAMILY_SIZE as usize);

            for seed in 0..INNER_BLOOM_HASH_FAMILY_SIZE {
                let hash_result = murmur3_32(&mut Cursor::new(&key.as_bytes()), seed).unwrap();
                let final_hash = hash_result as u64 % bloom_length;
                
                key_hashes.push(final_hash)
            }
            inner_hash_list.insert(vector_slot, key_hashes);
        }
        
        return Ok(inner_hash_list)
    }
     */
    // returns map of vec index to inner blooms hash indexes in the vector slot 
    fn inner_bloom_hash(
        &self,
        vector_partitions: &Vec<u32>,
        key: &str
    ) -> Result<HashMap<u32, Vec<u64>>> {
        let mut inner_hash_list = HashMap::new();
        let locked_meta_data = META_DATA.lock().unwrap();

        for &vector_slot in vector_partitions {
            // Attempt to fetch the bloom bit length for this partition
            let bloom_length = match locked_meta_data.inner_bloom_bit_length.get(&vector_slot) {
                Some(&len) => len,
                None => return Err(anyhow!("Missing metadata for vector_slot {}", vector_slot)),
            };

            let mut key_hashes = Vec::with_capacity(INNER_BLOOM_HASH_FAMILY_SIZE as usize);
            for seed in 0..INNER_BLOOM_HASH_FAMILY_SIZE {
                let hash_result = murmur3_32(&mut Cursor::new(key.as_bytes()), seed)?;
                let final_hash = hash_result as u64 % bloom_length;
                key_hashes.push(final_hash)
            }
            inner_hash_list.insert(vector_slot, key_hashes);
        }
        Ok(inner_hash_list)
    }

    // returns a vector of the vec partition to whetehr or not a full blown collision occured
    fn inner_bloom_check(&self, map: &HashMap<u32, Vec<u64>>) -> Result<HashMap<u32, bool>> {
        let mut collision_map: HashMap<u32, bool> = HashMap::new();
        
        for (index, map) in map.iter() { 
            // returns true if all slots collide else if any fail returns false
            let key_exists = map.iter().all(|&key|
                self.body[*index as usize][key as usize]
            );
            collision_map.insert(*index, key_exists);
        }

        Ok(collision_map)

    }
    // use get dont just [] index into structures like get() or get_mut()
    fn inner_bloom_insert(&mut self, inner_hashed: &HashMap<u32, Vec<u64>>) {
        for (vector_index, hashes) in inner_hashed {
            hashes.iter().for_each(|&bloom_index| {
                self.body[*vector_index as usize].set(bloom_index as usize, true)
            
            });
        }
    }
}


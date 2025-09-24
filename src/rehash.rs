use crate::{internals::{ACTIVE_STATE, GLOBAL_PBF, REHASH_QUEUE}, io::force_drain_for_rehash_shard, utils::FilterType};

use anyhow::Result;

const REHASH_BITVEC_THRESHOLD: f64 = 19.2;
const REHASH_BATCH_SIZE: u16 = 5;


fn rehash_shards(filter_type: &FilterType) -> Result<()>{
    // atomically flag shard for rehashing

    loop {
        let (rehash_state, drain_state, rehash_queue) = match filter_type {
            FilterType::Outer => (&ACTIVE_STATE.outer_rehash_state, &ACTIVE_STATE.outer_drain_state, &REHASH_QUEUE.outer_queue),
            FilterType::Inner => (&ACTIVE_STATE.inner_rehash_state, &ACTIVE_STATE.inner_drain_state, &REHASH_QUEUE.inner_queue)
        };

        let shard = rehash_queue.write().unwrap().pop_front();

        if let Some(shard_idx) = shard {
            // set the rehash atomic to true, this prevents new inserts 
            ACTIVE_STATE.outer_rehash_state[shard_idx as usize].store(true,std::sync::atomic::Ordering::SeqCst);

            // force drain the cahce this ensure consistency that the keys written to disk match the key ionserted before rehashing starts
            force_drain_for_rehash_shard(filter_type, shard_idx)?;


        }





    }


     // pop shardfrom the vec
     // add it to rehashing state
     // maek sure its disk io cache is empty, if not then drain it


     







    // ensure the shards io cache is empty iof not then drain it

    //read the strinsg from the shars files, insert them into a bloomfilter


    // atomically swap the filters and update filter metadata
    // remove it form rehash queue list after its done 
    Ok(())

}




fn bloom_rehash() {

}


pub fn metadata_computation(filter_type: &FilterType, shard_idx: &Vec<u32>) {
    let (shard_vec, rehash_queue, rehash_list, active_rehash) = match filter_type {
        FilterType::Outer => (&GLOBAL_PBF.outer_filter.shard_vector, &REHASH_QUEUE.outer_queue, &REHASH_QUEUE.outer_lookup_list, &ACTIVE_STATE.outer_rehash_state),
        FilterType::Inner =>  (&GLOBAL_PBF.inner_filter.shard_vector, &REHASH_QUEUE.inner_queue, &REHASH_QUEUE.inner_lookup_list, &ACTIVE_STATE.inner_rehash_state)
    };

    for shard in shard_idx {
        let shard_key_count = shard_vec[*shard as usize].key_count.read().unwrap();
        let shard_bloom_length = shard_vec[*shard as usize].bloom_length.read().unwrap();

        if (*shard_bloom_length as f64 / *shard_key_count as f64) < REHASH_BITVEC_THRESHOLD {
            if rehash_list.read().unwrap().contains(&shard){ // this is a linear scan and make not perfrom well overtime maye change this to a hashset
                continue
            } else if active_rehash[*shard as usize].load(std::sync::atomic::Ordering::SeqCst) {
                continue
            } else {
                rehash_list.write().unwrap().insert(*shard);
                rehash_queue.write().unwrap().push_back(*shard);
            }
        }
    }

}
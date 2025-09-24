use std::{collections::HashMap, fs, io::{self, Write}, sync::{Arc, Mutex}};

use crate::{hash::ARRAY_SHARDS, internals::{ACTIVE_STATE, GLOBAL_PBF}, utils::FilterType};
use anyhow::Result;
use csv::Writer;



pub fn insert_into_cache(key: &str, filter_type: &FilterType, shards: &Vec<u32>) -> Result<()> {
    let shard_vec = match filter_type {
        FilterType::Outer => &GLOBAL_PBF.outer_filter.shard_vector,
        FilterType::Inner => &GLOBAL_PBF.inner_filter.shard_vector
    };

    shards.iter().for_each(|shard| {
        shard_vec[*shard as usize].output_cache.write().unwrap().push(key.to_string());
    });

    Ok(())
}


pub fn drain_cache(filter_type: FilterType) -> Result<()> {
    let (shard_vec, rehash_state, drain_state) = match filter_type {
        FilterType::Outer => {
            (&GLOBAL_PBF.outer_filter.shard_vector,
            &ACTIVE_STATE.outer_rehash_state, 
            &ACTIVE_STATE.outer_drain_state)
        }
        
        
        FilterType::Inner => {
            (&GLOBAL_PBF.inner_filter.shard_vector,
            &ACTIVE_STATE.inner_rehash_state, 
            &ACTIVE_STATE.inner_drain_state)
        }
    };

    let mut drain_map: HashMap<u32, Vec<String>> = HashMap::new();

    for (idx, shard) in shard_vec.iter().enumerate() {
        if rehash_state[idx].load(std::sync::atomic::Ordering::SeqCst) {
            continue;
        } 
        else if shard.output_cache.read().unwrap().is_empty() {
            continue;
        } else {
            drain_state[idx].store(true, std::sync::atomic::Ordering::SeqCst);
            let data = std::mem::take(&mut *shard.output_cache.write().unwrap());
            drain_map.insert(idx as u32, data);
        }
    }

    write_disk_io_cache(&drain_map, &filter_type)?;

    drain_map.keys().into_iter().for_each(|&shard| 
        drain_state[shard as usize].store(true, std::sync::atomic::Ordering::SeqCst
    ));
    
    Ok(())
}

pub fn force_drain_for_rehash_shard(filter_type: &FilterType, shard_idx: u32) -> Result<()> {
    let (shard_vec, drain_state) = match filter_type {
        FilterType::Outer => (
            &GLOBAL_PBF.outer_filter.shard_vector,
            &ACTIVE_STATE.outer_drain_state,
        ),
        FilterType::Inner => (
            &GLOBAL_PBF.inner_filter.shard_vector,
            &ACTIVE_STATE.inner_drain_state,
        ),
    };

    let shard = &shard_vec[shard_idx as usize];
    
    // Skip if output cache is empty
    if shard.output_cache.read().unwrap().is_empty() {
        return Ok(());
    }

    // Take cache data to drain
    let data = std::mem::take(&mut *shard.output_cache.write().unwrap());

    // Prepare map to write cache data for this single shard
    let mut drain_map = HashMap::new();
    drain_map.insert(shard_idx, data);

    // Write drained data to disk
    write_disk_io_cache(&drain_map, filter_type)?;


    Ok(())
}


pub fn write_disk_io_cache(drain_map: &HashMap<u32, Vec<String>>, filter_type: &FilterType) -> Result<()> {
    let node_type = match filter_type {
        FilterType::Outer => "outer".to_string(),
        FilterType::Inner => "inner".to_string()
    };


    for (shard, keys) in drain_map{
        if let Some(parent) = std::path::Path::new("./data/pbf_data/init.txt").parent() {
            fs::create_dir_all(parent)?; // Create directory if it does not exist
        }
        let file_name = format!("./data/pbf_data/{}_{}.txt", node_type, shard);
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_name)?;

        let mut writer = io::BufWriter::new(file);

        for k in keys {
            writer.write_all(k.as_bytes())?;
            writer.write_all(b"\n")?;
        }

        writer.flush()?;

    }

    Ok(())
}

pub fn dump_metadata() {
    let file_path = "./data/metadata.csv";
     let mut wtr = Writer::from_path(file_path).unwrap();

    // Write header
    wtr.write_record(&["filter_type", "shard_index", "key_count"]).unwrap();

    // Dump outer filter key counts
    for (idx, shard) in GLOBAL_PBF.outer_filter.shard_vector.iter().enumerate() {
        let key_count = *shard.key_count.read().unwrap();
        wtr.write_record(&["outer", &idx.to_string(), &key_count.to_string()]).unwrap();
    }

    // Dump inner filter key counts
    for (idx, shard) in GLOBAL_PBF.inner_filter.shard_vector.iter().enumerate() {
        let key_count = *shard.key_count.read().unwrap();
        wtr.write_record(&["inner", &idx.to_string(), &key_count.to_string()]).unwrap();
    }

    wtr.flush().unwrap();
}
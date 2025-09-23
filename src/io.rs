use std::{collections::HashMap, fs, io::{self, Write}};

use crate::{hash::ARRAY_SHARDS, internals::GLOBAL_PBF, utils::FilterType};
use anyhow::Result;
use csv::Writer;



pub fn insert_into_cache(key: &str, filter_type: &FilterType, shards: Vec<u32>) -> Result<()> {
    match filter_type {
        FilterType::Outer => {
            shards.iter().for_each(|shard|{ 
                GLOBAL_PBF.outer_filter.shard_vector[*shard as usize].output_cache.write().unwrap().push(key.to_string());
            });
        },
        FilterType::Inner => {
            shards.iter().for_each(|shard|{ 
                GLOBAL_PBF.inner_filter.shard_vector[*shard as usize].output_cache.write().unwrap().push(key.to_string());
            });
        }
    }

    Ok(())

}


pub fn drain_cache(filter_type: FilterType) -> Result<()> {
    let mut drain_map: HashMap<u32, Vec<String>> = HashMap::new();

    match filter_type {
        FilterType::Outer => {
            for shard in 0..ARRAY_SHARDS {
                if *GLOBAL_PBF.outer_filter.shard_vector[shard as usize].active_rehash.read().unwrap() {
                    continue
                } 
                else if GLOBAL_PBF.outer_filter.shard_vector[shard as usize].output_cache.read().unwrap().is_empty() {
                    continue
                } else {
                    let data = std::mem::take(&mut *GLOBAL_PBF.outer_filter.shard_vector[shard as usize].output_cache.write().unwrap());
                    drain_map.insert(shard, data);
                }
            }

            write_disk_io_cache(drain_map, filter_type)?;
        },
        FilterType::Inner => {
            for shard in 0..ARRAY_SHARDS {
                if *GLOBAL_PBF.inner_filter.shard_vector[shard as usize].active_rehash.read().unwrap() {
                    continue
                } 
                else if GLOBAL_PBF.inner_filter.shard_vector[shard as usize].output_cache.read().unwrap().is_empty() {
                    continue
                } else {
                    let data = std::mem::take(&mut *GLOBAL_PBF.inner_filter.shard_vector[shard as usize].output_cache.write().unwrap());
                    drain_map.insert(shard, data);
                }
            }

            write_disk_io_cache(drain_map, filter_type)?;
        },
    }

    

    Ok(())
}

pub fn write_disk_io_cache(drain_map: HashMap<u32, Vec<String>>, filter_type: FilterType) -> Result<()> {
    let node_type = match filter_type {
        FilterType::Outer => "outer".to_string(),
        FilterType::Inner => "inner".to_string()
    };

    for (shard, keys) in drain_map {
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
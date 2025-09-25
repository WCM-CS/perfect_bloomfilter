use std::{collections::HashMap, fs, io::{self, Write}, sync::{Arc, Mutex, RwLock, RwLockWriteGuard}};

use crate::{internals::{ShardData, GLOBAL_PBF}, utils::FilterType};
use anyhow::Result;
use once_cell::sync::{OnceCell};

/*
pub const GIB: usize = 1024 * 1024 * 1024;
pub static LMDB_ENV: OnceCell<Arc<Env>> = OnceCell::new();


pub fn lmdb_init(lmdb_map_size: usize) -> Result<()> {
    let env = unsafe { 
        EnvOpenOptions::new()
            .map_size(lmdb_map_size)
            .max_dbs(2)
            //.max_readers(10)
            .open("./data/")?
    };

    let mut wtxn = env.write_txn()?;
    let _db1: Database<&[u8], &[u8]> = env.create_database(&mut wtxn, Some("outer"))?;
    let _db2: Database<&[u8], &[u8]> = env.create_database(&mut wtxn, Some("inner"))?;
    wtxn.commit()?;


    LMDB_ENV.set(Arc::new(env)).unwrap();

    Ok(())
}



pub fn lmdb_insert(
    key: &str, 
    shards: &[u32], 
    filter_type: &FilterType, 
    locked_shards: &[RwLockWriteGuard<'_, ShardData>]
) -> Result<()> {
    let db_name = match filter_type {
        FilterType::Outer => "outer", 
        FilterType::Inner => "inner",
    };

    let lmdb_key_vec: Vec<String> = shards.iter().enumerate().map(|(idx, shard)| {
        let key_idx = &locked_shards[idx].key_count;
        format!("{}:{}", shard, key_idx)
    }).collect();


    let client = LMDB_ENV.get().unwrap().clone();

    let mut wtxn = client.write_txn()?;
    let db: Database<Str, Bytes> = client.create_database(&mut wtxn, Some(db_name))?;

    for lmdb_key in lmdb_key_vec {
        let value_bytes = key.as_bytes();
        db.put(&mut wtxn, &lmdb_key, value_bytes)?;
    }

    wtxn.commit()?;


    Ok(())
}

*/

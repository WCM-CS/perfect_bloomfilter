use std::{collections::{HashMap, HashSet, VecDeque}, fs, io::{self, BufRead, Cursor}, sync::{Arc, Mutex, RwLock, RwLockWriteGuard}};
use anyhow::{Result, anyhow};
//use dashmap::DashSet;
use once_cell::sync::Lazy;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use bitvec::vec::BitVec;
use bitvec::bitvec;

use crate::{bloom::{hash::{HASH_SEED_SELECTION, INNER_BLOOM_HASH_FAMILY_SIZE, OUTER_BLOOM_HASH_FAMILY_SIZE}, init::GLOBAL_PBF, utils::{hash_remainder, power_of_two}}, metadata::meta::GLOBAL_METADATA, FilterType};



pub const BLOOM_LENGTH_PER_KEY: f64 = 19.2;
const REHASH_BATCH_SIZE: u32 = 10;


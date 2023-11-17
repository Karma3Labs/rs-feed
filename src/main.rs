use itertools::Itertools;
use matrix::Matrix;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use storage::{get_data_path, CSVFileStorage, Storage};
use thiserror::Error;

mod matrix;
mod storage;

const LIMIT: u8 = 2;
const NUM_ITER: usize = 30;

#[derive(Debug, Error)]
pub enum FeedError {
    #[error("FileIOError: {0}")]
    FileIOError(String),

    #[error("IOError: {0}")]
    IOError(std::io::Error),
}

#[derive(Clone, Deserialize, Serialize)]
struct TxRecord {
    from: String,
    to: String,
    value: u32,
    timestamp: u32,
}

fn search_neighbours(peers: Vec<String>, records: Vec<TxRecord>, level: u32) -> Vec<String> {
    if level > 2 {
        return Vec::new();
    }
    let mut set: HashSet<String> = HashSet::new();
    peers.iter().for_each(|x| {
        set.insert(x.clone());

        let neighours_tx = records.iter().filter(|y| y.from == x.clone()).collect_vec();
        let neighbours = neighours_tx.iter().map(|v| v.to.clone()).collect_vec();
        let res = search_neighbours(neighbours, records.clone(), level + 1);
        res.iter().for_each(|v| {
            set.insert(v.clone());
        });
    });
    set.iter().cloned().collect_vec()
}

fn main() {
    let loader = CSVFileStorage::new(get_data_path().unwrap());
    let records: Vec<TxRecord> = loader.load().unwrap();
    let starting_address = "0x857c86988c53c1bc5bff75edfb97893fa40a8000".to_string();

    let vacinity = search_neighbours(vec![starting_address], records.clone(), 0);
    println!("vacinity: {:?}", vacinity);
    println!("");

    let index_mapping: HashMap<String, usize> = vacinity
        .iter()
        .enumerate()
        .map(|(x, y)| (y.to_owned(), x))
        .collect();
    println!("index_mapping: {:?}", index_mapping);
    println!("");

    let mut outgoing_arc_weights: HashMap<(String, String), u32> = HashMap::new();
    vacinity.iter().for_each(|x| {
        let outgoing = records.iter().filter(|y| y.from == x.clone()).collect_vec();
        outgoing.iter().for_each(|y| {
            *outgoing_arc_weights
                .entry((x.clone(), y.to.clone()))
                .or_insert(y.value) += y.value;
        })
    });
    println!("outgoing_arc_weights: {:?}", outgoing_arc_weights);
    println!("");

    let size = vacinity.len();
    let mut local_trust_matrix = vec![vec![0u32; size]; size];

    vacinity.iter().for_each(|x| {
        vacinity.iter().for_each(|y| {
            let weight = outgoing_arc_weights
                .get(&(x.clone(), y.clone()))
                .unwrap_or(&0);

            let from_index = index_mapping.get(x).unwrap();
            let to_index = index_mapping.get(y).unwrap();
            local_trust_matrix[*from_index][*to_index] = *weight;
        });
    });

    println!("local_trust_matrix = {:?}", local_trust_matrix);
    println!("");

    let normalised_local_matrix = local_trust_matrix
        .iter()
        .map(|x| {
            let sum: u32 = x.iter().sum();
            if sum == 0 {
                vec![1; x.len()]
            } else {
                x.clone()
            }
        })
        .map(|x| {
            let sum: u32 = x.iter().sum();
            x.iter()
                .map(move |y| f64::from(*y) / f64::from(sum))
                .collect_vec()
        })
        .collect_vec();

    let mut global_scores = vec![0.; size];
    global_scores[0] = 1.;

    let mat = Matrix::new(normalised_local_matrix);
    let mat_t = mat.transpose();

    for _ in 0..NUM_ITER {
        global_scores = mat_t.mul_add(global_scores);
    }

    println!("global_scores = {:?}", global_scores);
    println!(
        "global_scores_sum = {:?}",
        global_scores.iter().sum::<f64>()
    );
}

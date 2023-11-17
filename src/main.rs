use itertools::Itertools;
use matrix::Matrix;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

fn main() {
    let loader = CSVFileStorage::new(get_data_path().unwrap());
    let records: Vec<TxRecord> = loader.load().unwrap();
    let starting_address = "0x857c86988c53c1bc5bff75edfb97893fa40a8000";

    let mut vacinity = Vec::new();
    let mut todo = vec![starting_address];
    let mut distance = HashMap::new();
    distance.insert(starting_address, 0);

    while !todo.is_empty() {
        let a = todo.pop().unwrap();
        vacinity.push(starting_address.to_string());
        let txs = records.iter().filter(|&x| x.from == a).collect_vec();
        if distance[a] < LIMIT {
            txs.iter()
                .filter(|x| vacinity.contains(&x.to))
                .for_each(|x| {
                    todo.push(&x.to);
                    distance.entry(&x.to).and_modify(|c| *c += 1).or_insert(1);
                });
        }
    }

    let index_mapping: HashMap<String, usize> = vacinity
        .iter()
        .enumerate()
        .map(|(x, y)| (y.to_owned(), x))
        .collect();

    let mut outgoing_arc_weights: HashMap<(String, String), u32> = HashMap::new();
    vacinity.iter().for_each(|x| {
        let outgoing = records.iter().filter(|y| y.from == x.clone()).collect_vec();
        outgoing.iter().for_each(|y| {
            outgoing_arc_weights
                .entry((x.clone(), y.to.clone()))
                .and_modify(|v| *v += y.value)
                .or_insert(0);
        })
    });

    let size = vacinity.len();
    let mut local_trust_matrix = vec![vec![0u32; size]; size];

    vacinity.iter().for_each(|x| {
        vacinity.iter().for_each(|y| {
            let weight = outgoing_arc_weights.get(&(x.clone(), y.clone())).unwrap();

            let from_index = index_mapping.get(x).unwrap();
            let to_index = index_mapping.get(y).unwrap();
            local_trust_matrix[*from_index][*to_index] = *weight;
        });
    });

    let normalised_local_matrix = local_trust_matrix
        .iter()
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

    for _ in 0..NUM_ITER {
        global_scores = mat.mul_add(global_scores);
    }

    println!("{:?}", global_scores);
}

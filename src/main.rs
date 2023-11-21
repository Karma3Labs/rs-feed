use crate::matrix::Vector;
use itertools::Itertools;
use matrix::Matrix;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use storage::{get_data_path, CSVFileStorage, Storage};
use thiserror::Error;

mod matrix;
mod storage;

const LIMIT: u8 = 2;
const NUM_ITER: usize = 30;
const PRE_TRUST_WEIGHT: f64 = 0.2;
const TIME_DECAY_RATE: f64 = 0.7;

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
}

#[derive(Clone, Deserialize, Serialize)]
struct Phase1Result {
    vacinity: Vec<String>,
    global_scores: Vec<f64>,
    #[serde(skip_serializing)]
    index_mapping: HashMap<String, usize>,
}

fn search_neighbours(peers: Vec<String>, records: Vec<TxRecord>, level: u8) -> Vec<String> {
    if level > LIMIT {
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

fn phase1() -> Phase1Result {
    let loader = CSVFileStorage::new(get_data_path("eoa-to-eoa").unwrap());
    let records: Vec<TxRecord> = loader.load().unwrap();
    let a0_address = "0x857c86988c53c1bc5bff75edfb97893fa40a8000".to_string();

    let vacinity = search_neighbours(vec![a0_address.clone()], records.clone(), 0);
    let index_mapping: HashMap<String, usize> = vacinity
        .iter()
        .enumerate()
        .map(|(x, y)| (y.to_owned(), x))
        .collect();

    let mut outgoing_arc_weights: HashMap<(String, String), u32> = HashMap::new();
    vacinity.iter().for_each(|x| {
        let outgoing = records.iter().filter(|y| y.from == x.clone()).collect_vec();
        outgoing.iter().for_each(|y| {
            *outgoing_arc_weights
                .entry((x.clone(), y.to.clone()))
                .or_insert(y.value) += y.value;
        })
    });

    let size = vacinity.len();
    let mut local_trust_matrix = vec![vec![0u32; size]; size];

    vacinity.iter().for_each(|x| {
        vacinity.iter().filter(|y| **y != a0_address).for_each(|y| {
            let weight = outgoing_arc_weights
                .get(&(x.clone(), y.clone()))
                .unwrap_or(&0);

            let from_index = index_mapping.get(x).unwrap();
            let to_index = index_mapping.get(y).unwrap();
            local_trust_matrix[*from_index][*to_index] = *weight;
        });
    });

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
        .enumerate()
        .map(|(i, mut x)| {
            x[i] = 0;
            x
        })
        .map(|x| {
            let sum: u32 = x.iter().sum();
            x.iter()
                .map(move |y| f64::from(*y) / f64::from(sum))
                .collect_vec()
        })
        .collect_vec();

    let mut global_scores = vec![0.; size];
    let a0_index = index_mapping.get(&a0_address).unwrap();
    global_scores[*a0_index] = 1.;

    let mut pre_trust = vec![0.; size];
    pre_trust[*a0_index] = 1.;

    let mat = Matrix::new(normalised_local_matrix);
    let mat_t = mat.transpose();

    for _ in 0..NUM_ITER {
        global_scores = mat_t.mul_add(global_scores);
        let gs_vec = Vector::new(global_scores.clone()).mul(1. - PRE_TRUST_WEIGHT);
        let pt_vec = Vector::new(pre_trust.clone()).mul(PRE_TRUST_WEIGHT);
        global_scores = gs_vec.add(pt_vec).data();
    }

    Phase1Result {
        vacinity,
        global_scores,
        index_mapping,
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct TopicRecord {
    from: String,
    topic: String,
    timestamp: u32,
}

#[derive(Clone, Deserialize, Serialize)]
struct Phase2Result {
    relevant_topics: Vec<String>,
    topic_scores: Vec<f64>,
}

fn decay(current: f64, tx_time: f64) -> f64 {
    TIME_DECAY_RATE.powf(current - tx_time)
}

fn phase2(phase1_res: Phase1Result) -> Phase2Result {
    let loader = CSVFileStorage::new(get_data_path("eoa-to-topic").unwrap());
    let records: Vec<TopicRecord> = loader.load().unwrap();

    let start = SystemTime::now();
    let current_hours = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs_f64()
        / 3600.;
    println!("current_hours: {:?}", current_hours);

    let mut topic_age_mapping = HashMap::new();
    records.iter().for_each(|tr| {
        topic_age_mapping.insert((tr.from.clone(), tr.topic.clone()), tr.timestamp);
    });

    let mut score_mapping = HashMap::new();
    topic_age_mapping
        .iter()
        .for_each(|((from, topic), age_in_hours)| {
            let age_f64 = f64::from(*age_in_hours);
            if phase1_res.vacinity.contains(from) {
                let decay = decay(current_hours, age_f64);
                let from_index = phase1_res.index_mapping.get(from).unwrap();
                let score = phase1_res.global_scores[*from_index];
                let weighted_score = decay * score;
                *score_mapping
                    .entry(topic.to_owned())
                    .or_insert(weighted_score) += weighted_score;
            }
        });

    let topics = score_mapping.keys().cloned().collect_vec();
    let scores = score_mapping.values().cloned().collect_vec();

    Phase2Result {
        relevant_topics: topics,
        topic_scores: scores,
    }
}

fn main() {
    let phase1_res = phase1();
    let phase2_res = phase2(phase1_res.clone());

    println!("set: {:?}", phase1_res.vacinity);
    println!("set_scores: {:?}", phase1_res.global_scores);
    println!("topics: {:?}", phase2_res.relevant_topics);
    println!("topic_scores: {:?}", phase2_res.topic_scores);

    let peers_touples = phase1_res
        .vacinity
        .iter()
        .cloned()
        .zip(phase1_res.global_scores)
        .collect_vec();
    let topics_touples = phase2_res
        .relevant_topics
        .iter()
        .cloned()
        .zip(phase2_res.topic_scores)
        .collect_vec();

    let mut peers_loader = CSVFileStorage::new(get_data_path("peers").unwrap());
    let mut topics_loader = CSVFileStorage::new(get_data_path("topics").unwrap());
    peers_loader.save(peers_touples).unwrap();
    topics_loader.save(topics_touples).unwrap();
}

use crate::FeedError;
use csv::{ReaderBuilder, WriterBuilder};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::{env::current_dir, fs::File, io::BufReader, path::PathBuf};

pub trait Storage<T> {
    type Err;
    fn load(&self) -> Result<T, Self::Err>;
    fn save(&mut self, data: T) -> Result<(), Self::Err>;
}

pub struct CSVFileStorage {
    filepath: PathBuf,
}

impl CSVFileStorage {
    pub fn new(filepath: PathBuf) -> Self {
        Self { filepath }
    }
}

impl<T: Serialize + DeserializeOwned + Clone> Storage<Vec<T>> for CSVFileStorage {
    type Err = FeedError;

    fn load(&self) -> Result<Vec<T>, FeedError> {
        let file = File::open(&self.filepath).map_err(FeedError::IOError)?;
        let mut reader = ReaderBuilder::new().from_reader(BufReader::new(file));

        reader
            .deserialize()
            .map(|result| result.map_err(|e| FeedError::FileIOError(e.to_string())))
            .collect()
    }

    fn save(&mut self, data: Vec<T>) -> Result<(), FeedError> {
        let mut writer = WriterBuilder::new()
            .from_path(&self.filepath)
            .map_err(|e| FeedError::FileIOError(e.to_string()))?;

        for record in &data {
            writer
                .serialize(record)
                .map_err(|e| FeedError::FileIOError(e.to_string()))?;
        }

        writer
            .flush()
            .map_err(|e| FeedError::FileIOError(e.to_string()))?;

        Ok(())
    }
}

pub fn get_data_path(filename: &str) -> Result<PathBuf, FeedError> {
    current_dir()
        .map_err(FeedError::IOError)
        .map(|current_dir| current_dir.join("data"))
        .map(|data_dir| data_dir.join(format!("{}.csv", filename)))
}

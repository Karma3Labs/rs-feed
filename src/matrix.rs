use itertools::Itertools;

pub struct Matrix {
    dat: Vec<Vec<f64>>,
    size: (usize, usize),
}

impl Matrix {
    pub fn new(dat: Vec<Vec<f64>>) -> Self {
        let x_size = dat[0].len();
        let y_size = dat.len();

        Self {
            dat,
            size: (x_size, y_size),
        }
    }

    pub fn zeros(size: (usize, usize)) -> Self {
        Self {
            dat: vec![vec![0.; size.0]; size.1],
            size,
        }
    }

    pub fn transpose(self) -> Self {
        let mut transposed = Self::zeros(self.size);
        for i in 0..self.size.0 {
            for j in 0..self.size.1 {
                transposed.dat[i][j] = self.dat[j][i];
            }
        }

        transposed
    }

    pub fn mul_add(&self, b: Vec<f64>) -> Vec<f64> {
        assert_eq!(self.size.0, b.len());

        let mut res = vec![0.; b.len()];
        for i in 0..self.size.0 {
            for j in 0..self.size.1 {
                res[i] += self.dat[i][j] * b[j];
            }
        }

        res
    }
}

pub struct Vector {
    dat: Vec<f64>,
}

impl Vector {
    pub fn new(vector: Vec<f64>) -> Self {
        Self {
            dat: vector.clone(),
        }
    }

    pub fn data(self) -> Vec<f64> {
        self.dat
    }

    pub fn mul(&self, scalar: f64) -> Self {
        Self {
            dat: self.dat.iter().map(|x| x * scalar).collect(),
        }
    }

    pub fn add(&self, other: Self) -> Self {
        let res = self
            .dat
            .iter()
            .zip(other.dat)
            .map(|(x, y)| x + y)
            .collect_vec();
        Self { dat: res }
    }
}

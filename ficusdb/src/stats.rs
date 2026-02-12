pub struct StateDBStats {
    pub t_commit: f64,
    pub t_merkle_write: f64,
    pub t_merkle_commit: f64,
}

impl StateDBStats {
    pub fn new() -> Self {
        Self {
            t_commit: 0.0,
            t_merkle_write: 0.0,
            t_merkle_commit: 0.0,
        }
    }

    pub fn reset(&mut self) {
        self.t_commit = 0.0;
        self.t_merkle_write = 0.0;
        self.t_merkle_commit = 0.0;
    }

    pub fn print_stats(&mut self) {
        println!("statedb:\tt_commit\tt_merkle_write\tt_merkle_commit");
        println!(
            "\t{:.3}\t{:.3}\t{:.3}",
            self.t_commit, self.t_merkle_write, self.t_merkle_commit
        );
    }
}

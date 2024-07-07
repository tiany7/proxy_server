use anyhow::{Ok, Result};

// the start is the start id of the missing segment
// this is to count the number of missing segments
// will be used to perform RESTFUL recovery after a certain number of missing segments
#[derive(Debug, Clone)]
pub struct Segment {
    pub start: u64,
    pub end: u64,
}

impl Segment {
    pub fn try_new(start: u64, end: u64) -> Result<Self, anyhow::Error> {
        if end < start {
            Err(anyhow::anyhow!("End must be greater than start"))
        } else {
            Ok(Segment { start, end })
        }
    }

    pub fn new(start: u64, end: u64) -> Self {
        Segment { start, end }
    }

    pub fn size(&self) -> u64 {
        self.end - self.start
    }
}

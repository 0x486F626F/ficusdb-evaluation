use super::CleanPtr;

pub trait Backend {
    fn tail(&self) -> CleanPtr;
    fn read(&mut self, ptr: CleanPtr, len: usize) -> Vec<u8>;
    fn write(&mut self, ptr: CleanPtr, data: &[u8]);
    fn flush(&mut self);
    #[cfg(feature = "stats")]
    fn print_stats(&mut self);
}

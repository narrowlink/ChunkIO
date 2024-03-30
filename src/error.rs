use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChunkIOError {
    #[error("Chunk is empty")]
    InvalidChunk,
    #[error("Chunk is out of order")]
    OutOfOrder,
    #[error("IO error {0}")]
    Future(#[from] std::io::Error),
}

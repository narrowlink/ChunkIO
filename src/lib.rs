mod error;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use error::ChunkIOError;
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::{Decoder, Encoder, Framed},
};

pub struct ChunkIO<T>(Framed<T, ChunkIOProto>);

impl<T> ChunkIO<T> {
    pub fn new(io: T) -> ChunkIO<T>
    where
        T: AsyncRead + AsyncWrite,
    {
        ChunkIO(tokio_util::codec::Framed::new(io, Default::default()))
    }
}

impl<T> Stream for ChunkIO<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<Vec<u8>, ChunkIOError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

impl<T> Sink<Vec<u8>> for ChunkIO<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Error = ChunkIOError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        self.0.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.0.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.0.poll_close_unpin(cx)
    }
}

#[derive(Default)]
struct ChunkIOProto {
    current_index: (u64, u64), // send index, receive index
}

impl Decoder for ChunkIOProto {
    type Item = Vec<u8>;

    type Error = ChunkIOError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 2 {
            return Ok(None);
        }
        let index_pointer = (src[0] >> 4) as u64;
        let len_pointer = (src[0] & 0xf) as u64;
        if index_pointer > 8 || len_pointer > 8 || len_pointer == 0 {
            return Err(ChunkIOError::InvalidChunk);
        }
        if (src.len() as u64) < (len_pointer + index_pointer + 1) {
            return Ok(None);
        }
        let index = match index_pointer {
            0 => 0_u64,
            1 => u8::from_be_bytes([src[1]]) as u64,
            2 => u16::from_be_bytes([src[1], src[2]]) as u64,
            3..=4 => u32::from_be_bytes(
                src[1..index_pointer as usize]
                    .try_into()
                    .or(Err(ChunkIOError::InvalidChunk))?,
            ) as u64,
            5..=8 => u64::from_be_bytes(
                src[1..index_pointer as usize]
                    .try_into()
                    .or(Err(ChunkIOError::InvalidChunk))?,
            ),
            _ => return Err(ChunkIOError::InvalidChunk),
        };
        if self.current_index.1 != index {
            return Err(ChunkIOError::OutOfOrder);
        }

        let length = match len_pointer {
            1 => u8::from_be_bytes([src[1 + index_pointer as usize]]) as u64,
            2 => u16::from_be_bytes([
                src[1 + index_pointer as usize],
                src[2 + index_pointer as usize],
            ]) as u64,
            3..=4 => u32::from_be_bytes(
                src[1 + (index_pointer as usize)..((index_pointer + len_pointer) as usize)]
                    .try_into()
                    .or(Err(ChunkIOError::InvalidChunk))?,
            ) as u64,
            5..=8 => u64::from_be_bytes(
                src[1 + index_pointer as usize..(index_pointer + len_pointer) as usize]
                    .try_into()
                    .or(Err(ChunkIOError::InvalidChunk))?,
            ),
            _ => {
                return Err(ChunkIOError::InvalidChunk);
            }
        };
        if src.len() < (index_pointer + len_pointer + length + 1) as usize {
            Ok(None)
        } else {
            src.advance((index_pointer + len_pointer + 1) as usize);
            self.current_index.1 += length;
            Ok(Some(src.split_to(length as usize).to_vec()))
        }
    }
}

impl Encoder<Vec<u8>> for ChunkIOProto {
    type Error = ChunkIOError;

    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let index = self
            .current_index
            .0
            .to_be_bytes()
            .into_iter()
            .skip_while(|x| *x == 0)
            .collect::<Vec<u8>>();
        let length = item
            .len()
            .to_be_bytes()
            .into_iter()
            .skip_while(|x| *x == 0)
            .collect::<Vec<u8>>();
        dst.extend_from_slice(&[((index.len() as u8) << 4) | (length.len() as u8)]);
        dst.extend_from_slice(&index);
        dst.extend_from_slice(&length);
        dst.extend_from_slice(&item);
        self.current_index.0 += item.len() as u64;

        Ok(())
    }
}

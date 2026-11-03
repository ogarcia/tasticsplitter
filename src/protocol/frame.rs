use bytes::{Buf, BytesMut};

/// Stream framing of the Meshtastic client API (serial and TCP), the
/// same in both directions: START1, START2, payload length (u16 BE),
/// then a ToRadio / FromRadio protobuf.
pub const START1: u8 = 0x94;
pub const START2: u8 = 0xC3;
pub const HEADER_SIZE: usize = 4;
/// The firmware rejects anything longer; a header announcing more is
/// garbage that happened to contain START1 START2.
pub const MAX_PAYLOAD: usize = 512;

pub fn encode(payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u16;
    let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
    buf.extend_from_slice(&[START1, START2]);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Splits a byte stream into frame payloads. Bytes outside a frame are
/// dropped: on serial the firmware prints its debug log in plain text
/// between frames, and clients send a run of START2 to wake the device.
#[derive(Debug, Default)]
pub struct FrameParser {
    buffer: BytesMut,
}

impl FrameParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, data: &[u8]) -> Vec<Vec<u8>> {
        self.buffer.extend_from_slice(data);
        let mut frames = Vec::new();

        loop {
            // Resynchronize on START1.
            match self.buffer.iter().position(|&b| b == START1) {
                Some(i) => self.buffer.advance(i),
                None => {
                    self.buffer.clear();
                    break;
                }
            }
            if self.buffer.len() < HEADER_SIZE {
                break;
            }
            if self.buffer[1] != START2 {
                self.buffer.advance(1);
                continue;
            }
            let len = u16::from_be_bytes([self.buffer[2], self.buffer[3]]) as usize;
            if len > MAX_PAYLOAD {
                tracing::trace!(len, "oversized frame header, resynchronizing");
                self.buffer.advance(1);
                continue;
            }
            if self.buffer.len() < HEADER_SIZE + len {
                break;
            }
            self.buffer.advance(HEADER_SIZE);
            frames.push(self.buffer.split_to(len).to_vec());
        }

        frames
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let payload = vec![0x01, 0x02, 0x03];
        let mut parser = FrameParser::new();
        assert_eq!(parser.feed(&encode(&payload)), vec![payload]);
    }

    #[test]
    fn partial_feed() {
        let payload = vec![7u8; 300];
        let encoded = encode(&payload);
        let mut parser = FrameParser::new();
        assert!(parser.feed(&encoded[..1]).is_empty());
        assert!(parser.feed(&encoded[1..10]).is_empty());
        assert_eq!(parser.feed(&encoded[10..]), vec![payload]);
    }

    #[test]
    fn skips_debug_text_and_wake_bytes() {
        let mut stream = b"INFO | booting\r\n".to_vec();
        stream.extend_from_slice(&[START2; 32]);
        stream.extend_from_slice(&encode(&[1]));
        stream.extend_from_slice(b"DEBUG | \x94 stray\n");
        stream.extend_from_slice(&encode(&[2, 3]));
        let mut parser = FrameParser::new();
        assert_eq!(parser.feed(&stream), vec![vec![1], vec![2, 3]]);
    }

    #[test]
    fn oversized_header_is_garbage() {
        let mut stream = vec![START1, START2, 0xFF, 0xFF];
        stream.extend_from_slice(&encode(&[9]));
        let mut parser = FrameParser::new();
        assert_eq!(parser.feed(&stream), vec![vec![9]]);
    }
}

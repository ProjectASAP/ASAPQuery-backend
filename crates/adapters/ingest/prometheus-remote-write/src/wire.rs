//! A protobuf reader over borrowed bytes, for decoders that walk a request
//! in place instead of building its messages: Remote Write's, and the
//! other ingest decoders' (`otlp`). A malformed message is a
//! [`RemoteWriteError::Protobuf`].

use crate::RemoteWriteError;

/// One protobuf field value.
pub enum Value<'a> {
    Varint(u64),
    Fixed64(u64),
    Bytes(&'a [u8]),
    Fixed32,
}

/// A protobuf reader over borrowed bytes.
pub struct Reader<'a> {
    bytes: &'a [u8],
    /// Where the next field starts.
    pub position: usize,
    /// Where the field read last starts (its tag).
    pub field_start: usize,
}

impl<'a> Reader<'a> {
    #[inline(always)]
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            position: 0,
            field_start: 0,
        }
    }

    #[cold]
    fn error(message: &str) -> RemoteWriteError {
        RemoteWriteError::Protobuf(message.into())
    }

    #[inline(always)]
    fn varint(&mut self) -> Result<u64, RemoteWriteError> {
        // One byte: most tags and lengths.
        if let Some(&byte) = self.bytes.get(self.position) {
            if byte < 0x80 {
                self.position += 1;
                return Ok(u64::from(byte));
            }
        }
        let (value, position) = varint_slow(self.bytes, self.position)?;
        self.position = position;
        Ok(value)
    }

    #[inline(always)]
    fn take(&mut self, len: usize) -> Result<&'a [u8], RemoteWriteError> {
        let end = self
            .position
            .checked_add(len)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| Self::error("field runs past the end of its message"))?;
        let taken = &self.bytes[self.position..end];
        self.position = end;
        Ok(taken)
    }

    /// The next field number and value; `None` at the end.
    #[inline(always)]
    pub fn field(&mut self) -> Result<Option<(u64, Value<'a>)>, RemoteWriteError> {
        if self.position == self.bytes.len() {
            return Ok(None);
        }
        self.field_start = self.position;
        let tag = self.varint()?;
        let field = tag >> 3;
        if field == 0 {
            return Err(Self::error("field number 0"));
        }
        let value = match tag & 7 {
            0 => Value::Varint(self.varint()?),
            1 => Value::Fixed64(u64::from_le_bytes(
                self.take(8)?.try_into().expect("8 bytes"),
            )),
            2 => {
                let len =
                    usize::try_from(self.varint()?).map_err(|_| Self::error("length too large"))?;
                Value::Bytes(self.take(len)?)
            }
            5 => {
                self.take(4)?;
                Value::Fixed32
            }
            wire => return Err(Self::error(&format!("unsupported wire type {wire}"))),
        };
        Ok(Some((field, value)))
    }
}

/// The varint at `position` of `bytes` and the position after it, for a
/// varint of more than one byte. A free function of plain values, not a
/// `Reader` method: the reader then never escapes to this out-of-line,
/// cross-crate call, so its position stays in a register in the callers'
/// loops.
#[inline(never)]
fn varint_slow(bytes: &[u8], mut position: usize) -> Result<(u64, usize), RemoteWriteError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes
            .get(position)
            .ok_or_else(|| Reader::error("truncated varint"))?;
        position += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok((value, position));
        }
    }
    Err(Reader::error("varint too long"))
}

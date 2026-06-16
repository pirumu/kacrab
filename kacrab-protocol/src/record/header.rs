//! [`RecordHeader`] — non-nullable key, nullable value, on a single record.

use bytes::{Bytes, BytesMut};

use super::Result;

/// A record header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    /// Header key (non-nullable; on-wire length must be `>= 0`).
    pub key: Bytes,
    /// Header value. `None` when the on-wire length is `-1`.
    pub value: Option<Bytes>,
}

impl RecordHeader {
    /// Return the exact encoded length of this header.
    pub fn encoded_len(&self) -> Result<usize> {
        let len = super::bytes_field_len("header key", &self.key)?;
        super::add_encoded_len(
            "header value",
            len,
            super::nullable_bytes_field_len("header value", self.value.as_ref())?,
        )
    }

    /// Encode this header into `buf`.
    pub fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        super::write_bytes_field(buf, "header key", &self.key)?;
        super::write_nullable_bytes_field(buf, "header value", self.value.as_ref())?;
        Ok(())
    }

    /// Decode one header from `buf`. Rejects negative key lengths.
    pub fn decode(buf: &mut Bytes) -> Result<Self> {
        let key = super::read_bytes_field(buf, "header key")?;
        let value = super::read_nullable_bytes_field(buf, "header value")?;

        Ok(Self { key, value })
    }
}

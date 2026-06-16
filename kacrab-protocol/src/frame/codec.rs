//! Kafka request and response envelope helpers.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    KafkaString,
    error::Result,
    frame::{FrameError, FrameErrorKind, MAX_FRAME_LENGTH},
    generated::{ApiKey, RequestHeaderData, ResponseHeaderData},
    version::{request_header_version, response_header_version},
};

/// Metadata required to encode a Kafka request frame.
#[derive(Debug, Clone, Copy)]
pub struct RequestFrameSpec<'a> {
    /// Kafka API key.
    pub api_key: ApiKey,
    /// Negotiated request API version.
    pub api_version: i16,
    /// Request correlation id.
    pub correlation_id: i32,
    /// Kafka client id written into the request header.
    pub client_id: &'a str,
    /// Expected encoded frame capacity, including the 4-byte length prefix.
    pub capacity_hint: usize,
}

/// Decoded Kafka response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseEnvelope {
    /// Correlation id decoded from the response header.
    pub correlation_id: i32,
    /// Response body bytes, advanced past the response header.
    pub body: Bytes,
}

/// Encode a Kafka request frame by writing header and body into one buffer.
pub fn encode_request_frame<F>(spec: RequestFrameSpec<'_>, write_body: F) -> Result<BytesMut>
where
    F: FnOnce(&mut BytesMut) -> Result<()>,
{
    let mut frame = BytesMut::with_capacity(spec.capacity_hint.max(4));
    encode_request_frame_with_buffer(&mut frame, spec, write_body)?;
    Ok(frame)
}

/// Encode a Kafka request frame into a caller-supplied buffer.
pub fn encode_request_frame_with_buffer<F>(
    frame: &mut BytesMut,
    spec: RequestFrameSpec<'_>,
    write_body: F,
) -> Result<()>
where
    F: FnOnce(&mut BytesMut) -> Result<()>,
{
    frame.clear();
    frame.reserve(spec.capacity_hint.max(4));
    frame.put_i32(0);
    write_request_header(frame, spec)?;
    write_body(frame)?;
    finish_request_frame(frame)
}

/// Encode a Kafka request frame with an already-encoded request body.
pub fn encode_request_frame_body(spec: RequestFrameSpec<'_>, body: &[u8]) -> Result<BytesMut> {
    encode_request_frame(spec, |frame| {
        frame.extend_from_slice(body);
        Ok(())
    })
}

/// Encode a Kafka request frame with an already-encoded request body into a
/// caller-supplied buffer.
pub fn encode_request_frame_body_with_buffer(
    frame: &mut BytesMut,
    spec: RequestFrameSpec<'_>,
    body: &[u8],
) -> Result<()> {
    encode_request_frame_with_buffer(frame, spec, |frame| {
        frame.extend_from_slice(body);
        Ok(())
    })
}

/// Parse a Kafka response header once and return the remaining body bytes.
pub fn decode_response_envelope(
    api_key: ApiKey,
    api_version: i16,
    mut bytes: Bytes,
) -> Result<ResponseEnvelope> {
    let header_version = response_header_version(api_key as i16, api_version);
    let header = ResponseHeaderData::read(&mut bytes, header_version)?;
    Ok(ResponseEnvelope {
        correlation_id: header.correlation_id,
        body: bytes,
    })
}

fn write_request_header(buf: &mut BytesMut, spec: RequestFrameSpec<'_>) -> Result<()> {
    let header_version = request_header_version(spec.api_key as i16, spec.api_version);
    RequestHeaderData {
        request_api_key: spec.api_key as i16,
        request_api_version: spec.api_version,
        correlation_id: spec.correlation_id,
        client_id: Some(KafkaString::from(spec.client_id.to_owned())),
        _unknown_tagged_fields: Vec::new(),
    }
    .write(buf, header_version)?;
    Ok(())
}

fn finish_request_frame(request_frame: &mut BytesMut) -> Result<()> {
    let payload_len = request_frame.len().checked_sub(4).ok_or_else(|| {
        crate::ProtocolError::Frame(FrameError::from(FrameErrorKind::Truncated {
            needed: 4,
            available: request_frame.len(),
        }))
    })?;
    let max_frame_length = usize::try_from(MAX_FRAME_LENGTH).unwrap_or(usize::MAX);
    if payload_len > max_frame_length {
        return Err(crate::ProtocolError::Frame(FrameError::from(
            FrameErrorKind::TooLarge {
                length: i32::try_from(payload_len).unwrap_or(i32::MAX),
                max: MAX_FRAME_LENGTH,
            },
        )));
    }
    let payload_len = i32::try_from(payload_len).map_err(|_error| {
        crate::ProtocolError::Frame(FrameError::from(FrameErrorKind::TooLarge {
            length: i32::MAX,
            max: MAX_FRAME_LENGTH,
        }))
    })?;
    let payload = request_frame.split_off(4);
    request_frame.clear();
    request_frame.put_i32(payload_len);
    request_frame.unsplit(payload);
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use super::{RequestFrameSpec, decode_response_envelope, encode_request_frame};
    use crate::{
        KafkaString,
        generated::{
            ApiKey, ApiVersionsRequestData, ApiVersionsResponseData, RequestHeaderData,
            ResponseHeaderData,
        },
        version::{request_header_version, response_header_version},
    };

    #[test]
    fn encode_request_frame_writes_length_header_and_body_in_one_buffer() {
        let request = ApiVersionsRequestData {
            client_software_name: KafkaString::from("kacrab".to_owned()),
            client_software_version: KafkaString::from("0.0.1".to_owned()),
            _unknown_tagged_fields: Vec::new(),
        };

        let frame = encode_request_frame(
            RequestFrameSpec {
                api_key: ApiKey::ApiVersions,
                api_version: 3,
                correlation_id: 42,
                client_id: "client-a",
                capacity_hint: 64,
            },
            |buf| request.write(buf, 3),
        )
        .expect("request frame");

        let mut payload = frame.freeze();
        let frame_len = payload.get_i32();
        assert_eq!(usize::try_from(frame_len).unwrap(), payload.remaining());

        let header_version = request_header_version(ApiKey::ApiVersions as i16, 3);
        let header = RequestHeaderData::read(&mut payload, header_version).expect("header");
        assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
        assert_eq!(header.request_api_version, 3);
        assert_eq!(header.correlation_id, 42);
        assert_eq!(
            header.client_id.as_ref().map(KafkaString::as_str),
            Some("client-a")
        );

        let decoded = ApiVersionsRequestData::read(&mut payload, 3).expect("body");
        assert_eq!(decoded.client_software_name.as_str(), "kacrab");
        assert_eq!(decoded.client_software_version.as_str(), "0.0.1");
        assert_eq!(payload.remaining(), 0);
    }

    #[test]
    fn decode_response_envelope_returns_correlation_and_body_bytes() {
        let response = ApiVersionsResponseData::default();
        let mut body = BytesMut::new();
        response.write(&mut body, 3).expect("response body");

        let mut payload = BytesMut::new();
        ResponseHeaderData {
            correlation_id: 7,
            _unknown_tagged_fields: Vec::new(),
        }
        .write(
            &mut payload,
            response_header_version(ApiKey::ApiVersions as i16, 3),
        )
        .expect("response header");
        payload.extend_from_slice(&body);

        let mut envelope = decode_response_envelope(ApiKey::ApiVersions, 3, payload.freeze())
            .expect("response envelope");

        assert_eq!(envelope.correlation_id, 7);
        let decoded = ApiVersionsResponseData::read(&mut envelope.body, 3).expect("response");
        assert_eq!(decoded.error_code, 0);
        assert_eq!(envelope.body.remaining(), 0);
    }
}

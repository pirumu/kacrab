//! Generated from MetadataRequest.json - DO NOT EDIT
#![allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::arithmetic_side_effects,
    reason = "Generated protocol modules mirror Kafka's schema shape and intentionally trade \
              hand-written lint style for reproducible wire-code output."
)]
use bytes::{Bytes, BytesMut};

use crate::*;

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataRequestData {
    /// The topics to fetch metadata for.
    pub topics: Option<Vec<MetadataRequestTopic>>,
    /// If this is true, the broker may auto-create topics that we requested which do not already
    /// exist, if it is configured to do so.
    pub allow_auto_topic_creation: bool,
    /// Whether to include cluster authorized operations.
    pub include_cluster_authorized_operations: bool,
    /// Whether to include topic authorized operations.
    pub include_topic_authorized_operations: bool,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for MetadataRequestData {
    fn default() -> Self {
        Self {
            topics: None,
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl MetadataRequestData {
    pub fn with_topics(mut self, value: Option<Vec<MetadataRequestTopic>>) -> Self {
        self.topics = value;
        self
    }
    pub fn with_allow_auto_topic_creation(mut self, value: bool) -> Self {
        self.allow_auto_topic_creation = value;
        self
    }
    pub fn with_include_cluster_authorized_operations(mut self, value: bool) -> Self {
        self.include_cluster_authorized_operations = value;
        self
    }
    pub fn with_include_topic_authorized_operations(mut self, value: bool) -> Self {
        self.include_topic_authorized_operations = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        if version < 0 || version > 13 {
            return Err(UnsupportedVersion::new(3, version).into());
        }
        let topics;
        let mut allow_auto_topic_creation = true;
        let mut include_cluster_authorized_operations = false;
        let mut include_topic_authorized_operations = false;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 1 {
            if version >= 9 {
                topics = {
                    let len = read_compact_array_length(buf)?;
                    if len < 0 {
                        None
                    } else {
                        let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                        for _ in 0..len {
                            arr.push(MetadataRequestTopic::read(buf, version)?);
                        }
                        Some(arr)
                    }
                };
            } else {
                topics = {
                    let len = read_array_length(buf)?;
                    if len < 0 {
                        None
                    } else {
                        let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                        for _ in 0..len {
                            arr.push(MetadataRequestTopic::read(buf, version)?);
                        }
                        Some(arr)
                    }
                };
            }
        } else {
            topics = Some({
                let len = read_array_length(buf)?;
                let mut arr = Vec::with_capacity(array_read_capacity(len, (buf).len()));
                for _ in 0..len {
                    arr.push(MetadataRequestTopic::read(buf, version)?);
                }
                arr
            });
        }
        if version >= 4 {
            allow_auto_topic_creation = read_bool(buf)?;
        }
        if version >= 8 && version <= 10 {
            include_cluster_authorized_operations = read_bool(buf)?;
        }
        if version >= 8 {
            include_topic_authorized_operations = read_bool(buf)?;
        }
        if version >= 9 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version < 0 || version > 13 {
            return Err(UnsupportedVersion::new(3, version).into());
        }
        if version >= 1 {
            if version >= 9 {
                match &self.topics {
                    None => {
                        write_compact_array_length(buf, -1);
                    },
                    Some(arr) => {
                        write_compact_array_length(buf, arr.len() as i32);
                        for el in arr {
                            el.write(buf, version)?;
                        }
                    },
                }
            } else {
                match &self.topics {
                    None => {
                        write_array_length(buf, -1);
                    },
                    Some(arr) => {
                        write_array_length(buf, arr.len() as i32);
                        for el in arr {
                            el.write(buf, version)?;
                        }
                    },
                }
            }
        } else {
            match &self.topics {
                Some(arr) => {
                    write_array_length(buf, arr.len() as i32);
                    for el in arr {
                        el.write(buf, version)?;
                    }
                },
                None => {
                    write_array_length(buf, 0);
                },
            }
        }
        if version >= 4 {
            write_bool(buf, self.allow_auto_topic_creation);
        } else if self.allow_auto_topic_creation != true {
            return Err(
                UnsupportedFieldVersion::new(3, "allow_auto_topic_creation", version).into(),
            );
        }
        if version >= 8 && version <= 10 {
            write_bool(buf, self.include_cluster_authorized_operations);
        } else if self.include_cluster_authorized_operations != false {
            return Err(UnsupportedFieldVersion::new(
                3,
                "include_cluster_authorized_operations",
                version,
            )
            .into());
        }
        if version >= 8 {
            write_bool(buf, self.include_topic_authorized_operations);
        } else if self.include_topic_authorized_operations != false {
            return Err(UnsupportedFieldVersion::new(
                3,
                "include_topic_authorized_operations",
                version,
            )
            .into());
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        if version < 0 || version > 13 {
            return Err(UnsupportedVersion::new(3, version).into());
        }
        let mut len: usize = 0;
        if version >= 1 {
            if version >= 9 {
                match &self.topics {
                    None => {
                        len += compact_array_length_len(-1);
                    },
                    Some(arr) => {
                        len += compact_array_length_len(arr.len() as i32);
                        for el in arr {
                            len += el.encoded_len(version)?;
                        }
                    },
                }
            } else {
                match &self.topics {
                    None => {
                        len += array_length_len();
                    },
                    Some(arr) => {
                        len += array_length_len();
                        for el in arr {
                            len += el.encoded_len(version)?;
                        }
                    },
                }
            }
        } else {
            match &self.topics {
                Some(arr) => {
                    len += array_length_len();
                    for el in arr {
                        len += el.encoded_len(version)?;
                    }
                },
                None => {
                    len += array_length_len();
                },
            }
        }
        if version >= 4 {
            len += 1;
        } else if self.allow_auto_topic_creation != true {
            return Err(
                UnsupportedFieldVersion::new(3, "allow_auto_topic_creation", version).into(),
            );
        }
        if version >= 8 && version <= 10 {
            len += 1;
        } else if self.include_cluster_authorized_operations != false {
            return Err(UnsupportedFieldVersion::new(
                3,
                "include_cluster_authorized_operations",
                version,
            )
            .into());
        }
        if version >= 8 {
            len += 1;
        } else if self.include_topic_authorized_operations != false {
            return Err(UnsupportedFieldVersion::new(
                3,
                "include_topic_authorized_operations",
                version,
            )
            .into());
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataRequestTopic {
    /// The topic id.
    pub topic_id: KafkaUuid,
    /// The topic name.
    pub name: Option<KafkaString>,
    pub _unknown_tagged_fields: Vec<RawTaggedField>,
}
impl Default for MetadataRequestTopic {
    fn default() -> Self {
        Self {
            topic_id: KafkaUuid::ZERO,
            name: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
impl MetadataRequestTopic {
    pub fn with_topic_id(mut self, value: KafkaUuid) -> Self {
        self.topic_id = value;
        self
    }
    pub fn with_name(mut self, value: Option<KafkaString>) -> Self {
        self.name = value;
        self
    }
    pub fn read(buf: &mut Bytes, version: i16) -> Result<Self> {
        let mut topic_id = KafkaUuid::ZERO;
        let name;
        let mut _unknown_tagged_fields: Vec<RawTaggedField> = Vec::new();
        if version >= 10 {
            topic_id = read_uuid(buf)?;
        }
        if version >= 10 {
            name = read_compact_nullable_string(buf)?;
        } else {
            if version >= 9 {
                name = Some(read_compact_string(buf)?);
            } else {
                name = Some(read_string(buf)?);
            }
        }
        if version >= 9 {
            let tagged_fields = read_tagged_fields(buf)?;
            for field in &tagged_fields {
                match field.tag {
                    _ => {
                        _unknown_tagged_fields.push(field.clone());
                    },
                }
            }
        }
        Ok(Self {
            topic_id,
            name,
            _unknown_tagged_fields,
        })
    }
    pub fn write(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        if version >= 10 {
            write_uuid(buf, &self.topic_id);
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(3, "topic_id", version).into());
        }
        if version >= 10 {
            write_compact_nullable_string(buf, self.name.as_ref())?;
        } else {
            {
                let _nn_default = KafkaString::default();
                let _nn_val = self.name.as_ref().unwrap_or(&_nn_default);
                if version >= 9 {
                    write_compact_string(buf, _nn_val)?;
                } else {
                    write_string(buf, _nn_val)?;
                }
            }
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            write_tagged_fields(buf, &all_tags)?;
        }
        Ok(())
    }
    pub fn encoded_len(&self, version: i16) -> Result<usize> {
        let mut len: usize = 0;
        if version >= 10 {
            len += 16;
        } else if self.topic_id != KafkaUuid::ZERO {
            return Err(UnsupportedFieldVersion::new(3, "topic_id", version).into());
        }
        if version >= 10 {
            len += compact_nullable_string_len(self.name.as_ref())?;
        } else {
            let _nn_default = KafkaString::default();
            let _nn_val = self.name.as_ref().unwrap_or(&_nn_default);
            if version >= 9 {
                len += compact_string_len(_nn_val)?;
            } else {
                len += string_len(_nn_val)?;
            }
        }
        if version >= 9 {
            let mut all_tags: Vec<RawTaggedField> = self._unknown_tagged_fields.clone();
            all_tags.sort_by_key(|f| f.tag);
            len += tagged_fields_len(&all_tags)?;
        }
        Ok(len)
    }
}

use crate::error::{HarborError, Result};

use super::protocol::*;

pub(super) const MAX_CONTAINER_ELEMENTS: usize = 16_384;
pub(super) const MAX_SKIP_DEPTH: usize = 32;

pub(super) struct Reader<'a> {
    buffer: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub(super) fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }

    pub(super) fn position(&self) -> usize {
        self.position
    }

    pub(super) fn read_u8(&mut self) -> Result<u8> {
        if self.position >= self.buffer.len() {
            return Err(HarborError::Thrift("unexpected end of message".into()));
        }
        let value = self.buffer[self.position];
        self.position += 1;
        Ok(value)
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.position + len > self.buffer.len() {
            return Err(HarborError::Thrift("unexpected end of message".into()));
        }
        let bytes = &self.buffer[self.position..self.position + len];
        self.position += len;
        Ok(bytes)
    }

    pub(super) fn read_i16(&mut self) -> Result<i16> {
        let bytes = self.read_exact(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub(super) fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.read_exact(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(super) fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_exact(8)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_double(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.read_i64()? as u64))
    }

    pub(super) fn read_string(&mut self) -> Result<String> {
        let bytes = self.read_binary()?;
        String::from_utf8(bytes)
            .map_err(|err| HarborError::Thrift(format!("invalid UTF-8 string: {err}")))
    }

    pub(super) fn read_binary(&mut self) -> Result<Vec<u8>> {
        let len = self.read_i32()?;
        if len < 0 {
            return Err(HarborError::Thrift("negative binary length".into()));
        }
        Ok(self.read_exact(len as usize)?.to_vec())
    }

    pub(super) fn read_field_begin(&mut self) -> Result<(u8, i16)> {
        let field_type = self.read_u8()?;
        if field_type == T_STOP {
            return Ok((T_STOP, 0));
        }
        if !valid_field_type(field_type) {
            return Err(HarborError::Thrift(format!(
                "unsupported Thrift type `{field_type}`"
            )));
        }
        let field_id = self.read_i16()?;
        Ok((field_type, field_id))
    }

    pub(super) fn skip_remaining_struct_fields(&mut self) -> Result<()> {
        self.skip_remaining_struct_fields_at_depth(0)
    }

    fn skip_remaining_struct_fields_at_depth(&mut self, depth: usize) -> Result<()> {
        self.check_depth(depth)?;
        loop {
            let (field_type, _) = self.read_field_begin()?;
            if field_type == T_STOP {
                return Ok(());
            }
            self.skip_at_depth(field_type, depth + 1)?;
        }
    }

    pub(super) fn skip(&mut self, field_type: u8) -> Result<()> {
        self.skip_at_depth(field_type, 0)
    }

    fn skip_at_depth(&mut self, field_type: u8, depth: usize) -> Result<()> {
        self.check_depth(depth)?;
        match field_type {
            T_STOP => Err(HarborError::Thrift(
                "invalid Thrift STOP type in value position".into(),
            )),
            T_BOOL | T_BYTE => {
                self.read_u8()?;
                Ok(())
            }
            T_I16 => {
                self.read_i16()?;
                Ok(())
            }
            T_I32 => {
                self.read_i32()?;
                Ok(())
            }
            T_I64 => {
                self.read_i64()?;
                Ok(())
            }
            T_DOUBLE => {
                self.read_double()?;
                Ok(())
            }
            T_STRING => {
                self.read_binary()?;
                Ok(())
            }
            T_STRUCT => self.skip_remaining_struct_fields_at_depth(depth + 1),
            T_MAP => {
                let key_type = self.read_container_element_type("map key")?;
                let value_type = self.read_container_element_type("map value")?;
                let size = self.read_container_size("map")?;
                for _ in 0..size {
                    self.skip_at_depth(key_type, depth + 1)?;
                    self.skip_at_depth(value_type, depth + 1)?;
                }
                Ok(())
            }
            T_LIST | T_SET => {
                let element_type = self.read_container_element_type("list/set element")?;
                let size = self.read_container_size("list/set")?;
                for _ in 0..size {
                    self.skip_at_depth(element_type, depth + 1)?;
                }
                Ok(())
            }
            other => Err(HarborError::Thrift(format!(
                "unsupported Thrift type `{other}`"
            ))),
        }
    }

    fn read_container_element_type(&mut self, position: &str) -> Result<u8> {
        let element_type = self.read_u8()?;
        if !valid_field_type(element_type) {
            return Err(HarborError::Thrift(format!(
                "invalid Thrift {position} type `{element_type}`"
            )));
        }
        Ok(element_type)
    }

    fn read_container_size(&mut self, kind: &str) -> Result<usize> {
        let size = self.read_i32()?;
        if size < 0 {
            return Err(HarborError::Thrift(format!("negative {kind} size")));
        }
        let size = size as usize;
        if size > MAX_CONTAINER_ELEMENTS {
            return Err(HarborError::Thrift(format!(
                "Thrift {kind} size exceeds maximum of {MAX_CONTAINER_ELEMENTS}"
            )));
        }
        Ok(size)
    }

    fn check_depth(&self, depth: usize) -> Result<()> {
        if depth > MAX_SKIP_DEPTH {
            return Err(HarborError::Thrift(format!(
                "Thrift nesting depth exceeds maximum of {MAX_SKIP_DEPTH}"
            )));
        }
        Ok(())
    }
}

pub(super) struct Writer {
    buffer: Vec<u8>,
}

impl Writer {
    pub(super) fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub(super) fn into_inner(self) -> Vec<u8> {
        self.buffer
    }

    pub(super) fn write_message_begin(&mut self, name: &str, message_type: u8, seqid: i32) {
        self.write_i32((T_BINARY_VERSION_1 | message_type as u32) as i32);
        self.write_string(name);
        self.write_i32(seqid);
    }

    pub(super) fn write_field<F>(&mut self, field_type: u8, field_id: i16, write_value: F)
    where
        F: FnOnce(&mut Writer),
    {
        self.buffer.push(field_type);
        self.write_i16(field_id);
        write_value(self);
    }

    pub(super) fn write_stop(&mut self) {
        self.buffer.push(T_STOP);
    }

    pub(super) fn write_list_begin(&mut self, element_type: u8, len: usize) {
        self.buffer.push(element_type);
        self.write_i32(len as i32);
    }

    pub(super) fn write_bool(&mut self, value: bool) {
        self.buffer.push(u8::from(value));
    }

    pub(super) fn write_i16(&mut self, value: i16) {
        self.buffer.extend(value.to_be_bytes());
    }

    pub(super) fn write_i32(&mut self, value: i32) {
        self.buffer.extend(value.to_be_bytes());
    }

    pub(super) fn write_i64(&mut self, value: i64) {
        self.buffer.extend(value.to_be_bytes());
    }

    pub(super) fn write_double(&mut self, value: f64) {
        self.buffer.extend((value.to_bits() as i64).to_be_bytes());
    }

    pub(super) fn write_string(&mut self, value: &str) {
        self.write_binary(value.as_bytes());
    }

    pub(super) fn write_binary(&mut self, value: &[u8]) {
        self.write_i32(value.len() as i32);
        self.buffer.extend(value);
    }
}

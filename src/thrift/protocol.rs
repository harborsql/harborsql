pub(super) const T_STOP: u8 = 0;
pub(super) const T_BOOL: u8 = 2;
pub(super) const T_BYTE: u8 = 3;
pub(super) const T_DOUBLE: u8 = 4;
pub(super) const T_I16: u8 = 6;
pub(super) const T_I32: u8 = 8;
pub(super) const T_I64: u8 = 10;
pub(super) const T_STRING: u8 = 11;
pub(super) const T_STRUCT: u8 = 12;
pub(super) const T_MAP: u8 = 13;
pub(super) const T_SET: u8 = 14;
pub(super) const T_LIST: u8 = 15;

pub(super) const T_MESSAGE_CALL: u8 = 1;
pub(super) const T_MESSAGE_REPLY: u8 = 2;
pub(super) const T_MESSAGE_EXCEPTION: u8 = 3;
pub(super) const T_BINARY_VERSION_1: u32 = 0x8001_0000;

pub(super) const SPARK_CLI_SERVICE_PROTOCOL_V7: i32 = 42247;
pub(super) const SUCCESS_STATUS: i32 = 0;
pub(super) const ERROR_STATUS: i32 = 3;
pub(super) const INVALID_HANDLE_STATUS: i32 = 4;
pub(super) const RUNNING_STATE: i32 = 1;
pub(super) const FINISHED_STATE: i32 = 2;
pub(super) const CANCELED_STATE: i32 = 3;
pub(super) const ERROR_STATE: i32 = 5;
pub(super) const EXECUTE_STATEMENT: i32 = 0;
pub(super) const COLUMN_BASED_SET: i32 = 1;
pub(super) const DEFAULT_FETCH_ROWS: usize = 1_000;
pub(super) const MAX_FETCH_ROWS: usize = 10_000;

pub(super) const BOOLEAN_TYPE: i32 = 0;
pub(super) const INT_TYPE: i32 = 3;
pub(super) const BIGINT_TYPE: i32 = 4;
pub(super) const DOUBLE_TYPE: i32 = 6;
pub(super) const STRING_TYPE: i32 = 7;
pub(super) const TIMESTAMP_TYPE: i32 = 8;
pub(super) const DATE_TYPE: i32 = 17;

pub(super) fn valid_field_type(field_type: u8) -> bool {
    matches!(
        field_type,
        T_BOOL
            | T_BYTE
            | T_DOUBLE
            | T_I16
            | T_I32
            | T_I64
            | T_STRING
            | T_STRUCT
            | T_MAP
            | T_SET
            | T_LIST
    )
}

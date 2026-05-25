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
pub(super) const TINYINT_TYPE: i32 = 1;
pub(super) const SMALLINT_TYPE: i32 = 2;
pub(super) const INT_TYPE: i32 = 3;
pub(super) const BIGINT_TYPE: i32 = 4;
pub(super) const FLOAT_TYPE: i32 = 5;
pub(super) const DOUBLE_TYPE: i32 = 6;
pub(super) const STRING_TYPE: i32 = 7;
pub(super) const TIMESTAMP_TYPE: i32 = 8;
pub(super) const BINARY_TYPE: i32 = 9;
pub(super) const ARRAY_TYPE: i32 = 10;
pub(super) const MAP_TYPE: i32 = 11;
pub(super) const STRUCT_TYPE: i32 = 12;
pub(super) const DECIMAL_TYPE: i32 = 15;
pub(super) const DATE_TYPE: i32 = 17;

pub(super) const CLI_MAX_DRIVER_CONNECTIONS: i32 = 0;
pub(super) const CLI_MAX_CONCURRENT_ACTIVITIES: i32 = 1;
pub(super) const CLI_DATA_SOURCE_NAME: i32 = 2;
pub(super) const CLI_FETCH_DIRECTION: i32 = 8;
pub(super) const CLI_SERVER_NAME: i32 = 13;
pub(super) const CLI_SEARCH_PATTERN_ESCAPE: i32 = 14;
pub(super) const CLI_DBMS_NAME: i32 = 17;
pub(super) const CLI_DBMS_VER: i32 = 18;
pub(super) const CLI_ACCESSIBLE_TABLES: i32 = 19;
pub(super) const CLI_ACCESSIBLE_PROCEDURES: i32 = 20;
pub(super) const CLI_CURSOR_COMMIT_BEHAVIOR: i32 = 23;
pub(super) const CLI_DATA_SOURCE_READ_ONLY: i32 = 25;
pub(super) const CLI_DEFAULT_TXN_ISOLATION: i32 = 26;
pub(super) const CLI_IDENTIFIER_CASE: i32 = 28;
pub(super) const CLI_IDENTIFIER_QUOTE_CHAR: i32 = 29;
pub(super) const CLI_MAX_COLUMN_NAME_LEN: i32 = 30;
pub(super) const CLI_MAX_CURSOR_NAME_LEN: i32 = 31;
pub(super) const CLI_MAX_SCHEMA_NAME_LEN: i32 = 32;
pub(super) const CLI_MAX_CATALOG_NAME_LEN: i32 = 34;
pub(super) const CLI_MAX_TABLE_NAME_LEN: i32 = 35;
pub(super) const CLI_TXN_CAPABLE: i32 = 46;
pub(super) const CLI_USER_NAME: i32 = 47;
pub(super) const CLI_TXN_ISOLATION_OPTION: i32 = 72;
pub(super) const CLI_INTEGRITY: i32 = 73;
pub(super) const CLI_GETDATA_EXTENSIONS: i32 = 81;
pub(super) const CLI_NULL_COLLATION: i32 = 85;
pub(super) const CLI_ALTER_TABLE: i32 = 86;
pub(super) const CLI_ORDER_BY_COLUMNS_IN_SELECT: i32 = 90;
pub(super) const CLI_SPECIAL_CHARACTERS: i32 = 94;
pub(super) const CLI_MAX_COLUMNS_IN_GROUP_BY: i32 = 97;
pub(super) const CLI_MAX_COLUMNS_IN_INDEX: i32 = 98;
pub(super) const CLI_MAX_COLUMNS_IN_ORDER_BY: i32 = 99;
pub(super) const CLI_MAX_COLUMNS_IN_SELECT: i32 = 100;
pub(super) const CLI_MAX_COLUMNS_IN_TABLE: i32 = 101;
pub(super) const CLI_MAX_INDEX_SIZE: i32 = 102;
pub(super) const CLI_MAX_ROW_SIZE: i32 = 104;
pub(super) const CLI_MAX_STATEMENT_LEN: i32 = 105;
pub(super) const CLI_MAX_TABLES_IN_SELECT: i32 = 106;
pub(super) const CLI_MAX_USER_NAME_LEN: i32 = 107;
pub(super) const CLI_OJ_CAPABILITIES: i32 = 115;
pub(super) const CLI_XOPEN_CLI_YEAR: i32 = 10000;
pub(super) const CLI_CURSOR_SENSITIVITY: i32 = 10001;
pub(super) const CLI_DESCRIBE_PARAMETER: i32 = 10002;
pub(super) const CLI_CATALOG_NAME: i32 = 10003;
pub(super) const CLI_COLLATION_SEQ: i32 = 10004;
pub(super) const CLI_MAX_IDENTIFIER_LEN: i32 = 10005;

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

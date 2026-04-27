use std::{any::Any, borrow::Cow, sync::Arc};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, ArrowPrimitiveType, AsArray, GenericStringArray, Int32Array,
            LargeStringBuilder, StringBuilder, StringViewBuilder,
        },
        compute::{DatePart, date_part},
        datatypes::DataType,
    },
    common::{
        DataFusionError, Result as DataFusionResult, ScalarValue, cast::as_string_view_array,
    },
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
        Volatility,
    },
    prelude::SessionContext,
};
use regex::Regex;

pub const REGEXP_REPLACE_CAPTURE_UDF: &str = "harborsql_regexp_replace_capture";
pub const EXTRACT_MINUTE_UDF: &str = "harborsql_extract_minute";

pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(RegexpReplaceCaptureFunc::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(ExtractMinuteFunc::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ExtractMinuteFunc {
    signature: Signature,
}

impl ExtractMinuteFunc {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ExtractMinuteFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        EXTRACT_MINUTE_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "{EXTRACT_MINUTE_UDF} expects one argument"
            )));
        }

        match &args.args[0] {
            ColumnarValue::Scalar(scalar) => {
                let input = scalar.to_array()?;
                let output = extract_minute_array(&input)?;
                ScalarValue::try_from_array(output.as_ref(), 0).map(ColumnarValue::Scalar)
            }
            ColumnarValue::Array(array) => extract_minute_array(array).map(ColumnarValue::Array),
        }
    }
}

fn extract_minute_array(array: &ArrayRef) -> DataFusionResult<ArrayRef> {
    use datafusion::arrow::datatypes::{
        TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };

    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, timezone) if is_utc_timezone(timezone.as_deref()) => {
            extract_timestamp_minute::<TimestampSecondType>(array, 1)
        }
        DataType::Timestamp(TimeUnit::Millisecond, timezone)
            if is_utc_timezone(timezone.as_deref()) =>
        {
            extract_timestamp_minute::<TimestampMillisecondType>(array, 1_000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone)
            if is_utc_timezone(timezone.as_deref()) =>
        {
            extract_timestamp_minute::<TimestampMicrosecondType>(array, 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, timezone)
            if is_utc_timezone(timezone.as_deref()) =>
        {
            extract_timestamp_minute::<TimestampNanosecondType>(array, 1_000_000_000)
        }
        _ => date_part(array.as_ref(), DatePart::Minute).map_err(DataFusionError::from),
    }
}

fn is_utc_timezone(timezone: Option<&str>) -> bool {
    timezone.is_none_or(|timezone| timezone.eq_ignore_ascii_case("UTC"))
}

fn extract_timestamp_minute<T>(
    array: &ArrayRef,
    units_per_second: i64,
) -> DataFusionResult<ArrayRef>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    let array = array.as_primitive::<T>();
    let minutes: Int32Array = array.unary_opt(|value| {
        let second = value.div_euclid(units_per_second);
        Some((second.rem_euclid(60 * 60) / 60) as i32)
    });
    Ok(Arc::new(minutes) as ArrayRef)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpReplaceCaptureFunc {
    signature: Signature,
}

impl RegexpReplaceCaptureFunc {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::Utf8,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpReplaceCaptureFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        REGEXP_REPLACE_CAPTURE_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 3 {
            return Err(DataFusionError::Execution(format!(
                "{REGEXP_REPLACE_CAPTURE_UDF} expects three arguments"
            )));
        }

        let pattern = scalar_string_arg(&args.args[1], "pattern")?;
        let capture_index = scalar_i64_arg(&args.args[2], "capture_index")?;
        if capture_index < 0 {
            return Err(DataFusionError::Execution(format!(
                "{REGEXP_REPLACE_CAPTURE_UDF} capture index must be non-negative"
            )));
        }
        let regex = Regex::new(&pattern).map_err(|err| DataFusionError::External(Box::new(err)))?;
        let capture_index = capture_index as usize;

        match &args.args[0] {
            ColumnarValue::Scalar(scalar) => {
                replace_scalar(scalar, &regex, capture_index).map(ColumnarValue::Scalar)
            }
            ColumnarValue::Array(array) => {
                replace_array(array, &regex, capture_index).map(ColumnarValue::Array)
            }
        }
    }
}

fn scalar_string_arg(arg: &ColumnarValue, name: &str) -> DataFusionResult<String> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(value)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(value))) => Ok(value.clone()),
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => Err(DataFusionError::Execution(
            format!("{REGEXP_REPLACE_CAPTURE_UDF} {name} cannot be NULL"),
        )),
        other => Err(DataFusionError::Execution(format!(
            "{REGEXP_REPLACE_CAPTURE_UDF} expects scalar string {name}, got {other:?}"
        ))),
    }
}

fn scalar_i64_arg(arg: &ColumnarValue, name: &str) -> DataFusionResult<i64> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => Ok(*value),
        ColumnarValue::Scalar(ScalarValue::Int64(None)) => Err(DataFusionError::Execution(
            format!("{REGEXP_REPLACE_CAPTURE_UDF} {name} cannot be NULL"),
        )),
        other => Err(DataFusionError::Execution(format!(
            "{REGEXP_REPLACE_CAPTURE_UDF} expects scalar Int64 {name}, got {other:?}"
        ))),
    }
}

fn replace_scalar(
    scalar: &ScalarValue,
    regex: &Regex,
    capture_index: usize,
) -> DataFusionResult<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(value) => {
            Ok(ScalarValue::Utf8(value.as_deref().map(|value| {
                replace_capture(value, regex, capture_index).into_owned()
            })))
        }
        ScalarValue::LargeUtf8(value) => {
            Ok(ScalarValue::LargeUtf8(value.as_deref().map(|value| {
                replace_capture(value, regex, capture_index).into_owned()
            })))
        }
        ScalarValue::Utf8View(value) => {
            Ok(ScalarValue::Utf8View(value.as_deref().map(|value| {
                replace_capture(value, regex, capture_index).into_owned()
            })))
        }
        other => Err(DataFusionError::Execution(format!(
            "{REGEXP_REPLACE_CAPTURE_UDF} expects a string source, got {other:?}"
        ))),
    }
}

fn replace_array(
    array: &ArrayRef,
    regex: &Regex,
    capture_index: usize,
) -> DataFusionResult<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{REGEXP_REPLACE_CAPTURE_UDF} received a non-Utf8 array"
                    ))
                })?;
            let mut builder = StringBuilder::new();
            for value in array.iter() {
                match value {
                    Some(value) => {
                        builder.append_value(replace_capture(value, regex, capture_index).as_ref())
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<GenericStringArray<i64>>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{REGEXP_REPLACE_CAPTURE_UDF} received a non-LargeUtf8 array"
                    ))
                })?;
            let mut builder = LargeStringBuilder::new();
            for value in array.iter() {
                match value {
                    Some(value) => {
                        builder.append_value(replace_capture(value, regex, capture_index).as_ref())
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Utf8View => {
            let array = as_string_view_array(array)?;
            let mut builder = StringViewBuilder::with_capacity(array.len());
            for value in array.iter() {
                match value {
                    Some(value) => {
                        builder.append_value(replace_capture(value, regex, capture_index).as_ref())
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        other => Err(DataFusionError::Execution(format!(
            "{REGEXP_REPLACE_CAPTURE_UDF} expects a string array, got {other:?}"
        ))),
    }
}

fn replace_capture<'a>(value: &'a str, regex: &Regex, capture_index: usize) -> Cow<'a, str> {
    let Some(captures) = regex.captures(value) else {
        return Cow::Borrowed(value);
    };
    let Some(full_match) = captures.get(0) else {
        return Cow::Borrowed(value);
    };
    let capture = captures
        .get(capture_index)
        .map(|capture| capture.as_str())
        .unwrap_or("");
    let mut result = String::with_capacity(
        full_match.start() + capture.len() + value.len().saturating_sub(full_match.end()),
    );
    result.push_str(&value[..full_match.start()]);
    result.push_str(capture);
    result.push_str(&value[full_match.end()..]);
    Cow::Owned(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::{
        array::{Array, ArrayRef, Int32Array, TimestampMicrosecondArray},
        datatypes::{DataType, TimeUnit},
    };
    use regex::Regex;

    use super::{extract_minute_array, replace_capture};

    #[test]
    fn replaces_first_match_with_capture() {
        let regex = Regex::new("b(..)").unwrap();
        assert_eq!(
            replace_capture("foobarbequebaz", &regex, 1),
            "fooarbequebaz"
        );
    }

    #[test]
    fn keeps_original_when_regex_does_not_match() {
        let regex = Regex::new("z(..)").unwrap();
        assert_eq!(
            replace_capture("foobarbequebaz", &regex, 1),
            "foobarbequebaz"
        );
    }

    #[test]
    fn uses_empty_string_for_missing_capture() {
        let regex = Regex::new("b(..)").unwrap();
        assert_eq!(replace_capture("foobarbequebaz", &regex, 2), "foobequebaz");
    }

    #[test]
    fn extracts_timestamp_minutes_with_euclidean_time() {
        let array = TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(59_000_000),
            Some(60_000_000),
            Some(-1),
            None,
        ])
        .with_data_type(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ));
        let result = extract_minute_array(&(Arc::new(array) as ArrayRef)).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.value(0), 0);
        assert_eq!(result.value(1), 0);
        assert_eq!(result.value(2), 1);
        assert_eq!(result.value(3), 59);
        assert!(result.is_null(4));
    }
}

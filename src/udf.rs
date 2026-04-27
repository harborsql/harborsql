use std::{any::Any, borrow::Cow, sync::Arc};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, GenericStringArray, LargeStringBuilder, StringBuilder,
            StringViewBuilder,
        },
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

pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(RegexpReplaceCaptureFunc::new()));
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
    use regex::Regex;

    use super::replace_capture;

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
}

use std::{any::Any, sync::Arc};

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

pub const EXTRACT_REFERER_HOST_UDF: &str = "harborsql_extract_referer_host";

pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(ExtractRefererHostFunc::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ExtractRefererHostFunc {
    signature: Signature,
}

impl ExtractRefererHostFunc {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ExtractRefererHostFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        EXTRACT_REFERER_HOST_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "{EXTRACT_REFERER_HOST_UDF} expects one argument"
            )));
        }

        match &args.args[0] {
            ColumnarValue::Scalar(scalar) => extract_scalar(scalar),
            ColumnarValue::Array(array) => extract_array(array).map(ColumnarValue::Array),
        }
    }
}

fn extract_scalar(scalar: &ScalarValue) -> DataFusionResult<ColumnarValue> {
    let value = match scalar {
        ScalarValue::Utf8(value) => {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                value.as_deref().map(extract_clickbench_referer_host),
            )));
        }
        ScalarValue::LargeUtf8(value) => {
            return Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(
                value.as_deref().map(extract_clickbench_referer_host),
            )));
        }
        ScalarValue::Utf8View(value) => value,
        other => {
            return Err(DataFusionError::Execution(format!(
                "{EXTRACT_REFERER_HOST_UDF} expects a string argument, got {other:?}"
            )));
        }
    };

    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(
        value.as_deref().map(extract_clickbench_referer_host),
    )))
}

fn extract_array(array: &ArrayRef) -> DataFusionResult<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{EXTRACT_REFERER_HOST_UDF} received a non-Utf8 array"
                    ))
                })?;
            let mut builder = StringBuilder::new();
            for value in array.iter() {
                match value {
                    Some(value) => builder.append_value(extract_clickbench_referer_host(value)),
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
                        "{EXTRACT_REFERER_HOST_UDF} received a non-LargeUtf8 array"
                    ))
                })?;
            let mut builder = LargeStringBuilder::new();
            for value in array.iter() {
                match value {
                    Some(value) => builder.append_value(extract_clickbench_referer_host(value)),
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
                    Some(value) => builder.append_value(extract_clickbench_referer_host(value)),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        other => Err(DataFusionError::Execution(format!(
            "{EXTRACT_REFERER_HOST_UDF} expects a string array, got {other:?}"
        ))),
    }
}

fn extract_clickbench_referer_host(value: &str) -> String {
    let Some(rest) = value
        .strip_prefix("http://")
        .or_else(|| value.strip_prefix("https://"))
    else {
        return value.to_string();
    };

    let rest = match rest.strip_prefix("www.") {
        Some(after_www) if after_www.find('/').is_some_and(|slash| slash > 0) => after_www,
        _ => rest,
    };

    match rest.find('/') {
        Some(slash) if slash > 0 && !rest[slash + 1..].contains('\n') => rest[..slash].to_string(),
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::extract_clickbench_referer_host;

    #[test]
    fn extracts_clickbench_referer_hosts() {
        assert_eq!(
            extract_clickbench_referer_host("http://example.com/path"),
            "example.com"
        );
        assert_eq!(
            extract_clickbench_referer_host("https://www.example.com/path?q=1"),
            "example.com"
        );
        assert_eq!(
            extract_clickbench_referer_host("ftp://www.example.com/path"),
            "ftp://www.example.com/path"
        );
        assert_eq!(
            extract_clickbench_referer_host("https://example.com"),
            "https://example.com"
        );
        assert_eq!(extract_clickbench_referer_host("http://www./path"), "www.");
        assert_eq!(
            extract_clickbench_referer_host("http://example.com/path\nmore"),
            "http://example.com/path\nmore"
        );
    }
}

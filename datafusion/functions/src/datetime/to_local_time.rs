// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::Any;

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Timestamp;
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};

use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct ToLocalTimeFunc {
    signature: Signature,
}

impl Default for ToLocalTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToLocalTimeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    fn to_local_time(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "to_local_time function requires 1 argument, got {}",
                args.len()
            );
        }

        let time_value = args[0].clone();
        let arg_type = time_value.data_type();
        match arg_type {
            DataType::Timestamp(_, None) => {
                // if no timezone specificed, just return the input
                Ok(time_value.clone())
            }
            // if has timezone, then remove the timezone in return type, keep the time value the same
            DataType::Timestamp(_, Some(_)) => match time_value {
                ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                    Some(ts),
                    Some(_),
                )) => Ok(ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                    Some(ts),
                    None,
                ))),
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                    Some(ts),
                    Some(_),
                )) => Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                    Some(ts),
                    None,
                ))),
                ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                    Some(ts),
                    Some(_),
                )) => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                    Some(ts),
                    None,
                ))),
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(ts),
                    Some(_),
                )) => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(ts),
                    None,
                ))),
                _ => {
                    exec_err!(
                        "to_local_time function requires timestamp argument, got {:?}",
                        arg_type
                    )
                }
            },
            _ => {
                exec_err!(
                    "to_local_time function requires timestamp argument, got {:?}",
                    arg_type
                )
            }
        }
    }
}

impl ScalarUDFImpl for ToLocalTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_local_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("to_local_time function requires 1 arguments, got 0");
        }

        match &arg_types[0] {
            Timestamp(Nanosecond, _) => Ok(Timestamp(Nanosecond, None)),
            Timestamp(Microsecond, _) => Ok(Timestamp(Microsecond, None)),
            Timestamp(Millisecond, _) => Ok(Timestamp(Millisecond, None)),
            Timestamp(Second, _) => Ok(Timestamp(Second, None)),
            _ => exec_err!(
                "The date_bin function can only accept timestamp as the second arg."
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.is_empty() {
            return exec_err!(
                "to_local_time function requires 1 or more arguments, got 0"
            );
        }

        self.to_local_time(args)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::ToLocalTimeFunc;

    #[test]
    fn test_to_local_time() {
        // TODO chunchun: update test cases to assert the result
        let res = ToLocalTimeFunc::new().invoke(&[ColumnarValue::Scalar(
            ScalarValue::TimestampSecond(Some(1), None),
        )]);
        assert!(res.is_ok());

        let res = ToLocalTimeFunc::new().invoke(&[ColumnarValue::Scalar(
            ScalarValue::TimestampSecond(Some(1), Some("+01:00".into())),
        )]);
        assert!(res.is_ok());

        let res = ToLocalTimeFunc::new().invoke(&[ColumnarValue::Scalar(
            ScalarValue::TimestampNanosecond(Some(1), Some("America/New_York".into())),
        )]);
        assert!(res.is_ok());

        let res = ToLocalTimeFunc::new().invoke(&[ColumnarValue::Scalar(
            // 2021-03-28T02:30:00-04:00
            ScalarValue::TimestampNanosecond(
                Some(1616916600),
                Some("America/New_York".into()),
            ),
        )]);
        assert!(res.is_ok());
    }
}

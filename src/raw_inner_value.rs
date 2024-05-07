use sqlparser::ast::{ColumnDef, Expr, DataType, ReferentialAction, Value};

pub trait InnerRawValue {
    fn get_inner_raw_str(&self)->Option<String>;
}


impl InnerRawValue for Value {
    fn get_inner_raw_str(&self)->Option<String> {
        match self {
            Value::Number(v, _l) => Some(v.clone()),
            Value::DoubleQuotedString(v) => Some(v.clone()),
            Value::SingleQuotedString(v) => Some(v.clone()),
            Value::DollarQuotedString(v) => Some(v.value.clone()),
            Value::EscapedStringLiteral(v) => Some(v.clone()),
            Value::NationalStringLiteral(v) => Some(v.clone()),
            Value::HexStringLiteral(v) => Some(v.clone()),
            Value::Boolean(v) => if *v { Some("true".to_string()) } else { Some("false".to_string()) },
            Value::SingleQuotedByteStringLiteral(v) => Some(v.clone()),
            Value::DoubleQuotedByteStringLiteral(v) => Some(v.clone()),
            Value::RawStringLiteral(v) => Some(v.clone()),
            Value::Null => None,
            Value::UnQuotedString(v) => Some(v.clone()),
            _=>None
        }
    }
}
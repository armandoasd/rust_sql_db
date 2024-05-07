use sqlparser::ast::{DataType, CharacterLength, Value, ExactNumberInfo};
use chrono::{ NaiveDate, NaiveDateTime, NaiveTime };
use serde_json::{Value as JsonValue};

const ABSOLUTE_CHAR_MAX:u64 =  536_870_912;

fn validate_value_for_char(length_p:&Option<CharacterLength>, value: &Value, def_size:u64, max_size:u64) -> bool {
    let length = if let Some(len) = length_p {
        match len {
            CharacterLength::IntegerLength {
                length,
                unit,
            } => *length,
            CharacterLength::Max => max_size,
        }
    } else {
        def_size
    };
    match value {
        Value::SingleQuotedString(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::EscapedStringLiteral(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::DoubleQuotedString(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        _=>{false}
    }
}

fn validate_value_for_clob(len_p_u64:&Option<u64>, value: &Value, max_size:u64) -> bool {
    if let Some(len) = len_p_u64 {
        validate_value_for_char(&None, value, *len, ABSOLUTE_CHAR_MAX)
    }else {
        validate_value_for_char(&None, value, 60, ABSOLUTE_CHAR_MAX)
    }
}

fn validate_value_for_binary(len_p_u64:&Option<u64>, value: &Value, def_size:u64) -> bool {
    let length = if let Some(len) = len_p_u64 {
        *len
    }else {
        def_size
    };
    match value {
        Value::Number(inner_value, _) => {
            (inner_value.len() as u64) <= length
        },
        Value::SingleQuotedString(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::EscapedStringLiteral(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::SingleQuotedByteStringLiteral(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::DoubleQuotedByteStringLiteral(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::RawStringLiteral(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::HexStringLiteral(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::DoubleQuotedString(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::UnQuotedString(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        _=>{false}
    }
}
fn validate_value_for_bytes(len_p_u64:&Option<u64>, value: &Value, def_size:u64) -> bool {
    let length = if let Some(len) = len_p_u64 {
        *len
    }else {
        def_size
    };
    match value {
        Value::Number(inner_value, _) => {
            (inner_value.len() as u64) <= length
        },
        Value::SingleQuotedByteStringLiteral(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::DoubleQuotedByteStringLiteral(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::HexStringLiteral(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        Value::UnQuotedString(inner_value) => {
            (inner_value.len() as u64) <= length
        },
        _=>{false}
    }
}

//def precizion 18
fn validate_value_for_decimal(dec_info:&ExactNumberInfo, value: &Value, def_precizion:u64) -> bool {
    match value {
        Value::Number(inner_value, _) => {
            //inner_value.len() <= length
            if let Err(_) = inner_value.parse::<f64>() {
                return false;
            }
            let parts:Vec<&str> = inner_value.split(".").collect();
            let value_precizion = (inner_value.len()-1) as u64;
            if parts.len() != 2 {
                return false;
            }
            let value_scale = parts.get(1).unwrap().len() as u64;
            match dec_info {
                ExactNumberInfo::None => {
                    value_precizion <= def_precizion
                },
                ExactNumberInfo::Precision(precizion) => {
                    value_precizion <= *precizion
                },
                ExactNumberInfo::PrecisionAndScale(precizion, scale) => {
                    (value_precizion <= *precizion) && (value_scale <= *scale)
                },
            }
        },
        _=>{false}
    }
}

fn validate_value_for_float(len_p_u64:&Option<u64>, value: &Value, def_precizion:u64) -> bool {
    if let Some(len) = len_p_u64 {
        validate_value_for_decimal(&ExactNumberInfo::None, value, *len)
    }else {
        validate_value_for_decimal(&ExactNumberInfo::None, value, def_precizion)
    }
}

fn validate_value_for_integer(len_p_u64:&Option<u64>, value: &Value, def_precizion:u64, sign: bool) -> bool {
    let length = if let Some(len) = len_p_u64 {
        *len
    }else {
        def_precizion
    };
    match value {
        Value::Number(inner_value, _) => {
            let value_len:u64 = inner_value.len() as u64;
            let can_parse = if sign {
                inner_value.parse::<i64>().is_ok()
            } else {
                inner_value.parse::<u64>().is_ok()
            };
            if !can_parse {
                return false;
            }else if value_len > def_precizion {
                return false;
            }else {
                return true;
            }
        },
        _=>{false}
    }
}

fn validate_value_for_bool(value: &Value)->bool{
    match value {
        Value::Number(inner_value, _) => {
            if inner_value == "1" || inner_value == "0" {
                return true;
            }else {
                return false;
            }
        }
        Value::Boolean(_) => {true},
        _=>{false}
    }
}

#[derive(Debug)]
enum DateTypes {
    Date,
    Time,
    DateTime,
    Timestamp
}

fn validate_value_for_date_string_general(value_str: &String, date_type: DateTypes) -> bool {
    match date_type {
        DateTypes::Date => NaiveDate::parse_from_str(value_str, "%Y-%m-%d").is_ok(),
        DateTypes::Time => NaiveTime::parse_from_str(value_str, "%H:%M:%S").is_ok(),
        DateTypes::DateTime => NaiveDateTime::parse_from_str(value_str, "%Y-%m-%d %H:%M:%S").is_ok(),
        DateTypes::Timestamp => NaiveDateTime::parse_from_str(value_str, "%Y-%m-%d %H:%M:%S.%2f").is_ok(),
    }
}

fn validate_value_for_dates_general(value: &Value, date_type: DateTypes) -> bool {
    match value {
        Value::SingleQuotedString(inner_value) => {
            validate_value_for_date_string_general(inner_value, date_type)
        },
        Value::EscapedStringLiteral(inner_value) => {
            validate_value_for_date_string_general(inner_value, date_type)
        },
        Value::DoubleQuotedString(inner_value) => {
            validate_value_for_date_string_general(inner_value, date_type)
        },
        _=>{false}
    }
}

fn validate_value_for_json(value: &Value) -> bool {
    match value {
        Value::SingleQuotedString(inner_value) => {
            serde_json::from_str::<JsonValue>(inner_value).is_ok()
        },
        Value::EscapedStringLiteral(inner_value) => {
            serde_json::from_str::<JsonValue>(inner_value).is_ok()
        },
        Value::DoubleQuotedString(inner_value) => {
            serde_json::from_str::<JsonValue>(inner_value).is_ok()
        },
        _=>{false}
    }
}

fn validate_value_for_enum(set:&Vec<String>, value:&Value)-> bool{
    match value {
        Value::SingleQuotedString(inner_value) => {
            set.contains(inner_value)
        },
        Value::UnQuotedString(inner_value) => {
            set.contains(inner_value)
        },
        Value::DoubleQuotedString(inner_value) => {
            set.contains(inner_value)
        },
        _=>{false}
    }
}

pub fn validate_value_for_col(type_name: &DataType, value: &Value, nullable:bool) -> bool {
    if let Value::Null = value {
        return nullable;
    }
    match type_name {
        DataType::Character(length_p)=>{
            validate_value_for_char(length_p, value, 1, ABSOLUTE_CHAR_MAX)
        },
        DataType::Char(length_p)=>{
            validate_value_for_char(length_p, value, 1, ABSOLUTE_CHAR_MAX)
        },
        DataType::CharacterVarying(length_p)=>{
            validate_value_for_char(length_p, value, 30, ABSOLUTE_CHAR_MAX)
        },
        DataType::CharVarying(length_p)=>{
            validate_value_for_char(length_p, value, 30, ABSOLUTE_CHAR_MAX)
        },
        DataType::Varchar(length_p)=>{
            validate_value_for_char(length_p, value, 30, ABSOLUTE_CHAR_MAX)
        },
        DataType::Nvarchar(len_p_u64)=>{
            validate_value_for_clob(len_p_u64, value, ABSOLUTE_CHAR_MAX)
        },
        DataType::Uuid=>{
            validate_value_for_char(&None, value, 30, ABSOLUTE_CHAR_MAX)
        },
        DataType::CharacterLargeObject(len_p_u64)=>{
            validate_value_for_clob(len_p_u64, value, ABSOLUTE_CHAR_MAX)
        },
        DataType::CharLargeObject(len_p_u64)=>{
            validate_value_for_clob(len_p_u64, value, ABSOLUTE_CHAR_MAX)
        },
        DataType::Clob(len_p_u64)=>{
            validate_value_for_clob(len_p_u64, value, ABSOLUTE_CHAR_MAX)
        },
        DataType::Binary(len_p_u64)=>{
            validate_value_for_binary(len_p_u64, value, 30)
        },
        DataType::Varbinary(len_p_u64)=>{
            validate_value_for_binary(len_p_u64, value, 30)
        },
        DataType::Blob(len_p_u64)=>{
            validate_value_for_binary(len_p_u64, value, 30)
        },
        DataType::Bytes(len_p_u64)=>{
            validate_value_for_bytes(len_p_u64, value, 30)
        },
        DataType::Numeric(number_info)=>{
            validate_value_for_decimal(number_info, value, 18)
        },
        DataType::Decimal(number_info)=>{
            validate_value_for_decimal(number_info, value, 18)
        },
        DataType::BigNumeric(number_info)=>{
            validate_value_for_decimal(number_info, value, 18)
        },
        DataType::BigDecimal(number_info)=>{
            validate_value_for_decimal(number_info, value, 18)
        },
        DataType::Dec(number_info)=>{
            validate_value_for_decimal(number_info, value, 18)
        },
        DataType::Float(len_p_u64)=>{
            validate_value_for_float(len_p_u64, value, 18)
        },
        DataType::TinyInt(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 4, true)
        },
        DataType::UnsignedTinyInt(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 3, false)
        },
        DataType::Int2(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 6, false)
        },
        DataType::UnsignedInt2(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 5, false)
        },
        DataType::SmallInt(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 6, false)
        },
        DataType::UnsignedSmallInt(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 5, false)
        },
        DataType::MediumInt(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 8, false)
        },
        DataType::UnsignedMediumInt(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 8, false)
        },
        DataType::Int(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 11, false)
        },
        DataType::Int4(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 11, false)
        },
        DataType::Int64 => validate_value_for_integer(&None, value, 21, false),
        DataType::Integer(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 11, false)
        },
        DataType::UnsignedInt(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 11, false)
        },
        DataType::UnsignedInt4(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 11, false)
        },
        DataType::UnsignedInteger(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 11, false)
        },
        DataType::BigInt(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 21, false)
        },
        DataType::UnsignedBigInt(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 20, false)
        },
        DataType::Int8(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 11, false)
        },
        DataType::UnsignedInt8(len_p_u64)=>{
            validate_value_for_integer(len_p_u64, value, 11, false)
        },
        DataType::Float4=>{
            validate_value_for_float(&None, value, 11)
        },
        DataType::Float64=>{
            validate_value_for_float(&None, value, 21)
        },
        DataType::Real=>{
            validate_value_for_float(&None, value, 21)
        },
        DataType::Float8=>{
            validate_value_for_float(&None, value, 11)
        },
        DataType::Double=>{
            validate_value_for_float(&None, value, 21)
        },
        DataType::DoublePrecision=>{
            validate_value_for_float(&None, value, 21)
        },
        DataType::Bool=>{
            validate_value_for_bool(value)
        },
        DataType::Boolean=>{
            validate_value_for_bool(value)
        },
        DataType::Date => {
            validate_value_for_dates_general(&value, DateTypes::Date)
        },
        DataType::Time(_precizion, _timezone_i)=> {
            validate_value_for_dates_general(&value, DateTypes::Time)
        },
        DataType::Datetime(_precizion)=> {
            validate_value_for_dates_general(&value, DateTypes::DateTime)
        },
        DataType::Timestamp(_precizion, _timezone_i)=> {
            validate_value_for_dates_general(&value, DateTypes::Timestamp)
        },
        DataType::JSON => {
            validate_value_for_json(&value)
        },
        DataType::JSONB => {
            validate_value_for_json(&value)
        },
        DataType::Text =>{
            validate_value_for_char(&None, value, 60, ABSOLUTE_CHAR_MAX)
        },
        DataType::String(len_p_u64)=>{
            validate_value_for_clob(len_p_u64, value, ABSOLUTE_CHAR_MAX)
        },
        DataType::Bytea => {
            validate_value_for_binary(&None, value, 30)
        },
        //Array(ArrayElemTypeDef),
        DataType::Enum(set) => {
            validate_value_for_enum(set, value)
        },
        DataType::Set(set) => {
            validate_value_for_enum(set, value)
        },
        _=>{false}
    }
}
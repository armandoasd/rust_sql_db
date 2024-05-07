use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use sqlparser::ast::{ColumnDef, Expr, DataType, ReferentialAction, Value};
use crate::validators::validate_value_for_col;
use crate::raw_inner_value::InnerRawValue;
use std::ops::Deref;
use buffers_unsafe_copy::vector::unsafe_copy;
use buffers_unsafe_copy::string::{unsafe_copy as unsafe_copy_str, unsafe_copy_option_str};

pub enum SelectFields {
    WildCard,
    NamedFields(Vec<String>)
}

#[derive(Debug)]
pub enum ColumnProperties {
    AutoIncrement,
    PriamryKey,
    Null,
    NotNull,
    Unique,
    Default(Expr),
    OnUpdate(Expr),
    ForeignKey {
        foreign_table: String,
        referred_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
}

#[derive(Debug)]
pub struct ColumnInfo {
    pub type_name: DataType,
    pub properties: Vec<ColumnProperties>
}

impl ColumnInfo {
    pub fn validate_value(&self, value: &Value)-> bool {
        let is_nullable = self.properties.iter().find(|&prop| {
            match prop {
                ColumnProperties::Null => true,
                ColumnProperties::AutoIncrement => true,
                ColumnProperties::Default(_expr)=> true,
                _=> false
            }
        }).is_some();
        validate_value_for_col(&self.type_name, value, is_nullable)
    }
}

#[derive(Debug)]
pub struct TableRowData {
    column_data: HashMap<String, Option<String>>,
}

#[derive(Debug)]
pub struct TableData {
    row_data: HashMap<String, Arc<TableRowData>>,//HashMap<"pk", {..data}>/HashMap<"pk1_pk2", {..data}>
    indexed_data: HashMap<String, Arc<TableRowData>>,
}

#[derive(Debug)]
pub struct TableInfo {
    table_name: String,
    primary_keys: Vec<String>,
    indexes: Vec<String>,
    column_properies: HashMap<String, ColumnInfo>,
    data: TableData,
}

#[derive(Debug)]
pub struct DataBase {
    tables: HashMap<String, Arc<Mutex<TableInfo>>>,
}

impl TableRowData {
    pub fn new() -> Self {
        Self {
            column_data: HashMap::new(),
        }
    }
    pub fn get_wild_card(&self) -> Vec<&Option<String>> {
        self.column_data.values().collect()
    }

    pub fn get_named_fields(&self, fields: &Vec<String>) -> Vec<&Option<String>> {
        fields.iter().map(|field_name| self.column_data.get(field_name).unwrap()).collect()
    }
}

impl TableData {
    pub fn new() -> Self {
        Self {
            row_data: HashMap::new(),
            indexed_data: HashMap::new(),
        }
    }

    pub fn find_all(&self, selection:SelectFields) -> Vec<Vec<&Option<String>>>{
        match selection {
            SelectFields::WildCard => {
                self.row_data.values().map(|val_ref| val_ref.get_wild_card()).collect()
            },
            SelectFields::NamedFields(fields) => {
                self.row_data.values().map(|val_ref| val_ref.get_named_fields(&fields)).collect()
            }
        }
    }
}

impl TableInfo {
    pub fn new(
        table_name: String,
        primary_keys: Vec<String>,
        indexes: Vec<String>,
        column_properies: HashMap<String, ColumnInfo>
    ) -> Self {

        Self {
            table_name,
            primary_keys,
            indexes,
            column_properies,
            data: TableData::new(),
        }
    }

    pub fn insert_values(&mut self, columns: &Vec<String>, values: Vec<Vec<Expr>>) -> Result<(), &'static str>{
        //let columns_with_prop:  Vec<(String, ColumnInfo)> = Vec::new();
        if columns.len() == 0 || columns.len() > self.column_properies.len() {
            return Err("wrong number of columns");
        }
        let mut found = 0;
        for (c_name, c_props) in &self.column_properies {
            if let Some (_) = columns.iter().find(|&c_name_i| c_name_i.eq(c_name)) {
                found += 1;
                //columns_with_prop.push((c_name, c_props));
            }
        }
        if columns.len() != found {
            return Err("the colummn list does not exist for table");
        }
        for val_row in values {
            if columns.len() != val_row.len() {
                return Err("wrong number of columns");
            }
            let mut pk_string = "".to_string();
            let mut row_data = TableRowData::new();
            for (c_name, value_expr) in columns.iter().zip(val_row.iter()) {
                let col_prop = self.column_properies.get(c_name).unwrap();
                if let Expr::Value(value) = value_expr {
                    let value_raw = value.get_inner_raw_str();
                    if self.primary_keys.contains(c_name){
                        if pk_string.len()>0 {
                            pk_string.push_str("_");
                        }
                        if let Some(v) = &value_raw {
                            pk_string.push_str(v);
                        }else {
                            pk_string.push_str("|");
                        }
                    }
                    if col_prop.validate_value(value){
                        row_data.column_data.insert(unsafe_copy_str(c_name), value_raw);
                    }
                }else {
                    return Err("only raw values are supported for insert");
                }

            }
            self.data.row_data.insert(pk_string, Arc::new(row_data));
        }
        Ok(())
    }
    pub fn find_all(&self, selection:SelectFields) -> Vec<Vec<&Option<String>>> {
        self.data.find_all(selection)
    }
}

impl DataBase {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
    //#[inline]
    pub fn create_table(&mut self,
        table_name: &String,
        primary_keys: Vec<String>,
        indexes: Vec<String>,
        column_properies: HashMap<String, ColumnInfo>
    ) {
        self.tables.insert(unsafe_copy_str(table_name), Arc::new(Mutex::new(TableInfo::new(unsafe_copy_str(table_name), primary_keys, indexes, column_properies))));
    }

    //#[inline]
    pub fn table_exists(&mut self, table_name: &String) -> bool {
        self.tables.contains_key(table_name)
    }

    pub fn get_table_ref(&mut self, table_name: &String) -> Option<&Arc<Mutex<TableInfo>>> {
        self.tables.get(table_name)
    }
}

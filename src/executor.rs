use crate::data_descriptor::{ ColumnInfo, ColumnProperties, DataBase, SelectFields};
use sqlparser::ast::{Statement, ObjectName, ColumnOption, SetExpr, TableFactor, SelectItem};
use std::collections::HashMap;
use buffers_unsafe_copy::string::{unsafe_copy as unsafe_copy_str, unsafe_copy_option_str};

#[derive(Debug)]
pub enum SuccessStatus {
    TableCreated(String),
    DataInserted,
    DataFetched(Vec<u8>)
}

pub trait Execute {
    fn execute(&self, db: &mut DataBase) -> Result<SuccessStatus, String>;
}

impl Execute for Statement {
    fn execute(&self, db: &mut DataBase) -> Result<SuccessStatus, String> {
        match self {
            Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            } => {
                let ObjectName(name_idents) = name;
                let table_name_str = unsafe_copy_str(&name_idents.get(0).expect("A valid table name must be provided").value);

                let mut primary_keys: Vec<String> = Vec::new();
                let mut indexes: Vec<String> = Vec::new();
                let mut column_properies: HashMap<String, ColumnInfo> = HashMap::new();
                for column in columns {
                    let col_name_str = unsafe_copy_str(&column.name.value);
                    let data_type = column.data_type.clone();
                    let mut column_property_list: Vec<ColumnProperties> =  Vec::new();
                    for option_def in column.options.clone() {
                        match option_def.option {
                            ColumnOption::Null => {column_property_list.push(ColumnProperties::Null);},
                            ColumnOption::NotNull => {column_property_list.push(ColumnProperties::NotNull);},
                            ColumnOption::Default(expr) => {column_property_list.push(ColumnProperties::Default(expr));},
                            ColumnOption::Unique {
                                is_primary,
                                characteristics
                            } => {
                                if is_primary {
                                    column_property_list.push(ColumnProperties::PriamryKey);
                                    primary_keys.push(col_name_str.clone());
                                }
                                column_property_list.push(ColumnProperties::Unique);
                                indexes.push(col_name_str.clone());
                            },
                            ColumnOption::ForeignKey {
                                foreign_table,
                                referred_columns,
                                on_delete,
                                on_update,
                                characteristics
                            } => {
                                let ObjectName(foreign_table_ident) = foreign_table;
                                let f_table_name_str = foreign_table_ident.get(0).expect("A valid table name must be provided").value.clone();
                                let ref_col_str_l = referred_columns.into_iter().map(|e| e.value).collect();
                                column_property_list.push(ColumnProperties::ForeignKey {
                                    foreign_table: f_table_name_str,
                                    referred_columns: ref_col_str_l,
                                    on_delete,
                                    on_update
                                });
                            },
                            ColumnOption::OnUpdate(expr) => {column_property_list.push(ColumnProperties::OnUpdate(expr));},
                            ColumnOption::DialectSpecific(tokens) => {
                                if let sqlparser::tokenizer::Token::Word(key_word) = tokens.get(0).unwrap(){
                                    column_property_list.push(ColumnProperties::AutoIncrement);
                                }
                            },
                            _ => {}
                        }
                    }
                    let col_info = ColumnInfo {
                        type_name: data_type,
                        properties: column_property_list,
                    };
                    column_properies.insert(col_name_str.clone(), col_info);
                }
                db.create_table(&table_name_str, primary_keys, indexes, column_properies);

                Ok(SuccessStatus::TableCreated(table_name_str))
            },
            Statement::Insert {
                table_name,
                columns,
                into,
                source,
                overwrite,
                ..
            } => {
                let ObjectName(name_idents) = table_name;
                let table_name_str = unsafe_copy_str(&name_idents.get(0).expect("A valid table name must be provided").value);
                let values = if let SetExpr::Values(value_list) = *source.clone().expect("No values to insert provided").body.clone() {
                    value_list.rows
                } else {
                    Vec::new()
                };
                let col_names: Vec<String> = columns.iter().map(|col| unsafe_copy_str(&col.value)).collect();
                if let Some(ref table) = db.get_table_ref(&table_name_str){
                    table.lock().unwrap().insert_values(&col_names, values);
                    Ok(SuccessStatus::DataInserted)
                }else {
                    Err(format!("No table named {} exists", table_name_str))
                }

            },
            Statement::Query (query) => {
                match &*query.body {
                    SetExpr::Select(select_q) => {
                        let mut ret_value: Vec<u8> = Vec::new();
                        for data_from in &*select_q.from {
                            match &data_from.relation {
                                TableFactor::Table {
                                    name,
                                    ..
                                } => {
                                    let ObjectName(name_idents) = name;
                                    let table_name_str = name_idents.get(0).expect("A valid table name must be provided").value.clone();

                                    if let Some(ref table) = db.get_table_ref(&table_name_str){
                                        for projection in &select_q.projection {
                                            match projection {
                                                SelectItem::Wildcard(_options) => {
                                                    //table.lock().unwrap().find_all(SelectFields::WildCard);
                                                    ret_value.append(&mut bitcode::encode(&table.lock().unwrap().find_all(SelectFields::WildCard)));
                                                    //ret_value.append(&mut serde_json::to_vec(&table.lock().unwrap().find_all(SelectFields::WildCard)).unwrap());
                                                    
                                                },
                                                _=> {return Err("not yet implemented".to_string());}
                                            }
                                        }
                                    }else {
                                        return Err(format!("No table named {} exists", table_name_str));
                                    }
                                },
                                _=> {return Err("not yet implemented".to_string());}
                            }
                        }
                        Ok(SuccessStatus::DataFetched(ret_value))
                    },
                    _=>{Err("not yet implemented".to_string())}
                }
            },
            _ => { Err("not yet implemented".to_string()) }
        }
    }
}
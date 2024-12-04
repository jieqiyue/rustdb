use crate::sql::types::DataType;
use std::collections::BTreeMap;

// Abstract Syntax Tree 抽象语法树定义
#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable { name: String, columns: Vec<Column> },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    } ,
    // 由于目前仅仅实现的是select * from xxxx表，这种类型的语句，所以这里仅仅存储一下表名就可以了。
    Select { 
        table_name:String 
    },
    Update {
        table_name: String,
        columns: BTreeMap<String, Expression>,
        where_clause: Option<(String, Expression)>,
    },
}

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Consts(Consts),
}

impl From<Consts> for Expression {
    fn from(value: Consts) -> Self {
        Self::Consts(value)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Consts {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}
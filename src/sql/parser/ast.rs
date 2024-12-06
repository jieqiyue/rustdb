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
        select: Vec<(Expression, Option<String>)>,
        from: FromItem,
        order_by: Vec<(String, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    
    Update {
        table_name: String,
        columns: BTreeMap<String, Expression>,
        where_clause: Option<(String, Expression)>,
    },
    Delete {
        table_name: String,
        where_clause: Option<(String, Expression)>,
    },
}

#[derive(Debug, PartialEq)]
pub enum FromItem {
    Table {
        name: String,
    },

    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
    },
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

#[derive(Debug, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
}

// 表达式定义，目前只有常量和列名
#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Field(String),
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
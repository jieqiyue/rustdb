use serde::{Deserialize, Serialize};
use crate::sql::types::{DataType, Value};

// 执行器里面和执行节点里面使用的都是同一个这个定义的Table
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>
}

// 属于schema的Column，和parser阶段的列不同
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
}
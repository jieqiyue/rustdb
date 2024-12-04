use serde::{Deserialize, Serialize};
use crate::error::{Error, Result};

use super::types::{DataType, Row, Value};

// 执行器里面和执行节点里面使用的都是同一个这个定义的Table
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>
}
impl Table {
    // 校验表的有效性
    pub fn validate(&self) -> Result<()> {
        // 校验是否有列信息
        if self.columns.is_empty() {
            return Err(Error::Internal(format!(
                "table {} has no columns",
                self.name
            )));
        }

        // 校验是否有主键
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => {
                return Err(Error::Internal(format!(
                    "No primary key for table {}",
                    self.name
                )))
            }
            _ => {
                return Err(Error::Internal(format!(
                    "Multiple primary keys for table {}",
                    self.name
                )))
            }
        }

        Ok(())
    }

    pub fn get_primary_key(&self, row: &Row) -> Result<Value> {
        let pos = self
            .columns
            .iter()
            .position(|c| c.primary_key)
            .expect("No primary key found");
        Ok(row[pos].clone())
    }

    pub fn get_col_index(&self, col_name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or(Error::Internal(format!("column {} not found", col_name)))
    }
}

// 属于schema的Column，和parser阶段的列不同
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
}
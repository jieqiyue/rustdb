use std::collections::HashMap;
use std::ffi::c_long;
use crate::error::{Error, Result};
use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::parser::ast::{Expression, Statement};

use crate::sql::schema::Table;
use crate::sql::types::{DataType, Row, Value};

// Insert可以看作是dml语句，所以存放在mutation.rs文件中

pub struct Insert{
    table_name: String,
    columns: Vec<String>,
    // 最外层的Vec代表了有很多行数据，里面的Vec是每一行数据中不同列的值
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table_name: String, 
               columns: Vec<String>, 
               values: Vec<Vec<Expression>>) -> Box<Self> {
        Box::new(Self {
                table_name, 
                columns, 
                values
            })
    }
}

// 列对齐
// tbl:
// insert into tbl values(1, 2, 3);
// a       b       c          d
// 1       2       3      default 填充
fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut results = row.clone();
    
    // 跳过已有的列
    for column in table.columns.iter().skip(row.len()) {
        if let Some(default) = &column.default {
            results.push(default.clone());
        } else {
            return Err(Error::Internal(format!(
                "No default value for column {}",
                column.name
            )));
        }
    }

    Ok(results)
}

// 为了处理SQL语句中，指定了SQL语句列的情况，这个列顺序是随机指定的，所以这里需要转化一下
// tbl:
// insert into tbl(d, c) values(1, 2);
//    a          b       c          d
// default   default     2          1
fn make_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row> {
    // 判断列数是否和value数一致
    if columns.len() != values.len() {
        return Err(Error::Internal(format!("columns and values num mismatch")));
    }

    let mut inputs = HashMap::new();
    for (i, col_name) in columns.iter().enumerate() {
        inputs.insert(col_name, values[i].clone());
    }

    let mut results = Vec::new();
    for col in table.columns.iter() {
        if let Some(value) = inputs.get(&col.name) {
            results.push(value.clone());
        } else if let Some(value) = &col.default {
            results.push(value.clone());
        } else {
            return Err(Error::Internal(format!(
                "No value given for the column {}",
                col.name
            )));
        }
    }

    Ok(results)
}

impl<T:Transaction> Executor<T> for Insert {
    fn execute(self:Box<Self>, txn: &mut T) -> crate::error::Result<super::ResultSet> {
        let mut count = 0;
        let table = txn.must_get_table(self.table_name.clone())?;
        for exprs in self.values{
            // 这个row是SQL语句中填写了名字的列名
            let row = exprs.into_iter().map(|expr|{Value::from_expression(expr)})
                .collect::<Vec<_>>();
            
            // 如果SQL语句中没有指定插入的列的名称，那么就需要当这几个列和定义里面的列的前几列进行匹配
            let insert_row = if self.columns.is_empty(){
                pad_row(&table, &row)?
            }else { 
                // 如果指定了列，那么就将指定的列和表定义的时候的列顺序对应起来，并且把没有指定列的值，并且没有默认值的列抛出异常
                make_row(&table, &self.columns, &row)?
            };
            
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }
        
        Ok(ResultSet::Insert {count})
    }
}
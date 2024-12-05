use crate::{
    error::{Error, Result},
    sql::{
        parser::ast,
        schema::{self, Table},
        types::Value,
    },
};

use super::{Node, Plan};

pub struct Planner;

/*
    Planner
        Planner作为一个辅助的结构体，作用就是将ast抽象语法树转化为一个执行计划Plan。Plan并不会直接去解析ast。
 */
impl Planner {
    pub fn new() -> Self {
        Self{}
    }
    
    pub fn build(&mut self, stmt:ast::Statement)-> Result<Plan>{
        Ok(Plan(self.build_statment(stmt)?))
    }

    fn build_statment(&self, stmt: ast::Statement) -> Result<Node> {
        Ok(match stmt {
            ast::Statement::CreateTable { name, columns } => Node::CreateTable {
                schema: Table {
                    name,
                    columns: columns
                        .into_iter()
                        .map(|c| {
                            let nullable = c.nullable.unwrap_or(!c.primary_key);
                            let default = match c.default {
                                Some(expr) => Some(Value::from_expression(expr)),
                                None if nullable => Some(Value::Null),
                                None => None,
                            };

                            schema::Column {
                                name: c.name,
                                datatype: c.datatype,
                                nullable,
                                default,
                                primary_key: c.primary_key,
                            }
                        })
                        .collect(),
                },
            },
            ast::Statement::Insert {
                table_name,
                columns,
                values,
            } => Node::Insert {
                table_name,
                columns: columns.unwrap_or_default(),
                values,
            },
            ast::Statement::Select {
                table_name,
                order_by,
                limit,
                offset,
            } => {
                let mut node = Node::Scan {
                    table_name,
                    filter: None,
                };

                // order by
                if !order_by.is_empty() {
                    node = Node::Order {
                        source: Box::new(node),
                        order_by,
                    }
                }

                // 由于可能同时存在offset和limit关键字，所以当两者都存在的时候，需要先进行limit，再进行offset，所以
                // 在这里构建Node的时候，需要先处理limit存在的情况。
                // 由于offset和limit关键字一般是出现在select语句当中的，所以这里是在ast::Statement::Select中进行处理。
                // offset
                if let Some(expr) = offset {
                    node = Node::Offset {
                        source: Box::new(node),
                        offset: match Value::from_expression(expr) {
                            Value::Integer(i) => i as usize,
                            _ => return Err(Error::Internal("invalid offset".into())),
                        },
                    }
                }
                
                // 由于这种递归的关系，所以到时候处理的时候，会先进行source的处理，就会有一个递归的效果
                // limit
                if let Some(expr) = limit {
                    node = Node::Limit {
                        source: Box::new(node),
                        limit: match Value::from_expression(expr) {
                            Value::Integer(i) => i as usize,
                            _ => return Err(Error::Internal("invalid limit".into())),
                        },
                    }
                }

                node
            },
            ast::Statement::Update {
                table_name,
                columns,
                where_clause,
            } => Node::Update {
                table_name: table_name.clone(),
                source: Box::new(Node::Scan {
                    table_name,
                    filter: where_clause,
                }),
                columns,
            },
            ast::Statement::Delete {
                table_name,
                where_clause,
            } => Node::Delete {
                table_name: table_name.clone(),
                source: Box::new(Node::Scan {
                    table_name,
                    filter: where_clause,
                }),
            },
        })
    }
}
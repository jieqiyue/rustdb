use crate::sql::parser::ast;
use crate::sql::parser::ast::Statement;
use super::{Node, Plan};
use crate::sql::{
    schema::{self, Table},
    types::Value,
};
pub struct Planner;

/*
    Planner
        Planner作为一个辅助的结构体，作用就是将ast抽象语法树转化为一个执行计划Plan。Plan并不会直接去解析ast。
 */
impl Planner {
    pub fn new() -> Self {
        Self{}
    }
    
    pub fn build(&mut self, stmt:ast::Statement)-> Plan{
        Plan(self.build_statment(stmt))
    }

    fn build_statment(&self, stmt: ast::Statement) -> Node {
        match stmt {
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
        }
    }
}
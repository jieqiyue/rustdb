use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};

// 查询相关的执行器，单独定义在这里

pub struct Scan{
    table_name:String
}

impl Scan{
    pub fn new(table_name:String) -> Box<Self>{
        Box::new(Self{table_name})
    }
    
}

impl<T:Transaction> Executor<T> for Scan{
    fn execute(self:Box<Self>, txn: &mut T) -> crate::error::Result<ResultSet> {
        let table = txn.must_get_table(self.table_name.clone())?;
        let rows = txn.scan_table(self.table_name.clone())?;
        
        Ok(ResultSet::Scan {
            column:table.columns.into_iter().map(|c|c.name.clone()).collect(),
            rows
        })
    }
}
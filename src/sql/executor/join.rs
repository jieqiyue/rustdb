use crate::{
    error::{Error, Result},
    sql::engine::Transaction,
};

use super::{Executor, ResultSet};

pub struct NestedLoopJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(left: Box<dyn Executor<T>>, right: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { left, right })
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // 先执行左边的
        if let ResultSet::Scan {
            columns: lcols,
            rows: lrows,
        } = self.left.execute(txn)?
        {
            let mut new_rows = Vec::new();
            let mut new_cols = lcols;
            // 再执行右边的
            if let ResultSet::Scan {
                columns: rcols,
                rows: rrows,
            } = self.right.execute(txn)?
            {
                new_cols.extend(rcols);

                for lrow in &lrows {
                    for rrow in &rrows {
                        let mut row = lrow.clone();
                        row.extend(rrow.clone());
                        new_rows.push(row);
                    }
                }
            }
            
            // 这里虽然是NestedLoopJoin，但是ResultSet确是Scan的，所以即使有递归的调用，也能够符合到上面的解构，
            // if let ResultSet::Scan。而如果某一个NestedLoopJoin的left还是一个NestedLoopJoin的话，那么它还是
            // 会调用到execute方法继续递归的查询，但是叶子节点肯定是一个Scan节点。所以从叶子节点往上返回递归的时候，
            // 就能够用上ResultSet::Scan的解构。
            return Ok(ResultSet::Scan {
                columns: new_cols,
                rows: new_rows,
            });
        }

        Err(Error::Internal("Unexpected result set".into()))
    }
}

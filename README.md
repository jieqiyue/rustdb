# sqldb-rs
Rust 实现的 SQL 数据库系统
## 系统架构图
![SQL数据库架构图.jpg](src/doc/SQL%E6%95%B0%E6%8D%AE%E5%BA%93%E6%9E%B6%E6%9E%84%E5%9B%BE.jpg)
![SQL执行流程.png](src/doc/SQL%E6%89%A7%E8%A1%8C%E6%B5%81%E7%A8%8B.png)
## 支持的 SQL 语法

### 1. Create Table
```sql
CREATE TABLE table_name (
    [ column_name data_type [ column_constraint [...] ] ]
    [, ... ]
   );

   where data_type is:
    - BOOLEAN(BOOL): true | false
    - FLOAT(DOUBLE)
    - INTEGER(INT)
    - STRING(TEXT, VARCHAR)

   where column_constraint is:
   [ NOT NULL | NULL | DEFAULT expr ]
```

### 2. Insert Into
```sql
INSERT INTO table_name
[ ( column_name [, ...] ) ]
values ( expr [, ...] );
```

### 3. Select
```sql
SELECT [* | col_name | function [ [ AS ] output_name [, ...] ]]
FROM from_item
[GROUP BY col_name]
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]
```

where `function` is:
* count(col_name)
* min(col_name)
* max(col_name)
* sum(col_name)
* avg(col_name)

where `from_item` is:
* table_name
* table_name `join_type` table_name [`ON` predicate]

where `join_type` is:
* cross join
* join
* left join
* right join

where `on predicate` is:
* column_name = column_name

### 4. Update
```sql
UPDATE table_name
SET column_name = expr [, ...]
[WHERE condition];
```
where condition is: `column_name = expr`

### 5. Delete
```sql
DELETE FROM table_name
[WHERE condition];
```
where condition is: `column_name = expr`

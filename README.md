# sqldb-rs
Rust 实现的 SQL 数据库系统
## 系统架构图

![SQL数据库架构图.jpg](src/doc/SQL%E6%95%B0%E6%8D%AE%E5%BA%93%E6%9E%B6%E6%9E%84%E5%9B%BE.jpg)
![SQL执行流程.png](src/doc/SQL%E6%89%A7%E8%A1%8C%E6%B5%81%E7%A8%8B.png)

## 部分关键特性解析
### 1. MVCC层的冲突检测
实现原理：
对于每一条表中的数据，有一个版本的概念。不同的事务只要更新（插入）了这条数据的话，并不会在原来的数据位置（磁盘位置）上面进行修改，而是新增一条数据。
每一个事务都有一个事务号，通过事务号的大小能够区分出两个事务之间的前后顺序（仅仅通过事务号的大小能够进行判断）。
在写入某一行数据的时候，在这个行中加上进行这个操作的事务的事务号。然后在某一个事务要进行更新（插入）的时候，将这行数据的最新的版本拿到，并且拿到这个事务号，
与本事务进行比较，如果发现这个已经存在的事务号对于自身来说是 **不可见** 的话，那就代表有冲突发生，即报出写入冲突检测的错误，让用户重试。

这个不可见包含了两层意思：
 - 最新的事务号在活跃事务号中（有另外一个事务还没有commit，并且也在修改这条数据）
 - 如果不在活跃事务号中的话，则要判断事务号是否比自己大

为什么要判断是否比自己大呢？因为更新操作要使用当前读。

### 2. MVCC层的一致性和持久性
实现原理：
每一个事务在进行操作之前都需要进行一个开启事务的操作。然后再进行更新（插入）等的操作。在进行这些操作的时候，会临时的往存储引擎里面记录当前事务操作了哪些行。
然后再最后，如果要进行回滚的话，就从存储引擎中拿到本事务进行的所有操作，然后进行反向的操作。即可进行回滚。
并且由于底层的存储引擎使用的是kv存储。这个记录的过程是通过一种特殊的key（事务号+本次操作的真实key），存入到存储引擎当中，然后再要拿到本次事务的所有操作的时候
使用这个事务号构建出这个特殊key，然后进行前缀扫描，就可以非常快速的拿到所有的操作。

### 3. KV存储引擎
实现原理：
使用了Bitcask存储模型。这是一种类似于LSM Tree的存储模型，基于了磁盘的顺序写速度远远大于随机写的速度，甚至超过了对内存的随机读写的速度的这个事实，诞生的一种
存储模型。能够非常快速的进行写入操作。对于读取方面，对于每一条在磁盘上面的数据构建了一个内存索引。然后要读取某一条数据的时候，先读取这个索引，然后能够得到这条
数据在磁盘上面的位置，然后去读取相应的文件的数据。
并且kv存储引擎比较好拓展。

### 4. 火山模型
在不同的执行节点中（Scan节点，Update，Order...节点）会创建不同的执行器去进行数据的操作。对于像条件查询，条件查询+Order By排序，条件删除等的语句，
因为附带了条件，所以会有前置执行的节点和后置执行的节点。比如说条件查询，会先查询出所有数据，再根据条件过滤，再根据Order By进行排序。最终得到结果。那么，
这几步就是一个递归的过程，也被称为火山模型。上层需要的数据不断的去拉取依赖的底层数据，当拿到底层的数据之后，再进行本层的操作。本层操作完毕之后，再返回给
上层（如果有上层）。

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

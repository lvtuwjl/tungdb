# tungdb
a simple embedded database

## 数据组织

```
LSM Tree

内存表 Memory Table
  Immutable Memory Table 不可变内存表,用于同步内存数据到SSTable
磁盘表 Sorted String Table (SSTable)
SSTable level分层
内存数据同步到磁盘后 当前WAL应该删除 重新创建一个空文件
后台合并 compact

crash recovey 崩溃恢复 使用WAL实现
WAL Write Ahead Log 预写日志 顺序写
```


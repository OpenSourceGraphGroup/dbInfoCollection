# dbInfoCollection
开源社区课程项目 - 数据库和负载信息采集⼯工具

## 编译
```
mvn install
```

## 运行
```
java -jar target/dbInfoCollection-1.0-SNAPSHOT-jar-with-dependencies.jar [d/l] [ip] [port] [schema] [user] [password] [sql_path]
```
例如获取数据库数据表信息：
```
java -jar target/dbInfoCollection-1.0-SNAPSHOT-jar-with-dependencies.jar d 127.0.0.1 3306 tpch root root
```
输出文件位于 out/tpch.databaseInfo

获取数据库负载信息
```
java -jar target/dbInfoCollection-1.0-SNAPSHOT-jar-with-dependencies.jar l 127.0.0.1 3306 tpch root root sql/1.sql
```
输出文件位于 out/1.sql.constraint 和 out/1.sql.refinedConstraint

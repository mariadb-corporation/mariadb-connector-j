# Benchmark

How to run : 
```script
mvn clean package -P bench -Dmaven.test.skip

# run all benchmarks
java -Duser.country=US -Duser.language=en -jar target/benchmarks.jar

# run a specific benchmark
java -Duser.country=US -Duser.language=en -jar target/benchmarks.jar "Select_1_user"
```

Configuration by system properties :
* TEST_HOST: localhost
* TEST_PORT: 3306
* TEST_USERNAME: root
* TEST_PASSWORD: ""
* TEST_DATABASE: "testj"

example: 
```script
mvn clean package -P bench -Dmaven.test.skip
java -DTEST_PORT=3307 -Duser.country=US -Duser.language=en -jar target/benchmarks.jar "Select_1_user"
```

### Test plan description

**Drivers used for test execution:**
* singlestore (1.2.1)
* mariadb (3.3.0)
* mysql (8.0.33) 

**Connection string parameters used in tests:**

|Connection string type|useServerPrepStmts|cachePrepStmts|rewriteBatchedStatements|prepStmtCacheSize|disablePipeline|
---|---|---|---|---|---
|**text**|false|false|-|-|-|
|**binary**|true|true|-|-|-|
|**rewrite**|false|false|true|-|-|
|**binaryNoCache**|true|false|-|0|-|
|**binaryNoPipeline**|true|false|-|0|true|

**Tests details per connection string type:**

|Test name|Description|text|binary|rewrite|binaryNoCache|binaryNoPipeline
---|---|---|---|---|---|---
|**Select_1**|Simple test with select number query|Uses Statement executeQuery|-|-|-|-|
|**Insert_batch**|Insert 100 batches test|-|_**singlestore**_ uses executeBatchPipeline from ServerPreparedStatement, _**mariadb**_ uses executeBatchStandard from ServerPreparedStatement (because STMT_BULK_OPERATIONS is disabled)|_**singlestore**_ uses executeWithRewrite from ClientPreparedStatement, _**mariadb**_ uses executeBatchPipeline from ClientPreparedStatement (because STMT_BULK_OPERATIONS is disabled and rewrite is deprecated - used bulk insert instead)|-|_**singlestore**_ uses executeBatchPipeline(but uses batch standard in fact as pipeline is disabled) from ServerPreparedStatement, _**mariadb**_ uses executeBatchStandard from ServerPreparedStatement (because STMT_BULK_OPERATIONS is disabled)|
|**Select_100_cols**|Select * and than get 100 columns test|Uses executeQuery from ClientPreparedStatement|Uses executeQuery from ServerPreparedStatement|-|Uses executeQuery from ServerPreparedStatement(COM_STMT_PREPARE is sent every execution)|-|
|**Select_1000_params**|Select 1000 columns test|Uses executeQuery from ClientPreparedStatement|Uses executeQuery from ServerPreparedStatement|-|-|-|
|**Select_10000_Rows**|Select 10000 rows test|Uses executeQuery from ClientPreparedStatement|Uses executeQuery from ServerPreparedStatement|-|-|-|
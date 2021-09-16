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


<p style="text-align: center;">
  <a href="https://mariadb.com/">
    <img alt="mariadb logo" src="https://mariadb.com/kb/static/images/logo-2018-black.png">
  </a>
</p>

# Benchmark

How to run :

```script
mvn clean package -P bench -DskipTests

# run all benchmarks
nohup java -Duser.country=US -Duser.language=en -jar target/benchmarks.jar > log.txt

# run a specific benchmark
java -Duser.country=US -Duser.language=en -jar target/benchmarks.jar "Select_100_cols"
```

Configuration by system properties :

* TEST_HOST: Hostname. default "localhost"
* TEST_PORT: port. default 3306
* TEST_USERNAME: user name. default "root"
* TEST_PASSWORD: password. default ""
* TEST_DATABASE: database. default "testj"
* TEST_OTHER: permit adding connection string options. default ""

example:

```script
mvn clean package -P bench -Dmaven.test.skip
java -DTEST_PORT=3307 -Duser.country=US -Duser.language=en -jar target/benchmarks.jar "Select_100_cols"
```


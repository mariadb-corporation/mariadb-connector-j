package org.mariadb.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConnectionPoolTest extends BaseTest {

    /**
     * Tables initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        for (int i = 0; i < 50; i++) {
            createTable("test_pool_batch" + i, "id int not null primary key auto_increment, test varchar(10)");
        }
    }


    @Test
    public void testBasicPool() throws SQLException {

        final HikariDataSource ds = new HikariDataSource();
        ds.setMaximumPoolSize(20);
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setJdbcUrl(connU);
        ds.addDataSourceProperty("user", username);
        if (password != null ) ds.addDataSourceProperty("password", password);
        ds.setAutoCommit(false);
        validateDataSource(ds);

    }

    @Test
    public void testPoolHikariCpWithConfig() throws SQLException {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(connU);
        config.setUsername(username);
        if (password != null ) config.setPassword(password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        try (HikariDataSource ds = new HikariDataSource(config)) {
            validateDataSource(ds);
        }

    }

    @Test
    public void testPoolEffectiveness() throws Exception {
        Assume.assumeFalse(sharedIsRewrite());
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(connU);
        config.setUsername(username);
        if (password != null ) config.addDataSourceProperty("password", password);

        try (HikariDataSource ds = new HikariDataSource(config)) {
            ds.setAutoCommit(true);

            //force pool loading
            forcePoolLoading(ds);

            long monoConnectionExecutionTime = insert500WithOneConnection(ds);


            for (int j = 0; j < 50; j++) {
                sharedConnection.createStatement().execute("TRUNCATE test_pool_batch" + j);
            }

            long poolExecutionTime = insert500WithPool(ds);
            System.out.println("mono connection execution time : " + monoConnectionExecutionTime);
            System.out.println("pool execution time : " + poolExecutionTime);
            Assert.assertTrue(monoConnectionExecutionTime > poolExecutionTime);
        }
    }


    private void forcePoolLoading(DataSource ds) {
        ExecutorService exec = Executors.newFixedThreadPool(50);
        //check blacklist shared

        //force pool loading
        for (int j = 0; j < 100; j++) {
            exec.execute(new ForceLoadPoolThread(ds));
        }
        exec.shutdown();
        try {
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            //eat exception
        }
        exec = Executors.newFixedThreadPool(50);

    }


    private void validateDataSource(DataSource ds) throws SQLException {
        try (Connection connection = ds.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT 1")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(1, rs.getInt(1));
                }
            }
        }
    }

    private long insert500WithOneConnection(DataSource ds) throws SQLException {
        long startTime = System.currentTimeMillis();
        try (Connection connection = ds.getConnection()) {
            for (int j = 0; j < 50; j++) {
                try {
                    PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test_pool_batch" + j + "(test) VALUES (?)");
                    for (int i = 1; i < 10; i++) {
                        preparedStatement.setString(1, i + "");
                        preparedStatement.addBatch();
                    }
                    preparedStatement.executeBatch();
                } catch (SQLException e) {
                    Assert.fail("ERROR insert : " + e.getMessage());
                }
            }
        }
        return System.currentTimeMillis() - startTime;
    }


    private long insert500WithPool(DataSource ds) throws SQLException {
        ExecutorService exec = Executors.newFixedThreadPool(50);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 50; i++) {
            exec.execute(new InsertThread(i, 10, ds));
        }
        exec.shutdown();
        try {
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            //eat exception
        }

        return System.currentTimeMillis() - startTime;
    }

    private class ForceLoadPoolThread implements Runnable {
        private DataSource dataSource;

        public ForceLoadPoolThread(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public void run() {
            try (Connection connection = dataSource.getConnection()) {
                connection.createStatement().execute("SELECT 1");
            } catch (SQLException e) {
                Assert.fail("ERROR insert : " + e.getMessage());
            }
        }

    }

    private class InsertThread implements Runnable {
        private DataSource dataSource;
        private int insertNumber;
        private int tableNumber;

        public InsertThread(int tableNumber, int insertNumber, DataSource dataSource) {
            this.insertNumber = insertNumber;
            this.tableNumber = tableNumber;
            this.dataSource = dataSource;
        }

        public void run() {

            try (Connection connection = dataSource.getConnection()) {
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test_pool_batch"
                        + tableNumber + "(test) VALUES (?)");
                for (int i = 1; i < insertNumber; i++) {
                    preparedStatement.setString(1, i + "");
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                Assert.fail("ERROR insert : " + e.getMessage());
            }
        }
    }
}

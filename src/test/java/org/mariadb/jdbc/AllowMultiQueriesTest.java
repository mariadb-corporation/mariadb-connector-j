package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AllowMultiQueriesTest extends BaseTest {
    /**
     * Tables initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("AllowMultiQueriesTest", "id int not null primary key auto_increment, test varchar(10)");
        createTable("AllowMultiQueriesTest2", "id int not null primary key auto_increment, test varchar(10)");
        if (testSingleHost) {
            try (Statement stmt = sharedConnection.createStatement()) {
                stmt.execute("INSERT INTO AllowMultiQueriesTest(test) VALUES ('a'), ('b')");
            }
        }
    }



    @Test
    public void allowMultiQueriesSingleTest() throws SQLException {
        try (Connection connection = setConnection("&allowMultiQueries=true")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1; SELECT 2; SELECT 3;");
                int counter = 1;
                do {
                    ResultSet resultSet = statement.getResultSet();
                    Assert.assertEquals(-1, statement.getUpdateCount());
                    Assert.assertTrue(resultSet.next());
                    Assert.assertEquals(counter++, resultSet.getInt(1));
                } while (statement.getMoreResults());
                Assert.assertEquals(4, counter);
            }
        }
    }

    @Test
    public void allowMultiQueriesFetchTest() throws SQLException {
        try (Connection connection = setConnection("&allowMultiQueries=true")) {
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(1);
                statement.execute("SELECT * from AllowMultiQueriesTest;SELECT * from AllowMultiQueriesTest;");
                do {
                    ResultSet resultSet = statement.getResultSet();
                    Assert.assertEquals(-1, statement.getUpdateCount());
                    Assert.assertTrue(resultSet.next());
                    Assert.assertEquals("a", resultSet.getString(2));
                } while (statement.getMoreResults());
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void allowMultiQueriesFetchKeepTest() throws SQLException {
        try (Connection connection = setConnection("&allowMultiQueries=true")) {
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(1);
                statement.execute("SELECT * from AllowMultiQueriesTest;SELECT 3;");
                ResultSet rs1 = statement.getResultSet();
                Assert.assertTrue(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
                Assert.assertTrue(rs1.next());
                Assert.assertEquals("a", rs1.getString(2));

                ResultSet rs = statement.getResultSet();
                Assert.assertTrue(rs.next());
                Assert.assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    public void allowMultiQueriesFetchCloseTest() throws SQLException {
        try (Connection connection = setConnection("&allowMultiQueries=true")) {
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(1);
                statement.execute("SELECT * from AllowMultiQueriesTest;SELECT * from AllowMultiQueriesTest;SELECT 3;");
                ResultSet rs1 = statement.getResultSet();
                Assert.assertTrue(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
                try {
                    rs1.next();
                    Assert.fail("Must have thrown exception, since closed");
                } catch (SQLException sqle) {
                    Assert.assertTrue(sqle.getMessage().contains("Operation not permit on a closed resultSet"));
                }

                rs1 = statement.getResultSet();
                Assert.assertTrue(statement.getMoreResults(Statement.KEEP_CURRENT_RESULT));
                Assert.assertTrue(rs1.next());
                Assert.assertEquals("a", rs1.getString(2));

                ResultSet rs = statement.getResultSet();
                Assert.assertTrue(rs.next());
                Assert.assertEquals(3, rs.getInt(1));
            }
        }
    }


    @Test
    public void allowMultiQueriesFetchInsertSelectTest() throws SQLException {
        try (Connection connection = setConnection("&allowMultiQueries=true")) {
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(1);
                statement.execute("INSERT INTO AllowMultiQueriesTest2(test) VALUES ('a'), ('b');SELECT * from AllowMultiQueriesTest;SELECT 3;");
                statement.close();
            }
        }
    }

}

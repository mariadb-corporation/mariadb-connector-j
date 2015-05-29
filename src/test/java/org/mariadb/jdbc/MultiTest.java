package org.mariadb.jdbc;

import org.junit.*;

import java.sql.*;
import java.util.Properties;

import junit.framework.Assert;

import static org.junit.Assert.*;


public class MultiTest extends BaseTest {
	private static Connection connection;

    public MultiTest() throws SQLException {

    }

    @BeforeClass
    public static void beforeClassMultiTest() throws SQLException {
        BaseTest baseTest = new BaseTest();
        baseTest.setConnection("&allowMultiQueries=true");
        connection = baseTest.connection;
        Statement st = connection.createStatement();
        st.executeUpdate("drop table if exists t1");
        st.executeUpdate("drop table if exists t2");
        st.executeUpdate("drop table if exists t3");
        st.executeUpdate("drop table if exists t4");
        st.executeUpdate("drop table if exists t5");
        st.executeUpdate("drop table if exists reWriteDuplicateTestTable");
        st.executeUpdate("create table t1(id int, test varchar(100))");
        st.executeUpdate("create table t2(id int, test varchar(100))");
        st.executeUpdate("create table t3(message text)");
        st.executeUpdate("create table t4(id int, test varchar(100), PRIMARY KEY (`id`))");
        st.executeUpdate("create table t5(id int, test varchar(100))");
        st.executeUpdate("create table reWriteDuplicateTestTable(id int, name varchar(100), PRIMARY KEY (`id`))");
        st.execute("insert into t1 values(1,'a'),(2,'a')");
        st.execute("insert into t2 values(1,'a'),(2,'a')");
        st.execute("insert into t3 values('hello')");
        st.execute("insert into t5 values(1,'a'),(2,'a'),(2,'b')");
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try {
            Statement st = connection.createStatement();
            st.executeUpdate("drop table if exists t1");
            st.executeUpdate("drop table if exists t2");
            st.executeUpdate("drop table if exists t3");
            st.executeUpdate("drop table if exists reWriteDuplicateTestTable");
         } catch (Exception e) {
            // eat
        } finally {
            try  {
                connection.close();
            } catch (Exception e) {

            }
        }
    }


    @Test
    public void basicTest() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from t1;select * from t2;");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertTrue(count > 0);
        assertTrue(statement.getMoreResults());
        rs = statement.getResultSet();
        count = 0;
        while (rs.next()) {
            count++;
        }
        assertTrue(count > 0);
        assertFalse(statement.getMoreResults());
    }

    @Test
    public void updateTest() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("update t5 set test='a " + System.currentTimeMillis() + "' where id = 2;select * from t2;");
        int updateNb = statement.getUpdateCount();
        log.fine("statement.getUpdateCount() " + updateNb);
        assertTrue(updateNb == 2);
        assertTrue(statement.getMoreResults());
        ResultSet rs = statement.getResultSet();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertTrue(count > 0);
        assertFalse(statement.getMoreResults());
    }

    @Test
    public void updateTest2() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("select * from t2;update t5 set test='a " + System.currentTimeMillis() + "' where id = 2;");
        ResultSet rs = statement.getResultSet();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertTrue(count == 2);
        statement.getMoreResults();

        int updateNb = statement.getUpdateCount();
        log.fine("statement.getUpdateCount() " + updateNb);
        assertEquals(2, updateNb);
    }

    @Test
    public void selectTest() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("select * from t2;select * from t1;");
        ResultSet rs = statement.getResultSet();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertTrue(count > 0);
        rs = statement.executeQuery("select * from t1");
        count = 0;
        while (rs.next()) {
            count++;
        }
        assertTrue(count > 0);
    }

    @Test
    public void setMaxRowsMulti() throws Exception {
        Statement st = connection.createStatement();
        assertEquals(0, st.getMaxRows());

        st.setMaxRows(1);
        assertEquals(1, st.getMaxRows());

        /* Check 3 rows are returned if maxRows is limited to 3, in every result set in batch */

       /* Check first result set for at most 3 rows*/
        ResultSet rs = st.executeQuery("select 1 union select 2;select 1 union select 2");
        int cnt = 0;

        while (rs.next()) {
            cnt++;
        }
        rs.close();
        assertEquals(1, cnt);

       /* Check second result set for at most 3 rows*/
        assertTrue(st.getMoreResults());
        rs = st.getResultSet();
        cnt = 0;
        while (rs.next()) {
            cnt++;
        }
        rs.close();
        assertEquals(1, cnt);
    }

    /**
     * CONJ-99: rewriteBatchedStatements parameter.
     *
     * @throws SQLException
     */
    @Test
    public void rewriteBatchedStatementsDisabledInsertionTest() throws SQLException {
        verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean.FALSE, 3000);
    }

    @Test
    public void rewriteBatchedStatementsEnabledInsertionTest() throws SQLException {
        //On batch mode, single insert query will be sent to MariaDB server.
        verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean.TRUE, 1);
    }

    private void verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean rewriteBatchedStatements, int totalInsertCommands) throws SQLException {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", rewriteBatchedStatements.toString());
        Connection tmpConnection = openNewConnection(connURI, props);
        try {
            verifyInsertCount(tmpConnection, 0);
            int cycles = 3000;
            tmpConnection.createStatement().execute("TRUNCATE t1");
            Statement statement = tmpConnection.createStatement();
            for (int i = 0; i < cycles; i++) {
                statement.addBatch("INSERT INTO t1 VALUES (" + i + ", 'testValue" + i + "')");
            }
            int[] updateCounts = statement.executeBatch();
            assertEquals(cycles, updateCounts.length);
            int totalUpdates = 0;
            for (int count = 0; count < updateCounts.length; count++) {
                assertEquals(1, updateCounts[count]);
                totalUpdates += updateCounts[count];
            }
            assertEquals(cycles, totalUpdates);
            verifyInsertCount(tmpConnection, totalInsertCommands);
        } finally {
            tmpConnection.close();
        }
    }

    private void verifyInsertCount(Connection tmpConnection, int insertCount) throws SQLException {
        assertEquals(insertCount, retrieveSessionVariableFromServer(tmpConnection, "Com_insert"));
    }

    private int retrieveSessionVariableFromServer(Connection tmpConnection, String variable) throws SQLException {
        Statement statement = tmpConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW STATUS LIKE '" + variable + "'");
        try {
            if (resultSet.next()) {
                return resultSet.getInt(2);
            }
        } finally {
            resultSet.close();
        }
        throw new RuntimeException("Unable to retrieve, variable value from Server " + variable);
    }

    /**
     * CONJ-142: Using a semicolon in a string with "rewriteBatchedStatements=true" fails
     *
     * @throws SQLException
     */
    @Test
    public void rewriteBatchedStatementsSemicolon() throws SQLException {
        // set the rewrite batch statements parameter
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        Connection tmpConnection = openNewConnection(connURI, props);
        try {
            tmpConnection.createStatement().execute("TRUNCATE t3");

            PreparedStatement sqlInsert = tmpConnection.prepareStatement("INSERT INTO t3 (message) VALUES (?)");
            sqlInsert.setString(1, "aa");
            sqlInsert.addBatch();
            sqlInsert.setString(1, "b;b");
            sqlInsert.addBatch();
            sqlInsert.setString(1, ";ccccccc");
            sqlInsert.addBatch();
            sqlInsert.setString(1, "ddddddddddddddd;");
            sqlInsert.addBatch();
            sqlInsert.setString(1, ";eeeeeee;;eeeeeeeeee;eeeeeeeeee;");
            sqlInsert.addBatch();
            int[] updateCounts = sqlInsert.executeBatch();

            // rewrite should be ok, so the above should be executed in 1 command updating 5 rows
            Assert.assertEquals(5, updateCounts.length);
            for (int i = 0; i < updateCounts.length; i++) {
                Assert.assertEquals(1, updateCounts[i]);
            }

            tmpConnection.commit();
            verifyInsertCount(tmpConnection, 1);
            // Test for multiple statements which isn't allowed. rewrite shouldn't work
            sqlInsert = tmpConnection.prepareStatement("INSERT INTO t3 (message) VALUES (?); INSERT INTO t3 (message) VALUES ('multiple')");
            sqlInsert.setString(1, "aa");
            sqlInsert.addBatch();
            sqlInsert.setString(1, "b;b");
            sqlInsert.addBatch();
            updateCounts = sqlInsert.executeBatch();

            // rewrite should NOT be possible. Therefore there should be 2 commands updating 1 row each.
            Assert.assertEquals(2, updateCounts.length);
            Assert.assertEquals(1, updateCounts[0]);
            Assert.assertEquals(1, updateCounts[1]);
            verifyInsertCount(tmpConnection, 5);
            tmpConnection.commit();
        } finally {
            tmpConnection.close();
        }
    }

    private PreparedStatement prepareStatementBatch(Connection tmpConnection, int size) throws SQLException {
        PreparedStatement preparedStatement = tmpConnection.prepareStatement("INSERT INTO t1 VALUES (?, ?)");
        for (int i = 0; i < size; i++) {
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, "testValue" + i);
            preparedStatement.addBatch();

            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, "testSecn" + i);
            preparedStatement.addBatch();
        }
        return preparedStatement;
    }

    /**
     * CONJ-99: rewriteBatchedStatements parameter.
     *
     * @throws SQLException
     */
    @Test
    public void rewriteBatchedStatementsUpdateTest() throws SQLException {
        // set the rewrite batch statements parameter
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        Connection tmpConnection = openNewConnection(connURI, props);
        try {
            tmpConnection.setClientInfo(props);
            verifyUpdateCount(tmpConnection, 0);
            tmpConnection.createStatement().execute("TRUNCATE t1");
            int cycles = 1000;
            prepareStatementBatch(tmpConnection, cycles).executeBatch();  // populate the table
            PreparedStatement preparedStatement = tmpConnection.prepareStatement("UPDATE t1 SET test = ? WHERE id = ?");
            for (int i = 0; i < cycles; i++) {
                preparedStatement.setString(1, "updated testValue" + i);
                preparedStatement.setInt(2, i);
                preparedStatement.addBatch();
            }
            int[] updateCounts = preparedStatement.executeBatch();
            assertEquals(cycles, updateCounts.length);
            int totalUpdates = 0;
            for (int count = 0; count < updateCounts.length; count++) {
                assertEquals(2, updateCounts[count]); //2 rows updated by update.
                totalUpdates += updateCounts[count];
            }

            verifyUpdateCount(tmpConnection, cycles); //1000 update commande launched
            assertEquals(cycles * 2, totalUpdates); // 2000 rows updates
        } finally {
            tmpConnection.close();
        }
    }

    /**
     * CONJ-152: rewriteBatchedStatements and multiple executeBatch check
     * @throws SQLException
     */
    @Test
    public void testMultipleExecuteBatch() throws SQLException {
        // set the rewrite batch statements parameter
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        Connection tmpConnection = openNewConnection(connURI, props);
        try {
            tmpConnection.setClientInfo(props);
            verifyUpdateCount(tmpConnection, 0);
            tmpConnection.createStatement().execute("TRUNCATE t1");
            tmpConnection.createStatement().execute("insert into t1 values(1,'a'),(2,'a')");

            PreparedStatement preparedStatement = tmpConnection.prepareStatement("UPDATE t1 SET test = ? WHERE id = ?");
            preparedStatement.setString(1, "executebatch");
            preparedStatement.setInt(2, 1);
            preparedStatement.addBatch();
            preparedStatement.setString(1, "executebatch2");
            preparedStatement.setInt(2, 3);
            preparedStatement.addBatch();

            preparedStatement.executeBatch();
            int[] updateCounts = preparedStatement.executeBatch();
            assertEquals(0, updateCounts.length);

            preparedStatement.setString(1, "executebatch3");
            preparedStatement.setInt(2, 1);
            preparedStatement.addBatch();
            updateCounts = preparedStatement.executeBatch();
            assertEquals(1, updateCounts.length);
        } finally {
            tmpConnection.close();
        }
    }

    @Test
    public void rewriteBatchedStatementsInsertWithDuplicateRecordsTest() throws SQLException {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        Connection tmpConnection = openNewConnection(connURI, props);
        try {
            verifyInsertCount(tmpConnection, 0);
            tmpConnection.createStatement().execute("TRUNCATE reWriteDuplicateTestTable");
            Statement statement = tmpConnection.createStatement();
            for (int i = 0; i < 100; i++) {
                int newId = i % 20; //to create duplicate id's
                String roleTxt = "VAMPIRE" + newId;
                statement.addBatch("INSERT IGNORE  INTO reWriteDuplicateTestTable VALUES (" + newId + ", '" + roleTxt + "')");
            }
            int[] updateCounts = statement.executeBatch();
            assertEquals(100, updateCounts.length);

            for (int i = 0; i < updateCounts.length; i++) {
                assertEquals(MySQLStatement.SUCCESS_NO_INFO, updateCounts[i]);
            }
            verifyInsertCount(tmpConnection, 1);
            verifyUpdateCount(tmpConnection, 0);
        } finally {
            tmpConnection.close();
        }
    }

    @Test
    public void updateCountTest() throws SQLException {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        Connection tmpConnection = openNewConnection(connURI, props);
        try {
            PreparedStatement sqlInsert = tmpConnection.prepareStatement("INSERT IGNORE INTO t4 (id,test) VALUES (?,?)");
            sqlInsert.setInt(1, 1);
            sqlInsert.setString(2, "value1");
            sqlInsert.addBatch();
            sqlInsert.setInt(1, 1);
            sqlInsert.setString(2, "valuenull");
            sqlInsert.addBatch();
            sqlInsert.setInt(1, 2);
            sqlInsert.setString(2, "value2");
            sqlInsert.addBatch();
            sqlInsert.setInt(1, 3);
            sqlInsert.setString(2, "value2");
            sqlInsert.addBatch();
            int[] insertCounts = sqlInsert.executeBatch();

            //Insert in prepare statement, cannot know the number og each one
            Assert.assertEquals(4, insertCounts.length);
            Assert.assertEquals(-2, insertCounts[0]);
            Assert.assertEquals(-2, insertCounts[1]);
            Assert.assertEquals(-2, insertCounts[2]);
            Assert.assertEquals(-2, insertCounts[3]);


            PreparedStatement sqlUpdate = tmpConnection.prepareStatement("UPDATE t4 SET test = ? WHERE test = ?");
            sqlUpdate.setString(1, "value1 - updated");
            sqlUpdate.setString(2, "value1");
            sqlUpdate.addBatch();
            sqlUpdate.setString(1, "value3 - updated");
            sqlUpdate.setString(2, "value3");
            sqlUpdate.addBatch();
            sqlUpdate.setString(1, "value2 - updated");
            sqlUpdate.setString(2, "value2");
            sqlUpdate.addBatch();

            int[] updateCounts = sqlUpdate.executeBatch();
            Assert.assertEquals(3, updateCounts.length);
            Assert.assertEquals(1, updateCounts[0]);
            Assert.assertEquals(0, updateCounts[1]);
            Assert.assertEquals(2, updateCounts[2]);
        } finally {
            tmpConnection.close();
        }
    }


    private void verifyUpdateCount(Connection tmpConnection, int updateCount) throws SQLException {
        assertEquals(updateCount, retrieveSessionVariableFromServer(tmpConnection, "Com_update"));
    }

}

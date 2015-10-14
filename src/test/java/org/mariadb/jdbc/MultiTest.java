package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;


public class MultiTest extends BaseTest {

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("MultiTestt1", "id int, test varchar(100)");
        createTable("MultiTestt2", "id int, test varchar(100)");
        createTable("MultiTestt3", "message text");
        createTable("MultiTestt4", "id int, test varchar(100), PRIMARY KEY (`id`)");
        createTable("MultiTestt5", "id int, test varchar(100)");
        createTable("MultiTestt6", "id int, test varchar(100)");
        createTable("MultiTestt7", "id int, test varchar(100)");
        createTable("MultiTestt8", "id int, test varchar(100)");
        createTable("MultiTestreWriteDuplicateTestTable", "id int, name varchar(100), PRIMARY KEY (`id`)");
        createTable("MultiTesttselect1", "LAST_UPDATE_DATETIME TIMESTAMP , nn int");
        createTable("MultiTesttselect2", "nn int");
        createTable("MultiTesttselect3", "LAST_UPDATE_DATETIME TIMESTAMP , nn int");
        createTable("MultiTesttselect4", "nn int");
        createTable("MultiTestt3_dupp", "col1 int, pkey int NOT NULL, col2 int, col3 int, col4 int, PRIMARY KEY (`pkey`)");
        createTable("MultiTesttest_table", "col1 VARCHAR(32), col2 VARCHAR(32), col3 VARCHAR(32), col4 VARCHAR(32), col5 VARCHAR(32)");
        createTable("MultiTesttest_table2", "col1 VARCHAR(32), col2 VARCHAR(32), col3 VARCHAR(32), col4 VARCHAR(32), col5 VARCHAR(32)");
        Statement st = sharedConnection.createStatement();
        st.execute("insert into MultiTestt1 values(1,'a'),(2,'a')");
        st.execute("insert into MultiTestt2 values(1,'a'),(2,'a')");
        st.execute("insert into MultiTestt5 values(1,'a'),(2,'a'),(2,'b')");

    }

    @Test
    public void rewriteSelectQuery() throws Throwable {
        Statement st = sharedConnection.createStatement();
        st.execute("INSERT INTO MultiTesttselect2 VALUES (1)");
        PreparedStatement ps = sharedConnection.prepareStatement("/*CLIENT*/ insert into MultiTesttselect1 (LAST_UPDATE_DATETIME, nn) select ?, nn from MultiTesttselect2");
        ps.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
        ps.executeUpdate();

        ResultSet rs = st.executeQuery("SELECT * FROM MultiTesttselect1");
        rs.next();
        Assert.assertEquals(rs.getInt(2), 1);
    }

    @Test
    public void rewriteSelectQueryServerPrepared() throws Throwable {
        Statement st = sharedConnection.createStatement();
        st.execute("INSERT INTO MultiTesttselect4 VALUES (1)");
        PreparedStatement ps = sharedConnection.prepareStatement("insert into MultiTesttselect3 (LAST_UPDATE_DATETIME, nn) select ?, nn from MultiTesttselect4");
        ps.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
        ps.executeUpdate();

        ResultSet rs = st.executeQuery("SELECT * FROM MultiTesttselect3");
        rs.next();
        Assert.assertEquals(rs.getInt(2), 1);
    }

    @Test
    public void basicTest() throws SQLException {
        log.debug("basicTest begin");
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select * from MultiTestt1;select * from MultiTestt2;");
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
            log.debug("basicTest end");
        } finally {
            connection.close();
        }
    }

    @Test
    public void updateTest() throws SQLException {
        log.debug("updateTest begin");
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            Statement statement = connection.createStatement();
            statement.execute("update MultiTestt5 set test='a " + System.currentTimeMillis() + "' where id = 2;select * from MultiTestt2;");
            int updateNb = statement.getUpdateCount();
            log.debug("statement.getUpdateCount() " + updateNb);
            assertTrue(updateNb == 2);
            assertTrue(statement.getMoreResults());
            ResultSet rs = statement.getResultSet();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue(count > 0);
            assertFalse(statement.getMoreResults());
            log.debug("updateTest end");
        } finally {
            connection.close();
        }
    }

    @Test
    public void updateTest2() throws SQLException {
        log.debug("updateTest2 begin");
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            Statement statement = connection.createStatement();
            statement.execute("select * from MultiTestt2;update MultiTestt5 set test='a " + System.currentTimeMillis() + "' where id = 2;");
            ResultSet rs = statement.getResultSet();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue(count == 2);
            statement.getMoreResults();

            int updateNb = statement.getUpdateCount();
            log.debug("statement.getUpdateCount() " + updateNb);
            assertEquals(2, updateNb);
            log.debug("updateTest2 end");
        } finally {
            connection.close();
        }
    }

    @Test
    public void selectTest() throws SQLException {
        log.debug("selectTest begin");
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            Statement statement = connection.createStatement();
            statement.execute("select * from MultiTestt2;select * from MultiTestt1;");
            ResultSet rs = statement.getResultSet();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue(count > 0);
            rs = statement.executeQuery("select * from MultiTestt1");
            count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue(count > 0);
            log.debug("selectTest end");
        } finally {
            connection.close();
        }
    }

    @Test
    public void setMaxRowsMulti() throws Exception {
        log.debug("setMaxRowsMulti begin");
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
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
            log.debug("setMaxRowsMulti end");
        } finally {
            connection.close();
        }
    }

    /**
     * CONJ-99: rewriteBatchedStatements parameter.
     *
     * @throws SQLException
     */
    @Test
    public void rewriteBatchedStatementsDisabledInsertionTest() throws SQLException {
        log.debug("rewriteBatchedStatementsDisabledInsertionTest begin");
        verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean.FALSE, 3000);
        log.debug("rewriteBatchedStatementsDisabledInsertionTest end");
    }

    @Test
    public void rewriteBatchedStatementsEnabledInsertionTest() throws SQLException {
        log.debug("rewriteBatchedStatementsEnabledInsertionTest begin");
        //On batch mode, single insert query will be sent to MariaDB server.
        verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean.TRUE, 1);
        log.debug("rewriteBatchedStatementsEnabledInsertionTest end");
    }

    private void verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean rewriteBatchedStatements, int totalInsertCommands) throws SQLException {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", rewriteBatchedStatements.toString());
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connURI, props);
            verifyInsertCount(tmpConnection, 0);
            int cycles = 3000;
            Statement statement = tmpConnection.createStatement();
            for (int i = 0; i < cycles; i++) {
                statement.addBatch("INSERT INTO MultiTestt6 VALUES (" + i + ", 'testValue" + i + "')");
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
     * CONJ-141 : Batch Statement Rewrite: Support for ON DUPLICATE KEY
     *
     * @throws SQLException
     */
    @Test
    public void rewriteBatchedStatementsWithQueryFirstAndLAst() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&rewriteBatchedStatements=true");

            PreparedStatement sqlInsert = connection.prepareStatement("INSERT INTO MultiTestt3_dupp(col1, pkey,col2,col3,col4) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE pkey=pkey+10");
            sqlInsert.setInt(1, 1);
            sqlInsert.setInt(2, 2);
            sqlInsert.addBatch();

            sqlInsert.setInt(1, 2);
            sqlInsert.setInt(2, 5);
            sqlInsert.addBatch();

            sqlInsert.setInt(1, 7);
            sqlInsert.setInt(2, 6);
            sqlInsert.addBatch();
            sqlInsert.executeBatch();
        } finally {
            connection.close();
        }

    }

    /**
     * CONJ-142: Using a semicolon in a string with "rewriteBatchedStatements=true" fails
     *
     * @throws SQLException
     */
    @Test
    public void rewriteBatchedStatementsSemicolon() throws SQLException {
        log.debug("rewriteBatchedStatementsSemicolon begin");
        // set the rewrite batch statements parameter
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connURI, props);

            int currentInsert = retrieveSessionVariableFromServer(tmpConnection, "Com_insert");

            PreparedStatement sqlInsert = tmpConnection.prepareStatement("/*CLIENT*/ INSERT INTO MultiTestt3 (message) VALUES (?)");
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

            assertEquals(1, retrieveSessionVariableFromServer(tmpConnection, "Com_insert") - currentInsert);

            currentInsert = retrieveSessionVariableFromServer(tmpConnection, "Com_insert");

            // Test for multiple statements which isn't allowed. rewrite shouldn't work
            sqlInsert = tmpConnection.prepareStatement("/*CLIENT*/ INSERT INTO MultiTestt3 (message) VALUES (?); INSERT INTO MultiTestt3 (message) VALUES ('multiple')");
            sqlInsert.setString(1, "aa");
            sqlInsert.addBatch();
            sqlInsert.setString(1, "b;b");
            sqlInsert.addBatch();
            updateCounts = sqlInsert.executeBatch();

            // rewrite should NOT be possible. Therefore there should be 2 commands updating 1 row each.
            Assert.assertEquals(2, updateCounts.length);
            Assert.assertEquals(1, updateCounts[0]);
            Assert.assertEquals(1, updateCounts[1]);

            assertEquals(4, retrieveSessionVariableFromServer(tmpConnection, "Com_insert") - currentInsert);

        } finally {
            log.debug("rewriteBatchedStatementsSemicolon end");
            if (tmpConnection != null) tmpConnection.close();
        }
    }

    private PreparedStatement prepareStatementBatch(Connection tmpConnection, int size) throws SQLException {
        PreparedStatement preparedStatement = tmpConnection.prepareStatement("INSERT INTO MultiTestt7 VALUES (?, ?)");
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
        log.debug("rewriteBatchedStatementsUpdateTest begin");
        // set the rewrite batch statements parameter
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connURI, props);
            tmpConnection.setClientInfo(props);
            verifyUpdateCount(tmpConnection, 0);
            int cycles = 1000;
            prepareStatementBatch(tmpConnection, cycles).executeBatch();  // populate the table
            PreparedStatement preparedStatement = tmpConnection.prepareStatement("UPDATE MultiTestt7 SET test = ? WHERE id = ?");
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
            log.debug("rewriteBatchedStatementsUpdateTest end");
            if (tmpConnection != null) tmpConnection.close();
        }
    }


    /**
     * CONJ-152: rewriteBatchedStatements and multiple executeBatch check
     *
     * @throws SQLException
     */
    @Test
    public void testMultipleExecuteBatch() throws SQLException {
        log.debug("testMultipleExecuteBatch begin");
        // set the rewrite batch statements parameter
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connURI, props);
            tmpConnection.setClientInfo(props);
            verifyUpdateCount(tmpConnection, 0);
            tmpConnection.createStatement().execute("insert into MultiTestt8 values(1,'a'),(2,'a')");

            PreparedStatement preparedStatement = tmpConnection.prepareStatement("UPDATE MultiTestt8 SET test = ? WHERE id = ?");
            preparedStatement.setString(1, "executebatch");
            preparedStatement.setInt(2, 1);
            preparedStatement.addBatch();
            preparedStatement.setString(1, "executebatch2");
            preparedStatement.setInt(2, 3);
            preparedStatement.addBatch();

            int[] updateCounts = preparedStatement.executeBatch();
            assertEquals(2, updateCounts.length);

            preparedStatement.setString(1, "executebatch3");
            preparedStatement.setInt(2, 1);
            preparedStatement.addBatch();
            updateCounts = preparedStatement.executeBatch();
            assertEquals(1, updateCounts.length);
        } finally {
            log.debug("testMultipleExecuteBatch end");
            if (tmpConnection != null) tmpConnection.close();
        }
    }

    @Test
    public void rewriteBatchedStatementsInsertWithDuplicateRecordsTest() throws SQLException {
        log.debug("rewriteBatchedStatementsInsertWithDuplicateRecordsTest begin");
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connURI, props);
            verifyInsertCount(tmpConnection, 0);
            Statement statement = tmpConnection.createStatement();
            for (int i = 0; i < 100; i++) {
                int newId = i % 20; //to create duplicate id's
                String roleTxt = "VAMPIRE" + newId;
                statement.addBatch("INSERT IGNORE  INTO MultiTestreWriteDuplicateTestTable VALUES (" + newId + ", '" + roleTxt + "')");
            }
            int[] updateCounts = statement.executeBatch();
            assertEquals(100, updateCounts.length);

            for (int i = 0; i < updateCounts.length; i++) {
                assertEquals(MySQLStatement.SUCCESS_NO_INFO, updateCounts[i]);
            }
            verifyInsertCount(tmpConnection, 1);
            verifyUpdateCount(tmpConnection, 0);
        } finally {
            log.debug("rewriteBatchedStatementsInsertWithDuplicateRecordsTest end");
            if (tmpConnection != null) tmpConnection.close();
        }
    }

    @Test
    public void updateCountTest() throws SQLException {
        log.debug("updateCountTest begin");
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connURI, props);
            PreparedStatement sqlInsert = tmpConnection.prepareStatement("INSERT IGNORE INTO MultiTestt4 (id,test) VALUES (?,?)");
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
            Assert.assertEquals(1, insertCounts[0]);
            Assert.assertEquals(0, insertCounts[1]);
            Assert.assertEquals(1, insertCounts[2]);
            Assert.assertEquals(1, insertCounts[3]);


            PreparedStatement sqlUpdate = tmpConnection.prepareStatement("UPDATE MultiTestt4 SET test = ? WHERE test = ?");
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
            log.trace("updateCounts : " + updateCounts.length);
            Assert.assertEquals(3, updateCounts.length);
            Assert.assertEquals(1, updateCounts[0]);
            Assert.assertEquals(0, updateCounts[1]);
            Assert.assertEquals(2, updateCounts[2]);
        } finally {
            log.debug("updateCountTest end");
            if (tmpConnection != null) tmpConnection.close();
        }
    }


    private void verifyUpdateCount(Connection tmpConnection, int updateCount) throws SQLException {
        assertEquals(updateCount, retrieveSessionVariableFromServer(tmpConnection, "Com_update"));
    }

    @Test
    public void testInsertWithLeadingConstantValue() throws Exception {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connURI, props);
            PreparedStatement insertStmt = tmpConnection.prepareStatement("INSERT INTO MultiTesttest_table (col1, col2, col3, col4, col5) values('some value', ?, 'other value', ?, 'third value')");
            insertStmt.setString(1, "a1");
            insertStmt.setString(2, "a2");
            insertStmt.addBatch();
            insertStmt.setString(1, "b1");
            insertStmt.setString(2, "b2");
            insertStmt.addBatch();
            insertStmt.executeBatch();
        } finally {
            if (tmpConnection != null) tmpConnection.close();
        }
    }


    @Test
    public void testInsertWithoutFirstContent() throws Exception {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connURI, props);
            PreparedStatement insertStmt = tmpConnection.prepareStatement("INSERT INTO MultiTesttest_table2 (col2, col3, col4, col5) values(?, 'other value', ?, 'third value')");
            insertStmt.setString(1, "a1");
            insertStmt.setString(2, "a2");
            insertStmt.addBatch();
            insertStmt.setString(1, "b1");
            insertStmt.setString(2, "b2");
            insertStmt.addBatch();
            insertStmt.executeBatch();
        } finally {
            if (tmpConnection != null) tmpConnection.close();
        }
    }

    @Test
    public void testduplicate() throws Exception {
        createTable("SOME_TABLE", "ID INT(11) not null, FOO INT(11), PRIMARY KEY (ID), UNIQUE INDEX `FOO` (`FOO`)");
        String sql = "insert into `SOME_TABLE` (`ID`, `FOO`) values (?, ?) on duplicate key update `SOME_TABLE`.`FOO` = ?";
        PreparedStatement st = sharedConnection.prepareStatement(sql);
        st.setInt(1, 1);
        st.setInt(2, 1);
        st.setInt(3, 1);
        st.addBatch();

        st.setInt(1, 2);
        st.setInt(2, 1);
        st.setInt(3, 2);
        st.addBatch();
        st.executeBatch();

        sql = "/*CLIENT*/"+sql;
        st = sharedConnection.prepareStatement(sql);
        st.setInt(1, 4);
        st.setInt(2, 4);
        st.setInt(3, 5);
        st.addBatch();

        st.setInt(1, 5);
        st.setInt(2, 4);
        st.setInt(3, 8);
        st.addBatch();
        st.executeBatch();
    }


}

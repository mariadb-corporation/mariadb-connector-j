package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;


public class MultiTest extends BaseTest {

    /**
     * Tables initialisation.
     */
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
        createTable("MultiTestt10", "id int");
        createTable("MultiTestreWriteDuplicateTestTable", "id int, name varchar(100), PRIMARY KEY (`id`)");
        createTable("MultiTesttselect1", "LAST_UPDATE_DATETIME TIMESTAMP , nn int");
        createTable("MultiTesttselect2", "nn int");
        createTable("MultiTesttselect3", "LAST_UPDATE_DATETIME TIMESTAMP , nn int");
        createTable("MultiTesttselect4", "nn int");
        createTable("MultiTestt3_dupp", "col1 int, pkey int NOT NULL, col2 int, col3 int, col4 int, PRIMARY KEY "
                + "(`pkey`)");
        createTable("MultiTesttest_table", "col1 VARCHAR(32), col2 VARCHAR(32), col3 VARCHAR(32), col4 VARCHAR(32), "
                + "col5 VARCHAR(32)");
        createTable("MultiTesttest_table2", "col1 VARCHAR(32), col2 VARCHAR(32), col3 VARCHAR(32), col4 VARCHAR(32), "
                + "col5 VARCHAR(32)");
        createTable("MultiTestValues", "col1 VARCHAR(32), col2 VARCHAR(32)");

        createTable("MultiTestprepsemi", "id int not null primary key auto_increment, text text");
        createTable("MultiTestA", "data varchar(10)");
        if (testSingleHost) {
            Statement st = sharedConnection.createStatement();
            st.execute("insert into MultiTestt1 values(1,'a'),(2,'a')");
            st.execute("insert into MultiTestt2 values(1,'a'),(2,'a')");
            st.execute("insert into MultiTestt5 values(1,'a'),(2,'a'),(2,'b')");
        }

    }

    @Test
    public void rewriteSelectQuery() throws Throwable {
        Statement st = sharedConnection.createStatement();
        st.execute("INSERT INTO MultiTesttselect2 VALUES (1)");
        PreparedStatement ps = sharedConnection.prepareStatement("/*CLIENT*/ insert into MultiTesttselect1 "
                + "(LAST_UPDATE_DATETIME, nn) select ?, nn from MultiTesttselect2");
        ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        ps.executeUpdate();

        ResultSet rs = st.executeQuery("SELECT * FROM MultiTesttselect1");
        rs.next();
        Assert.assertEquals(rs.getInt(2), 1);
    }

    @Test
    public void rewriteSelectQueryServerPrepared() throws Throwable {
        Statement st = sharedConnection.createStatement();
        st.execute("INSERT INTO MultiTesttselect4 VALUES (1)");
        PreparedStatement ps = sharedConnection.prepareStatement("insert into MultiTesttselect3 (LAST_UPDATE_DATETIME,"
                + " nn) select ?, nn from MultiTesttselect4");
        ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        ps.executeUpdate();

        ResultSet rs = st.executeQuery("SELECT * FROM MultiTesttselect3");
        rs.next();
        Assert.assertEquals(rs.getInt(2), 1);
    }

    @Test
    public void basicTest() throws SQLException {
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
        } finally {
            connection.close();
        }
    }

    @Test
    public void updateTest() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            Statement statement = connection.createStatement();
            statement.execute("update MultiTestt5 set test='a " + System.currentTimeMillis()
                    + "' where id = 2;select * from MultiTestt2;update MultiTestt5 set test='a2 " + System.currentTimeMillis()
                    + "' where id = 1;");
            assertNull(statement.getResultSet());
            assertEquals(2, statement.getUpdateCount());
            assertTrue(statement.getMoreResults());
            assertEquals(-1, statement.getUpdateCount());
            ResultSet rs = statement.getResultSet();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue(count > 0);

            assertTrue(statement.getMoreResults());
            assertEquals(1, statement.getUpdateCount());
            assertNull(statement.getResultSet());
            assertFalse(statement.getMoreResults());

        } finally {
            connection.close();
        }
    }

    @Test
    public void updateTest2() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            Statement statement = connection.createStatement();
            statement.execute("select * from MultiTestt2;update MultiTestt5 set test='a " + System.currentTimeMillis()
                    + "' where id = 2;");
            ResultSet rs = statement.getResultSet();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue(count == 2);
            statement.getMoreResults();

            int updateNb = statement.getUpdateCount();
            assertEquals(2, updateNb);
        } finally {
            connection.close();
        }
    }

    @Test
    public void selectTest() throws SQLException {
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
        } finally {
            connection.close();
        }
    }

    @Test
    public void setMaxRowsMulti() throws Exception {
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
        } finally {
            connection.close();
        }
    }

    /**
     * Conj-99: rewriteBatchedStatements parameter.
     *
     * @throws SQLException exception
     */
    @Test
    public void rewriteBatchedStatementsDisabledInsertionTest() throws SQLException {
        verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean.FALSE, 3000);
    }


    /**
     * Conj-206: rewriteBatchedStatements parameter take care of max_allowed_size.
     *
     * @throws SQLException exception
     */
    @Test
    public void rewriteBatchedMaxAllowedSizeTest() throws SQLException {

        createTable("MultiTestt6", "id int, test varchar(10000)");
        Assume.assumeTrue(checkMaxAllowedPacketMore8m("rewriteBatchedMaxAllowedSizeTest"));
        Statement st = sharedConnection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        if (rs.next()) {
            long maxAllowedPacket = rs.getInt(1);
            int totalInsertCommands = (int) Math.ceil(maxAllowedPacket / 10050);
            verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean.TRUE, totalInsertCommands);
        } else {
            fail();
        }
    }

    @Test
    public void rewriteBatchedWithoutParam() throws SQLException {
        try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO MultiTestt10 VALUES (1)");
            for (int i = 0; i < 100; i++) {
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            ResultSet rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM MultiTestt10");
            rs.next();
            assertEquals(100, rs.getInt(1));
        }
    }

    /**
     * CONJ-329 error for rewrite without parameter.
     *
     * @throws SQLException if exception occur
     */
    @Test
    public void rewriteStatementWithoutParameter() throws SQLException {
        try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
            PreparedStatement statement = connection.prepareStatement("SELECT 1");
            try {
                statement.executeQuery();
            } finally {
                statement.close();
            }
        }
    }

    /**
     * CONJ-330 - correction using execute...() for rewriteBatchedStatements
     *
     * @throws SQLException if exception occur
     */
    @Test
    public void rewriteMonoQueryStatementWithParameter() throws SQLException {
        try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
            String failingQuery1 = "SELECT (1=? AND 2=2)";
            String failingQuery2 = "SELECT (1=?) AND 2=2";
            String workingQuery = "SELECT 1=? AND (2=2)";

            try (PreparedStatement statement = connection.prepareStatement(failingQuery1)) {
                checkResult(statement);
            }

            try (PreparedStatement statement = connection.prepareStatement(failingQuery2)) {
                checkResult(statement);
            }

            try (PreparedStatement statement = connection.prepareStatement(workingQuery)) {
                checkResult(statement);
            }
        }
    }

    private void checkResult(PreparedStatement statement) throws SQLException {
        statement.setInt(1, 1);
        statement.executeQuery();
        ResultSet rs = statement.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getBoolean(1));
    }


    @Test
    public void testServerPrepareMeta() throws Throwable {
        Connection connection = null;
        try {
            connection = setConnection("&rewriteBatchedStatements=true");
            createTable("insertSelectTable1", "tt int");
            createTable("insertSelectTable2", "tt int");

            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO insertSelectTable1 "
                            + "SELECT a1.tt FROM insertSelectTable2 a1 "
                            + "WHERE a1.tt = ? ");
            ps.setInt(1, 1);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.addBatch();
            ps.executeBatch();

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void verifyInsertBehaviorBasedOnRewriteBatchedStatements(Boolean rewriteBatchedStatements,
                                                                     int totalInsertCommands) throws SQLException {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", rewriteBatchedStatements.toString());
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);
            verifyInsertCount(tmpConnection, 0);
            Statement statement = tmpConnection.createStatement();
            for (int i = 0; i < totalInsertCommands; i++) {
                statement.addBatch("INSERT INTO MultiTestt6 VALUES (" + i + ", 'testValue" + i + "')");
            }
            int[] updateCounts = statement.executeBatch();
            assertEquals(totalInsertCommands, updateCounts.length);
            int totalUpdates = 0;
            for (int count = 0; count < updateCounts.length; count++) {
                assertEquals(1, updateCounts[count]);
                totalUpdates += updateCounts[count];
            }
            assertEquals(totalInsertCommands, totalUpdates);
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
     * Conj-141 : Batch Statement Rewrite: Support for ON DUPLICATE KEY.
     *
     * @throws SQLException exception
     */
    @Test
    public void rewriteBatchedStatementsWithQueryFirstAndLAst() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&rewriteBatchedStatements=true");

            PreparedStatement sqlInsert = connection.prepareStatement("INSERT INTO MultiTestt3_dupp(col1, pkey,col2,"
                    + "col3,col4) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE pkey=pkey+10");
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
     * Conj-142: Using a semicolon in a string with "rewriteBatchedStatements=true" fails.
     *
     * @throws SQLException exception
     */
    @Test
    public void rewriteBatchedStatementsSemicolon() throws SQLException {
        // set the rewrite batch statements parameter
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);

            final int currentInsert = retrieveSessionVariableFromServer(tmpConnection, "Com_insert");

            PreparedStatement sqlInsert = tmpConnection.prepareStatement(
                    "INSERT INTO MultiTestt3 (message) VALUES (?)");
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
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, updateCounts[0]);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, updateCounts[1]);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, updateCounts[2]);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, updateCounts[3]);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, updateCounts[4]);
            assertEquals(1, retrieveSessionVariableFromServer(tmpConnection, "Com_insert") - currentInsert);

            final int secondCurrentInsert = retrieveSessionVariableFromServer(tmpConnection, "Com_insert");

            // rewrite for multiple statements isn't possible, so use allowMutipleQueries
            sqlInsert = tmpConnection.prepareStatement("INSERT INTO MultiTestt3 (message) VALUES (?); "
                    + "INSERT INTO MultiTestt3 (message) VALUES ('multiple')");
            sqlInsert.setString(1, "aa");
            sqlInsert.addBatch();
            sqlInsert.setString(1, "b;b");
            sqlInsert.addBatch();
            updateCounts = sqlInsert.executeBatch();

            Assert.assertEquals(4, updateCounts.length);
            Assert.assertEquals(1, updateCounts[0]);
            Assert.assertEquals(1, updateCounts[1]);
            Assert.assertEquals(1, updateCounts[2]);
            Assert.assertEquals(1, updateCounts[3]);

            assertEquals(4, retrieveSessionVariableFromServer(tmpConnection, "Com_insert") - secondCurrentInsert);

        } finally {
            if (tmpConnection != null) {
                tmpConnection.close();
            }
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
     * Conj-215: Batched statements with rewriteBatchedStatements that end with a semicolon fails.
     *
     * @throws SQLException exception
     */
    @Test
    public void semicolonTest() throws SQLException {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);
            Statement sqlInsert = tmpConnection.createStatement();
            verifyInsertCount(tmpConnection, 0);

            for (int i = 0; i < 100; i++) {
                sqlInsert.addBatch("insert into MultiTestprepsemi (text) values ('This is a test" + i + "');");
            }
            sqlInsert.executeBatch();
            verifyInsertCount(tmpConnection, 100);
            for (int i = 0; i < 100; i++) {
                sqlInsert.addBatch("insert into MultiTestprepsemi (text) values ('This is a test" + i + "')");
            }
            sqlInsert.executeBatch();
            verifyInsertCount(tmpConnection, 200);
        } finally {
            tmpConnection.close();
        }
    }


    /**
     * Conj-99: rewriteBatchedStatements parameter.
     *
     * @throws SQLException exception
     */
    @Test
    public void rewriteBatchedStatementsUpdateTest() throws SQLException {
        // set the rewrite batch statements parameter
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);
            tmpConnection.setClientInfo(props);
            verifyUpdateCount(tmpConnection, 0);
            int cycles = 1000;
            prepareStatementBatch(tmpConnection, cycles).executeBatch();  // populate the table
            PreparedStatement preparedStatement = tmpConnection.prepareStatement(
                    "UPDATE MultiTestt7 SET test = ? WHERE id = ?");
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
            if (tmpConnection != null) {
                tmpConnection.close();
            }
        }
    }


    /**
     * Conj-152: rewriteBatchedStatements and multiple executeBatch check.
     *
     * @throws SQLException exception
     */
    @Test
    public void testMultipleExecuteBatch() throws SQLException {
        // set the rewrite batch statements parameter
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);
            tmpConnection.setClientInfo(props);
            verifyUpdateCount(tmpConnection, 0);
            tmpConnection.createStatement().execute("insert into MultiTestt8 values(1,'a'),(2,'a')");

            PreparedStatement preparedStatement = tmpConnection.prepareStatement(
                    "UPDATE MultiTestt8 SET test = ? WHERE id = ?");
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
            if (tmpConnection != null) {
                tmpConnection.close();
            }
        }
    }

    @Test
    public void rewriteBatchedStatementsInsertWithDuplicateRecordsTest() throws SQLException {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);
            verifyInsertCount(tmpConnection, 0);
            Statement statement = tmpConnection.createStatement();
            for (int i = 0; i < 100; i++) {
                int newId = i % 20; //to create duplicate id's
                String roleTxt = "VAMPIRE" + newId;
                statement.addBatch("INSERT IGNORE  INTO MultiTestreWriteDuplicateTestTable VALUES (" + newId
                        + ", '" + roleTxt + "')");
            }
            int[] updateCounts = statement.executeBatch();
            assertEquals(100, updateCounts.length);

            for (int i = 0; i < updateCounts.length; i++) {
                assertEquals((i < 20) ? 1 : 0, updateCounts[i]);
            }
            verifyInsertCount(tmpConnection, 100);
            verifyUpdateCount(tmpConnection, 0);
        } finally {
            if (tmpConnection != null) {
                tmpConnection.close();
            }
        }
    }

    @Test
    public void updateCountTest() throws SQLException {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);
            PreparedStatement sqlInsert = tmpConnection.prepareStatement(
                    "INSERT IGNORE INTO MultiTestt4 (id,test) VALUES (?,?)");
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
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, insertCounts[0]);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, insertCounts[1]);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, insertCounts[2]);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, insertCounts[3]);


            PreparedStatement sqlUpdate = tmpConnection.prepareStatement(
                    "UPDATE MultiTestt4 SET test = ? WHERE test = ?");
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
            if (tmpConnection != null) {
                tmpConnection.close();
            }
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
            tmpConnection = openNewConnection(connUri, props);
            PreparedStatement insertStmt = tmpConnection.prepareStatement("INSERT INTO MultiTesttest_table (col1, col2,"
                    + " col3, col4, col5) values('some value', ?, 'other value', ?, 'third value')");
            insertStmt.setString(1, "a1");
            insertStmt.setString(2, "a2");
            insertStmt.addBatch();
            insertStmt.setString(1, "b1");
            insertStmt.setString(2, "b2");
            insertStmt.addBatch();
            insertStmt.executeBatch();
        } finally {
            if (tmpConnection != null) {
                tmpConnection.close();
            }
        }
    }


    @Test
    public void testInsertWithoutFirstContent() throws Exception {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);
            PreparedStatement insertStmt = tmpConnection.prepareStatement("INSERT INTO MultiTesttest_table2 "
                    + "(col2, col3, col4, col5) values(?, 'other value', ?, 'third value')");
            insertStmt.setString(1, "a1");
            insertStmt.setString(2, "a2");
            insertStmt.addBatch();
            insertStmt.setString(1, "b1");
            insertStmt.setString(2, "b2");
            insertStmt.addBatch();
            insertStmt.executeBatch();
        } finally {
            if (tmpConnection != null) {
                tmpConnection.close();
            }
        }
    }

    @Test
    public void testduplicate() throws Exception {
        createTable("SOME_TABLE", "ID INT(11) not null, FOO INT(11), PRIMARY KEY (ID), UNIQUE INDEX `FOO` (`FOO`)");
        String sql = "insert into `SOME_TABLE` (`ID`, `FOO`) values (?, ?) "
                + "on duplicate key update `SOME_TABLE`.`FOO` = ?";
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

        sql = "/*CLIENT*/" + sql;
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


    @Test
    public void valuesWithoutSpace() throws Exception {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("allowMultiQueries", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);
            PreparedStatement insertStmt = tmpConnection.prepareStatement("INSERT INTO MultiTestValues (col1, col2)VALUES (?, ?)");
            insertStmt.setString(1, "a");
            insertStmt.setString(2, "b");
            insertStmt.addBatch();
            insertStmt.setString(1, "c");
            insertStmt.setString(2, "d");
            insertStmt.addBatch();
            insertStmt.executeBatch();
        } finally {
            if (tmpConnection != null) {
                tmpConnection.close();
            }
        }
    }

    /**
     * Conj-208 : Rewritten batch inserts can fail without a space before the VALUES clause.
     *
     * @throws Exception exception
     */
    @Test
    public void valuesWithoutSpacewithoutRewrite() throws Exception {
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);
            PreparedStatement insertStmt = tmpConnection.prepareStatement("INSERT INTO MultiTestValues (col1, col2)VALUES (?, ?)");
            insertStmt.setString(1, "a");
            insertStmt.setString(2, "b");
            insertStmt.addBatch();
            insertStmt.setString(1, "c");
            insertStmt.setString(2, "d");
            insertStmt.addBatch();
            insertStmt.executeBatch();
        } finally {
            if (tmpConnection != null) {
                tmpConnection.close();
            }
        }
    }


    @Test
    public void continueOnBatchError() throws SQLException {
        for (int i = 0; i < 16; i++) {
            continueOnBatchError(i % 16 < 8, i % 8 < 4, i % 4 < 2, i % 2 == 0);
        }
    }

    private void continueOnBatchError(boolean continueBatch, boolean serverPrepare,
                                      boolean rewrite, boolean batchMulti) throws SQLException {
        System.out.println("continueBatch:" + continueBatch
                + " serverPrepare:" + serverPrepare
                + " rewrite:" + rewrite
                + " batchMulti:" + batchMulti);
        createTable("MultiTestt9", "id int not null primary key, test varchar(10)");
        try (Connection connection = setBlankConnection(
                "&useServerPrepStmts=" + serverPrepare
                        + "&useBatchMultiSend=" + batchMulti
                        + "&continueBatchOnError=" + continueBatch
                        + "&rewriteBatchedStatements=" + rewrite)) {
            PreparedStatement pstmt = connection.prepareStatement("INSERT INTO MultiTestt9 (id, test) VALUES (?, ?)");
            for (int i = 0; i < 10; i++) {
                pstmt.setInt(1, (i == 5) ? 0 : i);
                pstmt.setString(2, String.valueOf(i));
                pstmt.addBatch();
            }
            try {
                pstmt.executeBatch();
                fail("Must have thrown SQLException");
            } catch (BatchUpdateException e) {


                int[] updateCount = e.getUpdateCounts();
                Assert.assertEquals(10, updateCount.length);
                if (rewrite) {
                    //rewrite exception is all or nothing
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[0]);
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[1]);
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[2]);
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[3]);
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[4]);
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[5]);
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[6]);
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[7]);
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[8]);
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[9]);
                } else {
                    Assert.assertEquals(1, updateCount[0]);
                    Assert.assertEquals(1, updateCount[1]);
                    Assert.assertEquals(1, updateCount[2]);
                    Assert.assertEquals(1, updateCount[3]);
                    Assert.assertEquals(1, updateCount[4]);
                    Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[5]);
                    if (continueBatch) {
                        Assert.assertEquals(1, updateCount[6]);
                        Assert.assertEquals(1, updateCount[7]);
                        Assert.assertEquals(1, updateCount[8]);
                        Assert.assertEquals(1, updateCount[9]);
                    } else {
                        if (batchMulti) {
                            //send in batch, so continue will be handle, but send packet is executed.
                            Assert.assertEquals(1, updateCount[6]);
                            Assert.assertEquals(1, updateCount[7]);
                            Assert.assertEquals(1, updateCount[8]);
                            Assert.assertEquals(1, updateCount[9]);
                        } else {
                            Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[6]);
                            Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[7]);
                            Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[8]);
                            Assert.assertEquals(Statement.EXECUTE_FAILED, updateCount[9]);
                        }
                    }
                }

                ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM MultiTestt9");
                //check result
                if (!rewrite) {
                    checkNextData(0, rs);
                    checkNextData(1, rs);
                    checkNextData(2, rs);
                    checkNextData(3, rs);
                    checkNextData(4, rs);

                    if (continueBatch || batchMulti) {
                        checkNextData(6, rs);
                        checkNextData(7, rs);
                        checkNextData(8, rs);
                        checkNextData(9, rs);
                    }
                }
                Assert.assertFalse(rs.next());
            }
        }
    }

    private void checkNextData(int value, ResultSet rs) throws SQLException {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(value, rs.getInt(1));
        Assert.assertEquals(String.valueOf(value), rs.getString(2));
    }

    @Test
    public void testCloseStatementWithoutQuery() throws SQLException {
        final Statement statement = sharedConnection.createStatement();
        // Make sure it is a streaming statement:
        statement.setFetchSize(Integer.MIN_VALUE);
        for (int count = 1; count <= 10; count++) {
            statement.close();
        }
    }

    @Test
    public void testClosePrepareStatementWithoutQuery() throws SQLException {
        final PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT 1");
        // Make sure it is a streaming statement:
        preparedStatement.setFetchSize(Integer.MIN_VALUE);
        for (int count = 1; count <= 10; count++) {
            preparedStatement.close();
        }
    }

    @Test
    public void testCloseStatement() throws SQLException {
        createTable("testStatementClose", "id int");
        final Statement statement = sharedConnection.createStatement();
        // Make sure it is a streaming statement:
        statement.setFetchSize(1);

        statement.execute("INSERT INTO testStatementClose (id) VALUES (1)");
        for (int count = 1; count <= 10; count++) {
            statement.close();
        }
    }

    @Test
    public void testClosePrepareStatement() throws SQLException {
        createTable("testPrepareStatementClose", "id int");
        sharedConnection.createStatement().execute("INSERT INTO testPrepareStatementClose(id) VALUES (1),(2),(3)");
        final PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT * FROM testPrepareStatementClose");
        preparedStatement.execute();
        // Make sure it is a streaming statement:
        preparedStatement.setFetchSize(1);

        for (int count = 1; count <= 10; count++) {
            preparedStatement.close();
        }
    }


    @Test
    public void rewriteErrorRewriteValues() throws SQLException {
        prepareBatchUpdateException(true, true);
    }

    @Test
    public void rewriteErrorRewriteMulti() throws SQLException {
        prepareBatchUpdateException(false, true);
    }

    @Test
    public void rewriteErrorStandard() throws SQLException {
        prepareBatchUpdateException(false, false);
    }


    private void prepareBatchUpdateException(Boolean rewriteBatchedStatements, Boolean allowMultiQueries) throws SQLException {

        createTable("batchUpdateException", "i int,PRIMARY KEY (i)");
        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", rewriteBatchedStatements.toString());
        props.setProperty("allowMultiQueries", allowMultiQueries.toString());
        props.setProperty("useServerPrepStmts", "false");
        Connection tmpConnection = null;
        try {
            tmpConnection = openNewConnection(connUri, props);
            verifyInsertCount(tmpConnection, 0);

            PreparedStatement ps = tmpConnection.prepareStatement("insert into batchUpdateException values(?)");
            ps.setInt(1, 1);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.addBatch();
            ps.setInt(1, 1); // will fail, duplicate primary key
            ps.addBatch();
            ps.setInt(1, 3);
            ps.addBatch();

            try {
                ps.executeBatch();
                fail("exception should be throw above");
            } catch (BatchUpdateException bue) {
                int[] updateCounts = bue.getUpdateCounts();
                if (rewriteBatchedStatements) {
                    assertEquals(4, updateCounts.length);
                    assertEquals(Statement.EXECUTE_FAILED, updateCounts[0]);
                    assertEquals(Statement.EXECUTE_FAILED, updateCounts[1]);
                    assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
                    assertEquals(Statement.EXECUTE_FAILED, updateCounts[3]);
                    verifyInsertCount(tmpConnection, 1);
                } else {
                    assertEquals(4, updateCounts.length);
                    assertEquals(1, updateCounts[0]);
                    assertEquals(1, updateCounts[1]);
                    assertEquals(Statement.EXECUTE_FAILED, updateCounts[2]);
                    assertEquals(1, updateCounts[3]);
                    verifyInsertCount(tmpConnection, 4);
                }
                assertTrue(bue.getCause() instanceof SQLIntegrityConstraintViolationException);

            }
        } finally {
            if (tmpConnection != null) {
                tmpConnection.close();
            }
        }
    }

    /**
     * Test that using -1 (last prepared Statement), if next execution has parameter corresponding,
     * previous prepare will not be used.
     *
     * @throws Throwable if any error.
     */
    @Test
    public void testLastPrepareDiscarded() throws Throwable {

        PreparedStatement preparedStatement1 = sharedConnection.prepareStatement("INSERT INTO MultiTestA (data) VALUES (?)");
        preparedStatement1.setString(1, "A");
        preparedStatement1.execute();

        PreparedStatement preparedStatement2 = sharedConnection.prepareStatement("select * from (select ? `field1` from dual) as tt");
        preparedStatement2.setString(1, "B");
        try {
            preparedStatement2.execute();
            //must have thrown error if server prepare.
        } catch (Exception e) {
            //server prepare.
            ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT * FROM MultiTestA");
            assertTrue(rs.next());
            assertEquals("A", rs.getString(1));
            assertFalse(rs.next()); //"B" must not have been saved in Table MultiTestA
        }

    }
}

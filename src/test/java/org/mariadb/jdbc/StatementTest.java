package org.mariadb.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class StatementTest extends BaseTest {

    private final static int ER_BAD_FIELD_ERROR = 1054;
    private final static int ER_NON_INSERTABLE_TABLE = 1471;
    private final static int ER_NO_SUCH_TABLE = 1146;
    private final static int ER_NONUPDATEABLE_COLUMN = 1348;
    private final static int ER_PARSE_ERROR = 1064;
    private final static int ER_NO_PARTITION_FOR_GIVEN_VALUE = 1526;
    private final static int ER_LOAD_DATA_INVALID_COLUMN = 1611;
    private final static int ER_ADD_PARTITION_NO_NEW_PARTITION = 1514;
    private final String ER_BAD_FIELD_ERROR_STATE = "42S22";
    private final String ER_NON_INSERTABLE_TABLE_STATE = "HY000";
    private final String ER_NO_SUCH_TABLE_STATE = "42S02";
    private final String ER_NONUPDATEABLE_COLUMN_STATE = "HY000";
    private final String ER_PARSE_ERROR_STATE = "42000";
    private final String ER_NO_PARTITION_FOR_GIVEN_VALUE_STATE = "HY000";
    private final String ER_LOAD_DATA_INVALID_COLUMN_STATE = "HY000";
    private final String ER_ADD_PARTITION_NO_NEW_PARTITION_STATE = "HY000";
    private Statement statement;
    public StatementTest() {
    }

    @Test
    public void wrapperTest() throws SQLException {
        MySQLStatement mysqlStatement = new MySQLStatement((MySQLConnection) connection);
        assertTrue(mysqlStatement.isWrapperFor(Statement.class));
        assertFalse(mysqlStatement.isWrapperFor(SQLException.class));
        assertThat(mysqlStatement.unwrap(Statement.class), equalTo((Statement) mysqlStatement));
        try {
            mysqlStatement.unwrap(SQLException.class);
            fail("MySQLStatement class unwrapped as SQLException class");
        } catch (SQLException sqle) {
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }
        mysqlStatement.close();
    }

    /**
     * CONJ-90
     *
     * @throws SQLException
     */
    @Test
    public void reexecuteStatementTest() throws SQLException {
        setConnection("&allowMultiQueries=true");
        PreparedStatement stmt = connection.prepareStatement("SELECT 1");
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        rs = stmt.executeQuery();
        stmt.close();
        connection.close();
    }

    @Test(expected = SQLException.class)
    public void afterConnectionClosedTest() throws SQLException {
        Connection conn2 = DriverManager.getConnection("jdbc:mariadb://localhost:3306/test?user=root");
        Statement st1 = conn2.createStatement();
        st1.close();
        conn2.close();
        Statement st2 = conn2.createStatement();
        assertTrue(false);
        st2.close();
    }

    @Before
    public void setUp() throws SQLException {
        statement = connection.createStatement();
    }

    @Test
    public void testColumnsDoNotExist() throws SQLException {
        createTestTable("vendor_code_test","id int not null primary key auto_increment, test boolean");

        try {
            statement.executeQuery("select * from vendor_code_test where crazy_column_that_does_not_exist = 1");
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_BAD_FIELD_ERROR, sqlException.getErrorCode());
            assertEquals(ER_BAD_FIELD_ERROR_STATE, sqlException.getSQLState());
        }
    }

    @Test
    public void testNonInsertableTable() throws SQLException {
        createTestTable("vendor_code_test","id int not null primary key auto_increment, test boolean");
        statement.execute("create or replace view vendor_code_test_view as select id as id1, id as id2, test from vendor_code_test");

        try {
            statement.executeQuery("insert into vendor_code_test_view VALUES (null, null, true)");
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_NON_INSERTABLE_TABLE, sqlException.getErrorCode());
            assertEquals(ER_NON_INSERTABLE_TABLE_STATE, sqlException.getSQLState());
        }
    }

    @Test
    public void testNoSuchTable() throws SQLException, UnsupportedEncodingException {
        statement.execute("drop table if exists vendor_code_test");
        try {
            statement.execute("SELECT * FROM vendor_code_test");
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_NO_SUCH_TABLE, sqlException.getErrorCode());
            assertEquals(ER_NO_SUCH_TABLE_STATE, sqlException.getSQLState());
        }
    }

    @Test
    public void testNoSuchTableBatchUpdate() throws SQLException, UnsupportedEncodingException {
        statement.execute("drop table if exists vendor_code_test");
        statement.addBatch("INSERT INTO vendor_code_test VALUES('dummyValue')");
        try {
            statement.executeBatch();
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_NO_SUCH_TABLE, sqlException.getErrorCode());
            assertEquals(ER_NO_SUCH_TABLE_STATE, sqlException.getSQLState());
        }
    }

    @Test
    public void testNonUpdateableColumn() throws SQLException {
        createTestTable("vendor_code_test", "id int not null primary key auto_increment, test boolean");
        statement.execute("create or replace view vendor_code_test_view as select *, 1 as derived_column_that_does_no_exist from vendor_code_test");

        try {
            statement.executeQuery("UPDATE vendor_code_test_view SET derived_column_that_does_no_exist = 1");
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_NONUPDATEABLE_COLUMN, sqlException.getErrorCode());
            assertEquals(ER_NONUPDATEABLE_COLUMN_STATE, sqlException.getSQLState());
        }
    }

    @Test
    public void testParseErrorAddPartitionNoNewPartition() throws SQLException {
        try {
            statement.execute("totally_not_a_sql_command_this_cannot_be_parsed");
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_PARSE_ERROR, sqlException.getErrorCode());
            assertEquals(ER_PARSE_ERROR_STATE, sqlException.getSQLState());
        }
    }

    @Test
    public void testAddPartitionNoNewPartition() throws SQLException {
        createTestTable("vendor_code_test","a INT","PARTITION BY KEY (a) (PARTITION x0, PARTITION x1)");
        try {
            statement.execute("ALTER TABLE vendor_code_test ADD PARTITION PARTITIONS 0");
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_ADD_PARTITION_NO_NEW_PARTITION, sqlException.getErrorCode());
            assertEquals(ER_ADD_PARTITION_NO_NEW_PARTITION_STATE, sqlException.getSQLState());
        }
    }

    @Test
    public void testNoPartitionForGivenValue() throws SQLException {
        createTestTable("vendor_code_test","a INT","PARTITION BY LIST(a) (PARTITION p0 VALUES IN (1))");
        statement.execute("INSERT INTO vendor_code_test VALUES (1)");
        try {
            statement.execute("INSERT INTO vendor_code_test VALUES (2)");
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_NO_PARTITION_FOR_GIVEN_VALUE, sqlException.getErrorCode());
            assertEquals(ER_NO_PARTITION_FOR_GIVEN_VALUE_STATE, sqlException.getSQLState());
        }
    }

    @Test
    public void testLoadDataInvalidColumn() throws SQLException, UnsupportedEncodingException {
        statement.execute("drop view if exists v2");
        createTestTable("t1", "c1 INT, c2 VARCHAR(255)");
        statement.execute("CREATE VIEW v2 AS SELECT 1 + 2 AS c0, c1, c2 FROM t1;");

        MySQLStatement mysqlStatement;
        if (statement.isWrapperFor(org.mariadb.jdbc.MySQLStatement.class)) {
            mysqlStatement = statement.unwrap(org.mariadb.jdbc.MySQLStatement.class);
        } else {
            throw new RuntimeException("Mariadb JDBC adaptor must be used");
        }
        try {
            String data =
                    "\"1\", \"string1\"\n" +
                            "\"2\", \"string2\"\n" +
                            "\"3\", \"string3\"\n";
            ByteArrayInputStream loadDataInfileFile = new ByteArrayInputStream(data.getBytes("utf-8"));
            mysqlStatement.setLocalInfileInputStream(loadDataInfileFile);
            mysqlStatement.executeUpdate("LOAD DATA LOCAL INFILE 'dummyFileName' INTO TABLE v2 "
                    + "FIELDS ESCAPED BY '\\\\' "
                    + "TERMINATED BY ',' "
                    + "ENCLOSED BY '\"'"
                    + "LINES TERMINATED BY '\n' (c0, c2)");
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_LOAD_DATA_INVALID_COLUMN, sqlException.getErrorCode());
            assertEquals(ER_LOAD_DATA_INVALID_COLUMN_STATE, sqlException.getSQLState());
        }
    }
}
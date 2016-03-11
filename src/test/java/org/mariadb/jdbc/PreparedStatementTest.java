package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.sql.*;

import static org.junit.Assert.*;

public class PreparedStatementTest extends BaseTest {
    private static final int ER_NO_SUCH_TABLE = 1146;
    private static final String ER_NO_SUCH_TABLE_STATE = "42S02";

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("table1", "id1 int auto_increment primary key");
        createTable("table2", "id2 int auto_increment primary key");
        createTable("`testBigintTable`", "`id` bigint(20) unsigned NOT NULL, PRIMARY KEY (`id`)",
                "ENGINE=InnoDB DEFAULT CHARSET=utf8");
        createTable("`backTicksPreparedStatements`",
                "`id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                        + "`SLIndex#orBV#` text,"
                        + "`isM&M'sTasty?` bit(1) DEFAULT NULL,"
                        + "`Seems:LikeParam?` bit(1) DEFAULT NULL,"
                        + "`Webinar10-TM/ProjComp` text",
                 "ENGINE=InnoDB DEFAULT CHARSET=utf8");
        createTable("test_insert_select","`field1` varchar(20)");
        createTable("test_decimal_insert", "`field1` decimal(10, 7)");
    }

    @Test
    public void testClosingError() throws Exception {
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?");
        preparedStatement.close();
        preparedStatement.close();

    }

    /**
     * Conj-238.
     *
     * @throws SQLException exception
     */
    @org.junit.Test
    public void insertSelect() throws Exception {
        PreparedStatement stmt = sharedConnection.prepareStatement(
                "insert into test_insert_select ( field1) (select  TMP.field1 from (select ? `field1` from dual) TMP)");
        stmt.setString(1, "test");
        stmt.executeUpdate();
    }

    /**
     * Conj-90.
     *
     * @throws SQLException exception
     */
    @Test
    public void reexecuteStatementTest() throws SQLException {
        // set the allowMultiQueries parameter
        Connection connection = null;
        try {
            connection = setConnection("&allowMultiQueries=true");
            PreparedStatement stmt = connection.prepareStatement("SELECT 1");
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            rs = stmt.executeQuery();
            stmt.close();
        } finally {
            connection.close();
        }
    }

    @Test
    public void testNoSuchTableBatchUpdate() throws SQLException, UnsupportedEncodingException {
        sharedConnection.createStatement().execute("drop table if exists vendor_code_test");
        PreparedStatement preparedStatement = sharedConnection.prepareStatement(
                "/*CLIENT*/ INSERT INTO vendor_code_test VALUES(?)");
        preparedStatement.setString(1, "dummyValue");
        preparedStatement.addBatch();

        try {
            preparedStatement.executeBatch();
            fail("The above statement should result in an exception");
        } catch (SQLException sqlException) {
            assertEquals(ER_NO_SUCH_TABLE, sqlException.getErrorCode());
            assertEquals(ER_NO_SUCH_TABLE_STATE, sqlException.getSQLState());
        }
    }

    /**
     * CONJ-124: BigInteger not supported when setObject is used on PreparedStatements.
     *
     * @throws SQLException exception
     */
    @Test
    public void testBigInt() throws SQLException {
        Statement st = sharedConnection.createStatement();
        st.execute("INSERT INTO `testBigintTable` (`id`) VALUES (0)");
        PreparedStatement stmt = sharedConnection.prepareStatement("UPDATE `testBigintTable` SET `id` = ?");
        BigInteger bigT = BigInteger.valueOf(System.currentTimeMillis());
        stmt.setObject(1, bigT);
        stmt.executeUpdate();
        stmt = sharedConnection.prepareStatement("SELECT `id` FROM `testBigintTable` WHERE `id` = ?");
        stmt.setObject(1, bigT);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getBigDecimal(1).toBigInteger().compareTo(bigT));
    }

    /**
     * setObject should not truncate doubles.
     *
     * @throws SQLException exception
     */
    @Test
    public void testDoubleToDecimal() throws SQLException {
        PreparedStatement stmt = sharedConnection.prepareStatement("INSERT INTO test_decimal_insert (field1) VALUES (?)");
        Double value = 0.3456789;
        stmt.setObject(1, value, Types.DECIMAL, 7);
        stmt.executeUpdate();
        stmt = sharedConnection.prepareStatement("SELECT `field1` FROM test_decimal_insert");
        ResultSet rs = stmt.executeQuery();

        assertTrue(rs.next());
        assertEquals(value, rs.getDouble(1), 0.00000001);
    }

    @Test
    public void testPreparedStatementsWithQuotes() throws SQLException {


        String query = "INSERT INTO backTicksPreparedStatements (`SLIndex#orBV#`,`Seems:LikeParam?`,"
                + "`Webinar10-TM/ProjComp`,`isM&M'sTasty?`)"
                + " VALUES (?,?,?,?)";
        PreparedStatement ps = sharedConnection.prepareStatement(query);
        ps.setString(1, "slIndex");
        ps.setBoolean(2, false);
        ps.setString(3, "webinar10");
        ps.setBoolean(4, true);
        ps.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT `SLIndex#orBV#`,`Seems:LikeParam?`,"
                + "`Webinar10-TM/ProjComp`,`isM&M'sTasty?` FROM backTicksPreparedStatements");
        assertTrue(rs.next());
        assertEquals("slIndex", rs.getString(1));
        assertEquals(false, rs.getBoolean(2));
        assertEquals("webinar10", rs.getString(3));
        assertEquals(true, rs.getBoolean(4));
    }

    /**
     * CONJ-264: SQLException when calling PreparedStatement.executeBatch() without calling addBatch().
     *
     * @throws SQLException exception
     */
    @Test
    public void testExecuteBatch() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("INSERT INTO table1 VALUE ?");
        try {
            int[] result = preparedStatement.executeBatch();
            assertEquals(0, result.length);
        } catch (SQLException sqle) {
            fail("Must not throw error");
        }

    }


    /**
     * CONJ-263: Exception must be throwing exception if exception append in multiple query.
     *
     * @throws SQLException exception
     */
    @Test
    public void testCallExecuteErrorBatch() throws SQLException {
        PreparedStatement pstmt = sharedConnection.prepareStatement("SELECT 1;INSERT INTO INCORRECT_QUERY ;");
        try {
            pstmt.execute();
            fail("Must have thrown error");
        } catch (SQLException sqle) {
            //must have thrown error.
            assertTrue(sqle.getMessage().contains("INSERT INTO INCORRECT_QUERY"));
        }
    }

}

package org.mariadb.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

public class PreparedStatementTest extends BaseTest {
    private final static int ER_NO_SUCH_TABLE = 1146;
    private final String ER_NO_SUCH_TABLE_STATE = "42S02";
    private Statement statement;

    @Before
    public void setUp() throws SQLException {
        statement = connection.createStatement();
    }

    /**
     * CONJ-90
     *
     * @throws SQLException
     */
    @Test
    public void reexecuteStatementTest() throws SQLException {
        // set the allowMultiQueries parameter
        setConnection("&allowMultiQueries=true");
        PreparedStatement stmt = connection.prepareStatement("SELECT 1");
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        rs = stmt.executeQuery();
        stmt.close();
        connection.close();
    }

    @Test
    public void testNoSuchTableBatchUpdate() throws SQLException, UnsupportedEncodingException {
        statement.execute("drop table if exists vendor_code_test");
        PreparedStatement preparedStatement = connection.prepareStatement("/*CLIENT*/ INSERT INTO vendor_code_test VALUES(?)");
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

    @Test(expected = SQLException.class)
    public void testNoSuchTableBatchUpdateServer() throws SQLException, UnsupportedEncodingException {
        statement.execute("drop table if exists vendor_code_test");
        connection.prepareStatement("INSERT INTO vendor_code_test VALUES(?)");
    }

    /**
     * CONJ-124: BigInteger not supported when setObject is used on PreparedStatements.
     *
     * @throws SQLException
     */
    @Test
    public void testBigInt() throws SQLException {
        Statement st = connection.createStatement();
        createTestTable("`testBigintTable`","`id` bigint(20) unsigned NOT NULL, PRIMARY KEY (`id`)","ENGINE=InnoDB DEFAULT CHARSET=utf8");
        st.execute("INSERT INTO `testBigintTable` (`id`) VALUES (0)");
        PreparedStatement stmt = connection.prepareStatement("UPDATE `testBigintTable` SET `id` = ?");
        BigInteger bigT = BigInteger.valueOf(System.currentTimeMillis());
        stmt.setObject(1, bigT);
        stmt.executeUpdate();
        stmt = connection.prepareStatement("SELECT `id` FROM `testBigintTable` WHERE `id` = ?");
        stmt.setObject(1, bigT);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getBigDecimal(1).toBigInteger().compareTo(bigT));
    }

    @Test
    public void testPreparedStatementsWithQuotes() throws SQLException {
        createTestTable("`backTicksPreparedStatements`",
                          "`id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                        + "`SLIndex#orBV#` text,"
                        + "`isM&M'sTasty?` bit(1) DEFAULT NULL,"
                        + "`Seems:LikeParam?` bit(1) DEFAULT NULL,"
                        + "`Webinar10-TM/ProjComp` text"
                        ,"ENGINE=InnoDB DEFAULT CHARSET=utf8");

        String query = "INSERT INTO backTicksPreparedStatements (`SLIndex#orBV#`,`Seems:LikeParam?`,`Webinar10-TM/ProjComp`,`isM&M'sTasty?`)"
                + " VALUES (?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(query);
        ps.setString(1, "slIndex");
        ps.setBoolean(2, false);
        ps.setString(3, "webinar10");
        ps.setBoolean(4, true);
        ps.execute();
        ResultSet rs = connection.createStatement().executeQuery("SELECT `SLIndex#orBV#`,`Seems:LikeParam?`,`Webinar10-TM/ProjComp`,`isM&M'sTasty?` FROM backTicksPreparedStatements");
        assertTrue(rs.next());
        assertEquals("slIndex", rs.getString(1));
        assertEquals(false, rs.getBoolean(2));
        assertEquals("webinar10", rs.getString(3));
        assertEquals(true, rs.getBoolean(4));
    }

}

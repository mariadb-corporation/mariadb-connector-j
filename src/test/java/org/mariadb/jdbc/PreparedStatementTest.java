package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

public class PreparedStatementTest extends BaseTest {
	private Statement statement;
	private final static int ER_BAD_FIELD_ERROR       = 1054;
    private final String     ER_BAD_FIELD_ERROR_STATE = "42S22";
    private final static int ER_NON_INSERTABLE_TABLE       = 1471;
    private final String     ER_NON_INSERTABLE_TABLE_STATE = "HY000";
    private final static int ER_NO_SUCH_TABLE       = 1146;
    private final String     ER_NO_SUCH_TABLE_STATE = "42S02";
    private final static int ER_NONUPDATEABLE_COLUMN       = 1348;
    private final String     ER_NONUPDATEABLE_COLUMN_STATE = "HY000";
    private final static int ER_PARSE_ERROR       = 1064;
    private final String     ER_PARSE_ERROR_STATE = "42000";
    private final static int ER_NO_PARTITION_FOR_GIVEN_VALUE       = 1526;
    private final String     ER_NO_PARTITION_FOR_GIVEN_VALUE_STATE = "HY000";
    private final static int ER_LOAD_DATA_INVALID_COLUMN       = 1611;
    private final String     ER_LOAD_DATA_INVALID_COLUMN_STATE = "HY000";
    
    @Before
    public void setUp() throws SQLException {
    	statement = connection.createStatement();
    }
	
	/**
	 * CONJ-90
	 * @throws SQLException
	 */
	@Test
	public void reexecuteStatementTest() throws SQLException {
		connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&allowMultiQueries=true");
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
        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO vendor_code_test VALUES(?)");
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
}

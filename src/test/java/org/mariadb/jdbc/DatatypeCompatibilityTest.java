package org.mariadb.jdbc;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Arrays;

import static org.junit.Assert.*;

public class DatatypeCompatibilityTest extends BaseTest {

    @Test
    public void testIntegerTypes() throws SQLException {
        assertType("TINYINT", Integer.class, Types.TINYINT, "127", 127);
        assertType("TINYINT UNSIGNED", Integer.class, Types.TINYINT, "255", 255);
        assertType("SMALLINT", Integer.class, Types.SMALLINT, "0x7FFF", 0x7FFF);
        assertType("SMALLINT UNSIGNED", Integer.class, Types.SMALLINT, "0xFFFF", 0xFFFF);
        assertType("MEDIUMINT", Integer.class, Types.INTEGER, "0x7FFFFF", 0x7FFFFF);
        assertType("MEDIUMINT UNSIGNED", Integer.class, Types.INTEGER, "0xFFFFFF", 0xFFFFFF);
        assertType("INT", Integer.class, Types.INTEGER, "0x7FFFFFFF", 0x7FFFFFFF);
        assertType("INT UNSIGNED", Long.class, Types.INTEGER, "0xFFFFFFFF", 0xFFFFFFFFl);
        assertType("INTEGER", Integer.class, Types.INTEGER, "0x7FFFFFFF", 0x7FFFFFFF);
        assertType("INTEGER UNSIGNED", Long.class, Types.INTEGER, "0xFFFFFFFF", 0xFFFFFFFFl);
        assertType("BIGINT", Long.class, Types.BIGINT, "0x7FFFFFFFFFFFFFFF", Long.MAX_VALUE);
        assertType("BIGINT UNSIGNED", BigInteger.class, Types.BIGINT, "0xFFFFFFFFFFFFFFFF", new BigInteger("FFFFFFFFFFFFFFFF", 16));
    }

    @Test
    public void testFixedPointTypes() throws SQLException {
        requireMinimumVersion(5,0);
        assertType("DECIMAL(5,2)", BigDecimal.class, Types.DECIMAL, "-999.99", new BigDecimal(-99999).divide(new BigDecimal(100)));
        assertType("DECIMAL(5,2) UNSIGNED", BigDecimal.class, Types.DECIMAL, "999.99", new BigDecimal(99999).divide(new BigDecimal(100)));
        assertType("NUMERIC(5,2)", BigDecimal.class, Types.DECIMAL, "-999.99", new BigDecimal(-99999).divide(new BigDecimal(100))); // not Types.NUMERIC!
        assertType("NUMERIC(5,2) UNSIGNED", BigDecimal.class, Types.DECIMAL, "999.99", new BigDecimal(99999).divide(new BigDecimal(100))); // not Types.NUMERIC!
    }

    @Test
    public void testFloatingPointTypes() throws SQLException {
        assertType("FLOAT", Float.class, Types.REAL, "-1.0", -1.0f); // not Types.FLOAT!
        assertType("FLOAT UNSIGNED", Float.class, Types.REAL, "1.0", 1.0f); // not Types.FLOAT!
        assertType("DOUBLE", Double.class, Types.DOUBLE, "-1.0", -1.0d);
        assertType("DOUBLE UNSIGNED", Double.class, Types.DOUBLE, "1.0", 1.0d);
    }

    @Test
    public void testBitTypes() throws SQLException {
        requireMinimumVersion(5,0);
        assertType("BIT", Boolean.class, Types.BIT, "0", false);
        assertType("BIT(1)", Boolean.class, Types.BIT, "1", true);
        assertType("BIT(2)", byte[].class, Types.VARBINARY, "b'11'", new byte[]{3});
        assertType("BIT(8)", byte[].class, Types.VARBINARY, "b'11111111'", new byte[]{-1});
        assertType("BIT(16)", byte[].class, Types.VARBINARY, "b'1111111111111111'", new byte[]{-1, -1});
        assertType("BIT(24)", byte[].class, Types.VARBINARY, "b'111111111111111111111111'", new byte[]{-1, -1, -1});
        assertType("BIT(32)", byte[].class, Types.VARBINARY, "b'11111111111111111111111111111111'", new byte[]{-1, -1, -1, -1});
        assertType("BIT(64)", byte[].class, Types.VARBINARY, "b'1111111111111111111111111111111111111111111111111111111111111111'", new byte[]{-1, -1, -1, -1, -1, -1, -1, -1});
    }

    private void assertType(String columnType, Class expectedClass, int expectedJdbcType, String strValue, Object expectedObjectValue) throws SQLException {
        assertNotNull(expectedObjectValue);
        assertSame("bad test spec: ", expectedClass, expectedObjectValue.getClass());
        Statement statement = connection.createStatement();
        try {
            statement.execute("DROP TABLE IF EXISTS my_table");
            statement.execute("CREATE TABLE my_table (my_col " + columnType + ")");
            statement.execute("INSERT INTO my_table(my_col) VALUES (" + strValue + ")");
            statement.execute("SELECT * FROM my_table");
            ResultSet resultSet = statement.getResultSet();
            try {
                ResultSetMetaData metaData = resultSet.getMetaData();
                assertEquals("class name  for " + columnType, expectedClass.getName(), metaData.getColumnClassName(1));
                assertEquals("java.sql.Types code for " + columnType, expectedJdbcType, metaData.getColumnType(1));
                resultSet.next();
                Object objectValue = resultSet.getObject(1);
                assertEquals(expectedClass, objectValue.getClass());
                if (expectedClass.isArray()) {
                    assertTrue(Arrays.equals((byte[]) expectedObjectValue, (byte[]) objectValue));
                } else {
                    assertEquals(expectedObjectValue, objectValue);
                }
            } finally {
                resultSet.close();
            }
        } finally {
            statement.close();
        }
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void timeAsTimestamp() throws Exception{
       java.sql.Time aTime = new java.sql.Time(12,0,0);
       PreparedStatement ps =  connection.prepareStatement("SELECT ?");
       ps.setTime(1,aTime);
       ResultSet rs = ps.executeQuery();
       rs.next() ;
       Timestamp ts = rs.getTimestamp(1);
       Time time = rs.getTime(1);
       assertEquals(aTime,ts);
       assertEquals(aTime,time);
    }

}
package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Arrays;

import static org.junit.Assert.*;

public class DatatypeCompatibilityTest extends BaseTest {
    private static final String sql = "SELECT id, time_test FROM time_test;";

    /**
     * Initialization.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("pk_test", "val varchar(20), id1 int not null, id2 int not null,primary key(id1, id2)",
                "engine=innodb");
        createTable("datetime_test", "dt datetime");
        createTable("`manycols`",
                "  `tiny` tinyint(4) DEFAULT NULL,\n"
                        + "  `tiny_uns` tinyint(3) unsigned DEFAULT NULL,\n"
                        + "  `small` smallint(6) DEFAULT NULL,\n"
                        + "  `small_uns` smallint(5) unsigned DEFAULT NULL,\n"
                        + "  `medium` mediumint(9) DEFAULT NULL,\n"
                        + "  `medium_uns` mediumint(8) unsigned DEFAULT NULL,\n"
                        + "  `int_col` int(11) DEFAULT NULL,\n"
                        + "  `int_col_uns` int(10) unsigned DEFAULT NULL,\n"
                        + "  `big` bigint(20) DEFAULT NULL,\n"
                        + "  `big_uns` bigint(20) unsigned DEFAULT NULL,\n"
                        + "  `decimal_col` decimal(10,5) DEFAULT NULL,\n"
                        + "  `fcol` float DEFAULT NULL,\n"
                        + "  `fcol_uns` float unsigned DEFAULT NULL,\n"
                        + "  `dcol` double DEFAULT NULL,\n"
                        + "  `dcol_uns` double unsigned DEFAULT NULL,\n"
                        + "  `date_col` date DEFAULT NULL,\n"
                        + "  `time_col` time DEFAULT NULL,\n"
                        + "  `timestamp_col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE\n"
                        + "CURRENT_TIMESTAMP,\n"
                        + "  `year_col` year(4) DEFAULT NULL,\n"
                        + "  `bit_col` bit(5) DEFAULT NULL,\n"
                        + "  `char_col` char(5) DEFAULT NULL,\n"
                        + "  `varchar_col` varchar(10) DEFAULT NULL,\n"
                        + "  `binary_col` binary(10) DEFAULT NULL,\n"
                        + "  `varbinary_col` varbinary(10) DEFAULT NULL,\n"
                        + "  `tinyblob_col` tinyblob,\n"
                        + "  `blob_col` blob,\n"
                        + "  `mediumblob_col` mediumblob,\n"
                        + "  `longblob_col` longblob,\n"
                        + "  `text_col` text,\n"
                        + "  `mediumtext_col` mediumtext,\n"
                        + "  `longtext_col` longtext"
        );
        createTable("ytab", "y year");
        createTable("maxcharlength", "maxcharlength char(1)", "character set utf8");
        if (doPrecisionTest) {
            createTable("time_test", "ID int unsigned NOT NULL, time_test time(6), PRIMARY KEY (ID)", "engine=InnoDB");
            if (testSingleHost) {
                sharedConnection.createStatement().execute("insert into time_test(id, time_test) values(1, '00:00:00'), (2, '00:00:00.123'), (3, null)");
            }
        }
    }

    @Test
    public void testIntegerTypes() throws SQLException {
        assertType("TINYINT", Integer.class, Types.TINYINT, "127", 127);
        assertType("TINYINT UNSIGNED", Integer.class, Types.TINYINT, "255", 255);
        assertType("SMALLINT", Integer.class, Types.SMALLINT, "0x7FFF", 0x7FFF);
        assertType("SMALLINT UNSIGNED", Integer.class, Types.SMALLINT, "0xFFFF", 0xFFFF);
        assertType("MEDIUMINT", Integer.class, Types.INTEGER, "0x7FFFFF", 0x7FFFFF);
        assertType("MEDIUMINT UNSIGNED", Integer.class, Types.INTEGER, "0xFFFFFF", 0xFFFFFF);
        assertType("INT", Integer.class, Types.INTEGER, "0x7FFFFFFF", 0x7FFFFFFF);
        assertType("INT UNSIGNED", Long.class, Types.INTEGER, "0xFFFFFFFF", 0xFFFFFFFFL);
        assertType("INTEGER", Integer.class, Types.INTEGER, "0x7FFFFFFF", 0x7FFFFFFF);
        assertType("INTEGER UNSIGNED", Long.class, Types.INTEGER, "0xFFFFFFFF", 0xFFFFFFFFL);
        assertType("BIGINT", Long.class, Types.BIGINT, "0x7FFFFFFFFFFFFFFF", Long.MAX_VALUE);
        assertType("BIGINT UNSIGNED", BigInteger.class, Types.BIGINT, "0xFFFFFFFFFFFFFFFF",
                new BigInteger("FFFFFFFFFFFFFFFF", 16));
    }

    @Test
    public void testFixedPointTypes() throws SQLException {
        requireMinimumVersion(5, 0);
        assertType("DECIMAL(5,2)", BigDecimal.class, Types.DECIMAL, "-999.99", new BigDecimal(-99999)
                .divide(new BigDecimal(100)));
        assertType("DECIMAL(5,2) UNSIGNED", BigDecimal.class, Types.DECIMAL, "999.99", new BigDecimal(99999)
                .divide(new BigDecimal(100)));
        assertType("NUMERIC(5,2)", BigDecimal.class, Types.DECIMAL, "-999.99", new BigDecimal(-99999)
                .divide(new BigDecimal(100))); // not Types.NUMERIC!
        assertType("NUMERIC(5,2) UNSIGNED", BigDecimal.class, Types.DECIMAL, "999.99", new BigDecimal(99999)
                .divide(new BigDecimal(100))); // not Types.NUMERIC!
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
        requireMinimumVersion(5, 0);
        assertType("BIT", Boolean.class, Types.BIT, "0", false);
        assertType("BIT(1)", Boolean.class, Types.BIT, "1", true);
        assertType("BIT(2)", byte[].class, Types.VARBINARY, "b'11'", new byte[]{3});
        assertType("BIT(8)", byte[].class, Types.VARBINARY, "b'11111111'", new byte[]{-1});
        assertType("BIT(16)", byte[].class, Types.VARBINARY, "b'1111111111111111'", new byte[]{-1, -1});
        assertType("BIT(24)", byte[].class, Types.VARBINARY, "b'111111111111111111111111'", new byte[]{-1, -1, -1});
        assertType("BIT(32)", byte[].class, Types.VARBINARY, "b'11111111111111111111111111111111'",
                new byte[]{-1, -1, -1, -1});
        assertType("BIT(64)", byte[].class, Types.VARBINARY, "b'1111111111111111111111111111111111111111111111111111"
                + "111111111111'", new byte[]{-1, -1, -1, -1, -1, -1, -1, -1});
    }

    private void assertType(String columnType, Class expectedClass, int expectedJdbcType, String strValue,
                            Object expectedObjectValue) throws SQLException {
        assertNotNull(expectedObjectValue);
        assertSame("bad test spec: ", expectedClass, expectedObjectValue.getClass());
        Statement statement = sharedConnection.createStatement();
        try {
            createTable("my_table", "my_col " + columnType);
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

    @SuppressWarnings("deprecation")
    @Test
    public void timeAsTimestamp() throws Exception {
        Time testTime = new Time(12, 0, 0);
        PreparedStatement ps = sharedConnection.prepareStatement("SELECT CONVERT(?, TIME)");
        ps.setTime(1, testTime);
        ResultSet rs = ps.executeQuery();
        rs.next();
        Timestamp ts = rs.getTimestamp(1);
        Time time = rs.getTime(1);
        assertEquals(testTime, ts);
        assertEquals(testTime, time);
    }

    /**
     * Check Time getTime() answer using Statement.
     *
     * @param connection connection
     * @throws SQLException if any error occur
     */
    public void testStatementGetTime(Connection connection) throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("00:00:00", "" + resultSet.getTime(2));
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("00:00:00", "" + resultSet.getTime(2));
                Assert.assertTrue(resultSet.next());
                Assert.assertNull(resultSet.getTime(2));
                Assert.assertFalse(resultSet.next());
            }
        }
    }

    /**
     * Check Time getString() answer using Statement.
     *
     * @param connection connection
     * @throws SQLException if any error occur
     */
    public void testStatementGetString(Connection connection) throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("00:00:00.000000", resultSet.getString(2));
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("00:00:00.123000", resultSet.getString(2));
                Assert.assertTrue(resultSet.next());
                Assert.assertNull(resultSet.getString(2));
                Assert.assertFalse(resultSet.next());
            }
        }
    }

    /**
     * Check Time getTime() answer using prepareStatement.
     *
     * @param connection connection
     * @throws SQLException if any error occur
     */
    public void testPreparedStatementGetTime(Connection connection) throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("00:00:00", "" + resultSet.getTime(2));
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("00:00:00", "" + resultSet.getTime(2));
                Assert.assertTrue(resultSet.next());
                Assert.assertNull(resultSet.getTime(2));
                Assert.assertFalse(resultSet.next());
            }
        }
    }

    /**
     * Check Time getString() answer using prepareStatement.
     *
     * @param connection connection
     * @throws SQLException if any error occur
     */
    public void testPreparedStatementGetString(Connection connection) throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("00:00:00.000000", resultSet.getString(2));
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals("00:00:00.123000", resultSet.getString(2));
                Assert.assertTrue(resultSet.next());
                Assert.assertNull(resultSet.getString(2));
                Assert.assertFalse(resultSet.next());
            }
        }
    }

    @Test
    public void testTimePrepareStatement() throws SQLException {
        try (Connection connection = setConnection("&useServerPrepStmts=true")) {
            testStatementGetTime(connection);
            testPreparedStatementGetTime(connection);
            testStatementGetString(connection);
            testPreparedStatementGetString(connection);
        }
    }

    @Test
    public void testTimeNotPrepareStatement() throws SQLException {
        try (Connection connection = setConnection("&useServerPrepStmts=false")) {
            testStatementGetTime(connection);
            testPreparedStatementGetTime(connection);
            testStatementGetString(connection);
            testPreparedStatementGetString(connection);
        }
    }
}
package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class DatatypeTest extends BaseTest {

    private ResultSet resultSet;

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("Driverstreamtest", "id int not null primary key, strm text");
        createTable("Driverstreamtest2", "id int primary key not null, strm text");
        createTable("objecttest", "int_test int primary key not null, string_test varchar(30), "
                + "timestamp_test timestamp, serial_test blob");
        createTable("bintest", "id int not null primary key auto_increment, bin1 varbinary(300), bin2 varbinary(300)");
        createTable("bigdectest", "id int not null primary key auto_increment, bd decimal", "engine=innodb");
        createTable("bytetest", "id int not null primary key auto_increment, a int", "engine=innodb");
        createTable("shorttest", "id int not null primary key auto_increment,a int", "engine=innodb");
        createTable("doubletest", "id int not null primary key auto_increment,a double", "engine=innodb");
        createTable("bittest", "id int not null primary key auto_increment, b int");
        createTable("emptytest", "id int");
        createTable("test_setobjectconv", "id int not null primary key auto_increment, v1 varchar(40), v2 varchar(40)");
        createTable("blabla", "valsue varchar(20)");
    }

    @Before
    public void checkSupported() throws Exception {
        requireMinimumVersion(5, 0);
    }

    void checkClass(String column, Class<?> clazz, String mysqlType, int javaSqlType) throws Exception {
        int index = resultSet.findColumn(column);

        if (resultSet.getObject(column) != null) {
            assertEquals("Unexpected class for column " + column, clazz, resultSet.getObject(column).getClass());
        }
        assertEquals("Unexpected class name for column " + column, clazz.getName(),
                resultSet.getMetaData().getColumnClassName(index));
        assertEquals("Unexpected MySQL type for column " + column, mysqlType,
                resultSet.getMetaData().getColumnTypeName(index));
        assertEquals("Unexpected java sql type for column " + column, javaSqlType,
                resultSet.getMetaData().getColumnType(index));
    }

    /**
     * Testing different date parameters.
     * @param connection current connection
     * @param tinyInt1isBit tiny bit must be consider as boolean
     * @param yearIsDateType year must be consider as Date or int
     * @throws Exception exception
     */
    public void datatypes(Connection connection, boolean tinyInt1isBit, boolean yearIsDateType) throws Exception {

        createTable("datatypetest",
                "bit1 BIT(1) default 0,"
                        + "bit2 BIT(2) default 1,"
                        + "tinyint1 TINYINT(1) default 0,"
                        + "tinyint2 TINYINT(2) default 1,"
                        + "bool0 BOOL default 0,"
                        + "smallint0 SMALLINT default 1,"
                        + "smallint_unsigned SMALLINT UNSIGNED default 0,"
                        + "mediumint0 MEDIUMINT default 1,"
                        + "mediumint_unsigned MEDIUMINT UNSIGNED default 0,"
                        + "int0 INT default 1,"
                        + "int_unsigned INT UNSIGNED default 0,"
                        + "bigint0 BIGINT default 1,"
                        + "bigint_unsigned BIGINT UNSIGNED default 0,"
                        + "float0 FLOAT default 0,"
                        + "double0 DOUBLE default 1,"
                        + "decimal0 DECIMAL default 0,"
                        + "date0 DATE default '2001-01-01',"
                        + "datetime0 DATETIME default '2001-01-01 00:00:00',"
                        + "timestamp0 TIMESTAMP default  '2001-01-01 00:00:00',"
                        + "timestamp_zero TIMESTAMP,"
                        + "time0 TIME default '22:11:00',"
                        + ((!isMariadbServer() && minVersion(5, 6)) ? "year2 YEAR(4) default 99," : "year2 YEAR(2) default 99,")
                        + "year4 YEAR(4) default 2011,"
                        + "char0 CHAR(1) default '0',"
                        + "char_binary CHAR (1) binary default '0',"
                        + "varchar0 VARCHAR(1) default '1',"
                        + "varchar_binary VARCHAR(10) BINARY default 0x1,"
                        + "binary0 BINARY(10) default 0x1,"
                        + "varbinary0 VARBINARY(10) default 0x1,"
                        + "tinyblob0 TINYBLOB,"
                        + "tinytext0 TINYTEXT,"
                        + "blob0 BLOB,"
                        + "text0 TEXT,"
                        + "mediumblob0 MEDIUMBLOB,"
                        + "mediumtext0 MEDIUMTEXT,"
                        + "longblob0 LONGBLOB,"
                        + "longtext0 LONGTEXT,"
                        + "enum0 ENUM('a','b') default 'a',"
                        + "set0 SET('a','b') default 'a' ");


        connection.createStatement().execute("insert into datatypetest (tinyblob0,mediumblob0,blob0,longblob0,"
                + "tinytext0,mediumtext0,text0, longtext0) values(0x1,0x1,0x1,0x1, 'a', 'a', 'a', 'a')");

        resultSet = connection.createStatement().executeQuery("select * from datatypetest");
        resultSet.next();

        Class<?> byteArrayClass = (new byte[0]).getClass();

        checkClass("bit1", Boolean.class, "BIT", Types.BIT);
        checkClass("bit2", byteArrayClass, "BIT", Types.VARBINARY);

        checkClass("tinyint1",
                tinyInt1isBit ? Boolean.class : Integer.class, "TINYINT",
                tinyInt1isBit ? Types.BIT : Types.TINYINT);
        checkClass("tinyint2", Integer.class, "TINYINT", Types.TINYINT);
        checkClass("bool0", tinyInt1isBit ? Boolean.class : Integer.class, "TINYINT",
                tinyInt1isBit ? Types.BIT : Types.TINYINT);
        checkClass("smallint0", Integer.class, "SMALLINT", Types.SMALLINT);
        checkClass("smallint_unsigned", Integer.class, "SMALLINT UNSIGNED", Types.SMALLINT);
        checkClass("mediumint0", Integer.class, "MEDIUMINT", Types.INTEGER);
        checkClass("mediumint_unsigned", Integer.class, "MEDIUMINT UNSIGNED", Types.INTEGER);
        checkClass("int0", Integer.class, "INTEGER", Types.INTEGER);
        checkClass("int_unsigned", Long.class, "INTEGER UNSIGNED", Types.INTEGER);
        checkClass("bigint0", Long.class, "BIGINT", Types.BIGINT);
        checkClass("bigint_unsigned", BigInteger.class, "BIGINT UNSIGNED", Types.BIGINT);
        checkClass("float0", Float.class, "FLOAT", Types.REAL);
        checkClass("double0", Double.class, "DOUBLE", Types.DOUBLE);
        checkClass("decimal0", BigDecimal.class, "DECIMAL", Types.DECIMAL);
        checkClass("date0", java.sql.Date.class, "DATE", Types.DATE);
        checkClass("time0", java.sql.Time.class, "TIME", Types.TIME);
        checkClass("timestamp0", java.sql.Timestamp.class, "TIMESTAMP", Types.TIMESTAMP);
        checkClass("timestamp_zero", java.sql.Timestamp.class, "TIMESTAMP", Types.TIMESTAMP);

        if (isMariadbServer() || !minVersion(5, 6)) {
            //MySQL deprecated YEAR(2) since 5.6
            checkClass("year2",
                    yearIsDateType ? java.sql.Date.class : Short.class, "YEAR",
                    yearIsDateType ? Types.DATE : Types.SMALLINT);
        }
        checkClass("year4",
                yearIsDateType ? java.sql.Date.class : Short.class, "YEAR",
                yearIsDateType ? Types.DATE : Types.SMALLINT);
        checkClass("char0", String.class, "CHAR", Types.CHAR);
        checkClass("char_binary", String.class, "CHAR", Types.CHAR);
        checkClass("varchar0", String.class, "VARCHAR", Types.VARCHAR);
        checkClass("varchar_binary", String.class, "VARCHAR", Types.VARCHAR);
        checkClass("binary0", byteArrayClass, "BINARY", Types.BINARY);
        checkClass("varbinary0", byteArrayClass, "VARBINARY", Types.VARBINARY);
        checkClass("tinyblob0", byteArrayClass, "TINYBLOB", Types.VARBINARY);
        checkClass("tinytext0", String.class, "VARCHAR", Types.VARCHAR);
        checkClass("blob0", byteArrayClass, "BLOB", Types.VARBINARY);
        checkClass("text0", String.class, "VARCHAR", Types.VARCHAR);
        checkClass("mediumblob0", byteArrayClass, "MEDIUMBLOB", Types.VARBINARY);
        checkClass("mediumtext0", String.class, "VARCHAR", Types.VARCHAR);
        checkClass("longblob0", byteArrayClass, "LONGBLOB", Types.LONGVARBINARY);
        checkClass("longtext0", String.class, "VARCHAR", Types.LONGVARCHAR);
        checkClass("enum0", String.class, "CHAR", Types.CHAR);
        checkClass("set0", String.class, "CHAR", Types.CHAR);

        resultSet = connection.createStatement().executeQuery("select NULL as foo");
        resultSet.next();
        checkClass("foo", String.class, "NULL", Types.NULL);

    }

    @Test
    public void datatypes1() throws Exception {
        datatypes(sharedConnection, true, true);
    }

    @Test
    public void datatypes2() throws Exception {
        Connection connection = null;
        try {
            connection = setConnection("&tinyInt1isBit=0&yearIsDateType=0");
            datatypes(connection, false, false);
        } finally {
            connection.close();
        }
    }

    @Test
    public void datatypes3() throws Exception {
        Connection connection = null;
        try {
            connection = setConnection("&tinyInt1isBit=1&yearIsDateType=0");
            datatypes(connection, true, false);
        } finally {
            connection.close();
        }
    }

    @Test
    public void datatypes4() throws Exception {
        Connection connection = null;
        try {
            connection = setConnection("&tinyInt1isBit=0&yearIsDateType=1");
            datatypes(connection, false, true);
        } finally {
            connection.close();
        }
    }
















    @SuppressWarnings("deprecation")
    @Test
    public void testCharacterStreams() throws SQLException, IOException {
        PreparedStatement stmt = sharedConnection.prepareStatement(
                "insert into Driverstreamtest (id, strm) values (?,?)");
        stmt.setInt(1, 2);
        String toInsert = "abcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from Driverstreamtest");
        rs.next();
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(sb.toString(), (toInsert));
        rdr = rs.getCharacterStream(2);
        sb = new StringBuilder();

        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(sb.toString(), (toInsert));
        InputStream is = rs.getAsciiStream("strm");
        sb = new StringBuilder();

        while ((ch = is.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(sb.toString(), (toInsert));
        is = rs.getUnicodeStream("strm");
        sb = new StringBuilder();

        while ((ch = is.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(sb.toString(), (toInsert));
    }

    @Test
    public void testCharacterStreamWithLength() throws SQLException, IOException {
        PreparedStatement stmt = sharedConnection.prepareStatement(
                "insert into Driverstreamtest2 (id, strm) values (?,?)");
        stmt.setInt(1, 2);
        String toInsert = "abcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader, 5);
        stmt.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from Driverstreamtest2");
        rs.next();
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(sb.toString(), toInsert.substring(0, 5));
    }


    @Test
    public void testEmptyResultSet() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        assertEquals(true, stmt.execute("SELECT * FROM emptytest"));
        assertEquals(false, stmt.getResultSet().next());
    }

    @Test
    public void testLongColName() throws SQLException {
        DatabaseMetaData dbmd = sharedConnection.getMetaData();
        String str = "";
        for (int i = 0; i < dbmd.getMaxColumnNameLength(); i++) {
            str += "x";
        }
        createTable("longcol", str + " int not null primary key");
        sharedConnection.createStatement().execute("insert into longcol values (1)");
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from longcol");
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(str));
    }

    @Test(expected = SQLException.class)
    public void testBadParamlist() throws SQLException {
        PreparedStatement ps = null;
        ps = sharedConnection.prepareStatement("insert into blah values (?)");
        ps.execute();
    }

    @Test
    public void setobjectTest() throws SQLException, IOException, ClassNotFoundException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into objecttest values (?,?,?,?)");
        ps.setObject(1, 5);
        ps.setObject(2, "aaa");
        ps.setObject(3, Timestamp.valueOf("2009-01-17 15:41:01"));
        ps.setObject(4, new SerializableClass("testing", 8));
        ps.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from objecttest");
        if (rs.next()) {
            Object theInt = rs.getObject(1);
            assertTrue(theInt instanceof Integer);
            Object theInt2 = rs.getObject("int_test");
            assertTrue(theInt2 instanceof Integer);
            Object theString = rs.getObject(2);
            assertTrue(theString instanceof String);
            Object theTimestamp = rs.getObject(3);
            assertTrue(theTimestamp instanceof Timestamp);
            Object theBlob = rs.getObject(4);
            assertNotNull(theBlob);

            byte[] rawBytes = rs.getBytes(4);
            ByteArrayInputStream bais = new ByteArrayInputStream(rawBytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            SerializableClass sc = (SerializableClass) ois.readObject();

            assertEquals(sc.getVal(), "testing");
            assertEquals(sc.getVal2(), 8);
            rawBytes = rs.getBytes("serial_test");
            bais = new ByteArrayInputStream(rawBytes);
            ois = new ObjectInputStream(bais);
            sc = (SerializableClass) ois.readObject();

            assertEquals(sc.getVal(), "testing");
            assertEquals(sc.getVal2(), 8);
        } else {
            fail();
        }
    }

    @Test
    public void binTest() throws SQLException, IOException {
        byte[] allBytes = new byte[256];
        for (int i = 0; i < 256; i++) {
            allBytes[i] = (byte) (i & 0xff);
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(allBytes);
        PreparedStatement ps = sharedConnection.prepareStatement("insert into bintest (bin1,bin2) values (?,?)");
        ps.setBytes(1, allBytes);
        ps.setBinaryStream(2, bais);
        ps.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select bin1,bin2 from bintest");
        assertTrue(rs.next());
        rs.getBlob(1);
        InputStream is = rs.getBinaryStream(1);

        for (int i = 0; i < 256; i++) {
            int read = is.read();
            assertEquals(i, read);
        }
        is = rs.getBinaryStream(2);

        for (int i = 0; i < 256; i++) {
            int read = is.read();
            assertEquals(i, read);
        }

    }


    @Test
    public void binTest2() throws SQLException, IOException {


        if (sharedConnection.getMetaData().getDatabaseProductName().toLowerCase().equals("mysql")) {
            createTable("bintest2", "bin1 longblob", "engine=innodb");
        } else {
            createTable("bintest2", "id int not null primary key auto_increment, bin1 blob");
        }

        byte[] buf = new byte[1000000];
        for (int i = 0; i < 1000000; i++) {
            buf[i] = (byte) i;
        }
        InputStream is = new ByteArrayInputStream(buf);
        PreparedStatement ps = sharedConnection.prepareStatement("insert into bintest2 (bin1) values (?)");
        ps.setBinaryStream(1, is);
        ps.execute();
        ps = sharedConnection.prepareStatement("insert into bintest2 (bin1) values (?)");
        is = new ByteArrayInputStream(buf);
        ps.setBinaryStream(1, is);
        ps.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select bin1 from bintest2");
        if (rs.next()) {
            byte[] buf2 = rs.getBytes(1);
            for (int i = 0; i < 1000000; i++) {
                assertEquals((byte) i, buf2[i]);
            }

            if (rs.next()) {
                buf2 = rs.getBytes(1);
                for (int i = 0; i < 1000000; i++) {
                    assertEquals((byte) i, buf2[i]);
                }
                if (rs.next()) {
                    fail();
                }
            } else {
                fail();
            }
        } else {
            fail();
        }
    }

    @Test
    public void bigDecimalTest() throws SQLException {
        requireMinimumVersion(5, 0);
        BigDecimal bd = BigDecimal.TEN;
        PreparedStatement ps = sharedConnection.prepareStatement("insert into bigdectest (bd) values (?)");
        ps.setBigDecimal(1, bd);
        ps.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select bd from bigdectest");
        assertTrue(rs.next());
        Object bb = rs.getObject(1);
        assertEquals(bd, bb);
        BigDecimal bigD = rs.getBigDecimal(1);
        BigDecimal bigD2 = rs.getBigDecimal("bd");
        assertEquals(bd, bigD);
        assertEquals(bd, bigD2);
        bigD = rs.getBigDecimal("bd");
        assertEquals(bd, bigD);
    }


    @Test
    public void byteTest() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into bytetest (a) values (?)");
        ps.setByte(1, Byte.MAX_VALUE);
        ps.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select a from bytetest");
        assertTrue(rs.next());

        Byte bc = rs.getByte(1);
        Byte bc2 = rs.getByte("a");

        assertTrue(Byte.MAX_VALUE == bc);
        assertEquals(bc2, bc);


    }


    @Test
    public void shortTest() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into shorttest (a) values (?)");
        ps.setShort(1, Short.MAX_VALUE);
        ps.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select a from shorttest");
        assertTrue(rs.next());

        Short bc = rs.getShort(1);
        Short bc2 = rs.getShort("a");

        assertTrue(Short.MAX_VALUE == bc);
        assertEquals(bc2, bc);


    }

    @Test
    public void doubleTest() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into doubletest (a) values (?)");
        double sendDoubleValue = 1.5;
        ps.setDouble(1, sendDoubleValue);
        ps.execute();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select a from doubletest");
        assertTrue(rs.next());
        Object returnObject = rs.getObject(1);
        assertEquals(returnObject.getClass(), Double.class);
        Double bc = rs.getDouble(1);
        Double bc2 = rs.getDouble("a");

        assertTrue(sendDoubleValue == bc);
        assertEquals(bc2, bc);


    }

    @Test
    public void getUrlTest() throws SQLException {
        ResultSet rs = sharedConnection.createStatement().executeQuery("select 'http://mariadb.org' as url");
        rs.next();
        URL url = rs.getURL(1);
        assertEquals("http://mariadb.org", url.toString());
        url = rs.getURL("url");
        assertEquals("http://mariadb.org", url.toString());

    }

    @Test(expected = SQLException.class)
    public void getUrlFailTest() throws SQLException {
        ResultSet rs = sharedConnection.createStatement().executeQuery("select 'asdf' as url");
        rs.next();
        URL url = rs.getURL(1);
        assertNotNull(url);


    }

    @Test(expected = SQLException.class)
    public void getUrlFailTest2() throws SQLException {
        ResultSet rs = sharedConnection.createStatement().executeQuery("select 'asdf' as url");
        rs.next();
        URL url = rs.getURL("url");
        assertNotNull(url);
    }

    @Test
    public void setNull() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert blabla VALUE (?)");
        ps.setString(1, null);
    }

    @Test
    public void testSetObject() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into test_setobjectconv values (null, ?, ?)");
        ps.setObject(1, "2009-01-01 00:00:00", Types.TIMESTAMP);
        ps.setObject(2, "33", Types.DOUBLE);
        ps.execute();
    }

    @Test
    public void testBit() throws SQLException {
        PreparedStatement stmt = sharedConnection.prepareStatement("insert into bittest values(null, ?)");
        stmt.setBoolean(1, true);
        stmt.execute();
        stmt.setBoolean(1, false);
        stmt.execute();

        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from bittest");
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getBoolean("b"));
        Assert.assertTrue(rs.next());
        assertFalse(rs.getBoolean("b"));
        assertFalse(rs.next());
    }


}

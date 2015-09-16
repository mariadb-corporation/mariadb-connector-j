package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mariadb.jdbc.internal.common.queryresults.PrepareResult;
import org.mariadb.jdbc.internal.mysql.Protocol;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ServerPrepareStatementTest extends BaseTest {

    @Test
    public void ServerExecutionTest() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementTest");
        statement.execute("create table ServerPrepareStatementTest (id int not null primary key auto_increment, test boolean)");
        ResultSet rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
        int nbStatementCount = 0;
        rs.next();
        if (rs.first()) nbStatementCount = rs.getInt(2);

        PreparedStatement ps = connection.prepareStatement("INSERT INTO ServerPrepareStatementTest (test) VALUES (?)");
        ps.setBoolean(1, true);
        ps.addBatch();
        ps.execute();

        rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
        rs.next();
        assertTrue(rs.getInt(2) == nbStatementCount + 1);
    }

    @Test
    public void withoutParameterClientExecutionTest() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementTest");
        statement.execute("create table ServerPrepareStatementTest (id int not null primary key auto_increment, test boolean)");
        ResultSet rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
        int nbStatementCount = 0;
        rs.next();
        if (rs.first()) nbStatementCount = rs.getInt(2);

        PreparedStatement ps = connection.prepareStatement("INSERT INTO ServerPrepareStatementTest (test) VALUES (1)");
        ps.addBatch();
        ps.execute();

        rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
        rs.next();
        assertTrue(rs.getInt(2) == nbStatementCount);
    }

    @Test
    public void serverCacheStatementTest() throws Throwable {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementTestCache");
        statement.execute("create table ServerPrepareStatementTestCache (id int not null primary key auto_increment, test boolean)");

        Protocol protocol = getProtocolFromConnection(connection);
        int cacheSize = protocol.prepareStatementCache().size();
        connection.prepareStatement("INSERT INTO ServerPrepareStatementTestCache(test) VALUES (?)");
        assertTrue(cacheSize + 1 == protocol.prepareStatementCache().size());
        connection.prepareStatement("INSERT INTO ServerPrepareStatementTestCache(test) VALUES (?)");
        assertTrue(cacheSize + 1 == protocol.prepareStatementCache().size());
    }

    @Test
    public void prepStmtCacheSizeTest1() throws Throwable {
        BaseTest baseTest = new BaseTest();
        baseTest.setConnection("&prepStmtCacheSize=10");
        Connection connection2 = baseTest.connection;
        setConnection("&prepStmtCacheSize=10");

        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementCacheSize3");
        statement.execute("create table ServerPrepareStatementCacheSize3 (id int not null primary key auto_increment, test boolean)");

        ResultSet rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
        rs.next();
        int prepareServerStatement = rs.getInt(2);
        log.debug("server side : " + prepareServerStatement);

        PreparedStatement ps1 = connection2.prepareStatement("INSERT INTO ServerPrepareStatementCacheSize3(test) VALUES (?)");
        rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
        rs.next();
        int prepareServerStatement2 = rs.getInt(2);
        log.debug("server side before closing: " + prepareServerStatement2);
        assertTrue(prepareServerStatement2 == prepareServerStatement + 1);
        connection2.close();

        rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
        rs.next();
        int prepareServerStatement3 = rs.getInt(2);
        log.debug("server side after closing: " + prepareServerStatement3);

        assertTrue(prepareServerStatement3 == prepareServerStatement);
    }


    @Test
    public void prepStmtCacheSizeTest() throws Throwable {
        setConnection("&prepStmtCacheSize=10");
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementCacheSize");
        statement.execute("create table ServerPrepareStatementCacheSize (id int not null primary key auto_increment, test int)");

        PreparedStatement[] sts = new PreparedStatement[20];
        for (int i = 0; i < 20; i++) {
            String sql = "INSERT INTO ServerPrepareStatementCacheSize(id, test) VALUES (" + (i + 1) + ",?)";
            log.debug(sql);
            sts[i] = connection.prepareStatement(sql);
        }
        //check max cache size
        Protocol protocol = getProtocolFromConnection(connection);
        assertTrue(protocol.prepareStatementCache().size() == 10);

        //check all prepared statement worked even if not cached
        for (int i = 0; i < 20; i++) {
            sts[i].setInt(1, i);
            sts[i].addBatch();
            sts[i].execute();
        }
        assertTrue(protocol.prepareStatementCache().size() == 10);
        for (int i = 0; i < 20; i++) {
            sts[i].close();
        }
        assertTrue(protocol.prepareStatementCache().size() == 0);
    }


    @Test
    public void timeFractionnalSecondTest() throws SQLException {
        setConnection("&useFractionalSeconds=false");
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists preparetestFactionnal");
        statement.execute("CREATE TABLE preparetestFactionnal (time0 TIME(6) default '22:11:00')");

        Time time0 = new Time(55549392);
        Time time1 = new Time(55549000);

        PreparedStatement ps = connection.prepareStatement("INSERT INTO preparetestFactionnal (time0) VALUES (?)");
        ps.setTime(1, time0);
        ps.addBatch();
        ps.setTime(1, time1);
        ps.addBatch();
        ps.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT * from preparetestFactionnal");
        rs.next();
        //must be equal to time1 and not time0
        assertEquals(rs.getTime(1), time1);
        rs.next();
        assertEquals(rs.getTime(1), time1);

    }

    @Test
    public void dataConformityTest() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists preparetest");
        statement.execute("CREATE TABLE preparetest (" +
                "bit1 BIT(1)," +
                "bit2 BIT(2)," +
                "tinyint1 TINYINT(1)," +
                "tinyint2 TINYINT(2)," +
                "bool0 BOOL default 1," +
                "smallint0 SMALLINT default 1," +
                "smallint_unsigned SMALLINT UNSIGNED default 0," +
                "mediumint0 MEDIUMINT default 1," +
                "mediumint_unsigned MEDIUMINT UNSIGNED default 0," +
                "int0 INT default 1," +
                "int_unsigned INT UNSIGNED default 0," +
                "bigint0 BIGINT default 1," +
                "bigint_unsigned BIGINT UNSIGNED default 0," +
                "float0 FLOAT default 0," +
                "double0 DOUBLE default 1," +
                "decimal0 DECIMAL default 0," +
                "decimal1 DECIMAL(15,4) default 0," +
                "date0 DATE default '2001-01-01'," +
                "datetime0 DATETIME(6) default '2001-01-01 00:00:00'," +
                "timestamp0 TIMESTAMP(6) default  '2001-01-01 00:00:00'," +
                "timestamp1 TIMESTAMP(0) default  '2001-01-01 00:00:00'," +
                "timestamp_zero TIMESTAMP  null, " +
                "time0 TIME(6) default '22:11:00'," +
                "year2 YEAR(2) default 99," +
                "year4 YEAR(4) default 2011," +
                "char0 CHAR(1) default '0'," +
                "char_binary CHAR (1) binary default '0'," +
                "varchar0 VARCHAR(1) default '1'," +
                "varchar_binary VARCHAR(10) BINARY default 0x1," +
                "binary0 BINARY(10) default 0x1," +
                "varbinary0 VARBINARY(10) default 0x1" +
                ")");

        boolean bit1 = Boolean.FALSE;
        byte bit2 = (byte) 3;
        byte tinyint1 = (byte) 127;
        short tinyint2 = 127;
        boolean bool0 = Boolean.FALSE;
        short smallint0 = 5;
        short smallint_unsigned = Short.MAX_VALUE;
        int mediumint0 = 55000;
        int mediumint_unsigned = 55000;
        int int0 = Integer.MAX_VALUE;
        int int_unsigned = Integer.MAX_VALUE;
        long bigint0 = 5000l;
        BigInteger bigint_unsigned = new BigInteger("3147483647");
        float float0 = 3147483647.7527F;
        double double0 = 3147483647.8527D;
        BigDecimal decimal0 = new BigDecimal("3147483647");
        BigDecimal decimal1 = new BigDecimal("3147483647.9527");
        System.out.println(System.currentTimeMillis());
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+00:00"));
        Date date0 = new Date(1441238400000l);

        Timestamp datetime0 = new Timestamp(-2124690212000l);
        datetime0.setNanos(392005000);
        Timestamp timestamp0 = new Timestamp(1441290349000l);
        timestamp0.setNanos(392005000);
        Timestamp timestamp1 = new Timestamp(1441290349000l);
        Time time0 = new Time(55549392);
        short year2 = 30;
        int year4 = 2050;

        String char0 = "\n";
        String char_binary = "\n";
        String varchar0 = "\b";
        String varchar_binary = "\b";
        byte[] binary0 = "1234567890".getBytes();
        byte[] varbinary0 = "azerty".getBytes();
        PreparedStatement ps = connection.prepareStatement("INSERT INTO preparetest (bit1,bit2,tinyint1,tinyint2,bool0,smallint0,smallint_unsigned,mediumint0,mediumint_unsigned,int0," +
                "int_unsigned,bigint0,bigint_unsigned, float0, double0, decimal0,decimal1, date0,datetime0, timestamp0,timestamp1,timestamp_zero, time0," +
                "year2,year4,char0, char_binary, varchar0, varchar_binary, binary0, varbinary0)  " +
                "VALUES (?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?)");
        ps.setBoolean(1, bit1);
        ps.setByte(2, bit2);
        ps.setByte(3, tinyint1);
        ps.setShort(4, tinyint2);
        ps.setBoolean(5, bool0);
        ps.setShort(6, smallint0);
        ps.setShort(7, smallint_unsigned);
        ps.setInt(8, mediumint0);
        ps.setInt(9, mediumint_unsigned);
        ps.setInt(10, int0);
        ps.setInt(11, int_unsigned);
        ps.setLong(12, bigint0);
        ps.setObject(13, bigint_unsigned);
        ps.setFloat(14, float0);
        ps.setDouble(15, double0);
        ps.setBigDecimal(16, decimal0);
        ps.setBigDecimal(17, decimal1);
        ps.setDate(18, date0);
        ps.setTimestamp(19, datetime0);
        ps.setTimestamp(20, timestamp0);
        ps.setTimestamp(21, timestamp1);
        ps.setTimestamp(22, null);
        ps.setTime(23, time0);
        ps.setShort(24, year2);
        ps.setInt(25, year4);
        ps.setObject(26, char0, java.sql.Types.CHAR);
        ps.setString(27, char_binary);
        ps.setString(28, varchar0);
        ps.setString(29, varchar_binary);
        ps.setBytes(30, binary0);
        ps.setBytes(31, varbinary0);

        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = connection.createStatement().executeQuery("SELECT * from preparetest");
        rs.next();
        assertEquals(rs.getBoolean(1), bit1);
        assertEquals(rs.getByte(2), bit2);
        assertEquals(rs.getByte(3), tinyint1);
        assertEquals(rs.getShort(4), tinyint2);
        assertEquals(rs.getBoolean(5), bool0);
        assertEquals(rs.getShort(6), smallint0);
        assertEquals(rs.getShort(7), smallint_unsigned);
        assertEquals(rs.getInt(8), mediumint0);
        assertEquals(rs.getInt(9), mediumint_unsigned);
        assertEquals(rs.getInt(10), int0);
        assertEquals(rs.getInt(11), int_unsigned);
        assertEquals(rs.getInt(12), bigint0);
        assertEquals(rs.getObject(13), bigint_unsigned);
        assertEquals(rs.getFloat(14), float0, 10000);
        assertEquals(rs.getDouble(15), double0, 10000);
        assertEquals(rs.getBigDecimal(16), decimal0);
        assertEquals(rs.getBigDecimal(17), decimal1);
        System.out.println("date0 : " + date0.getTime());
        System.out.println("rs.getDate(18) : " + rs.getDate(18).getTime());
        assertEquals(rs.getDate(18), date0);
        assertEquals(rs.getTimestamp(19), datetime0);
        assertEquals(rs.getTimestamp(20), timestamp0);
        assertEquals(rs.getTimestamp(21), timestamp1);
        assertNull(rs.getTimestamp(22));
        assertEquals(rs.getTime(23), time0);
        assertEquals(rs.getInt(24), year2);
        assertEquals(rs.getInt(25), year4);
        assertEquals(rs.getString(26), char0);
        assertEquals(rs.getString(27), char_binary);
        assertEquals(rs.getString(28), varchar0);
        assertEquals(rs.getString(29), varchar_binary);


        assertEquals(new String(rs.getBytes(30), StandardCharsets.UTF_8), new String(binary0, StandardCharsets.UTF_8));
        assertEquals(new String(rs.getBytes(31), StandardCharsets.UTF_8), new String(varbinary0, StandardCharsets.UTF_8));

    }


    @Test
    public void checkReusability() throws Throwable {
        setConnection("&prepStmtCacheSize=10");
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementCacheSize2");
        statement.execute("create table ServerPrepareStatementCacheSize2 (id int not null primary key auto_increment, test boolean)");

        ExecutorService exec = Executors.newFixedThreadPool(2);

        //check blacklist shared
        exec.execute(new CreatePrepareDouble("INSERT INTO ServerPrepareStatementCacheSize2( test) VALUES (?)", connection, 100, 100));
        exec.execute(new CreatePrepareDouble("INSERT INTO ServerPrepareStatementCacheSize2( test) VALUES (?)", connection, 500, 100));
        //wait for thread endings
        exec.shutdown();
        try {
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
        }
    }

    @Test
    public void blobTest() throws Throwable {
        BaseTest baseTest = new BaseTest();
        baseTest.setConnection("&prepStmtCacheSize=10");

        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementCacheSize3");
        statement.execute("create table ServerPrepareStatementCacheSize3 (id int not null primary key auto_increment, test blob)");

        PreparedStatement ps = connection.prepareStatement("INSERT INTO ServerPrepareStatementCacheSize3(test) VALUES (?)");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("logback.xml");

        ps.setBlob(1, input);
        ps.addBatch();
        ps.executeBatch();
    }

    @Test
    public void readerTest() throws Throwable {
        BaseTest baseTest = new BaseTest();
        baseTest.setConnection("&prepStmtCacheSize=10");

        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementCacheSize3");
        statement.execute("create table ServerPrepareStatementCacheSize3 (id int not null primary key auto_increment, test blob)");

        PreparedStatement ps = connection.prepareStatement("INSERT INTO ServerPrepareStatementCacheSize3(test) VALUES (?)");
        Reader reader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("logback.xml")));

        ps.setCharacterStream(1, reader);
        ps.addBatch();
        ps.executeBatch();
    }

    @Test(expected = SQLException.class)
    public void parametersNotSetTest() throws Throwable {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementParameters");
        statement.execute("create table ServerPrepareStatementParameters (id int, id2 int)");

        PreparedStatement ps = connection.prepareStatement("INSERT INTO ServerPrepareStatementParameters(id, id2) VALUES (?,?)");
        ps.setInt(1, 1);
        ps.addBatch();
    }

    @Test
    public void checkSendDifferentParameterTypeTest() throws Throwable {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementParameters");
        statement.execute("create table ServerPrepareStatementParameters (id int, id2 int)");
        PreparedStatement ps = connection.prepareStatement("INSERT INTO ServerPrepareStatementParameters(id, id2) VALUES (?,?)");
        ps.setByte(1, (byte) 1);
        ps.setShort(2, (short) 1);
        ps.addBatch();
        ps.setInt(1, Integer.MIN_VALUE);
        ps.setInt(2, Integer.MAX_VALUE);
        ps.addBatch();
        ps.setInt(1, Integer.MIN_VALUE);
        ps.setInt(2, Integer.MAX_VALUE);
        ps.addBatch();
        ps.execute();
    }

    @Test
    public void blobMultipleSizeTest() throws Throwable {
        Assume.assumeTrue(checkMaxAllowedPacketMore40m("blobMultipleSizeTest"));

        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementCacheSize4");
        statement.execute("create table ServerPrepareStatementCacheSize4 (id int not null primary key auto_increment, test LONGBLOB) ROW_FORMAT=COMPRESSED ENGINE=INNODB");

        PreparedStatement ps = connection.prepareStatement("INSERT INTO ServerPrepareStatementCacheSize4(test) VALUES (?)");
        byte[] arr = new byte[20000000];
        Arrays.fill(arr, (byte) 'b');
        InputStream input = new ByteArrayInputStream(arr);
        InputStream input2 = new ByteArrayInputStream(arr);
        InputStream input3 = new ByteArrayInputStream(arr);

        ps.setBlob(1, input);
        ps.addBatch();
        ps.setBlob(1, input2);
        ps.addBatch();
        ps.setBlob(1, input3);
        ps.addBatch();
        ps.executeBatch();

        ResultSet rs = statement.executeQuery("select * from ServerPrepareStatementCacheSize4");
        rs.next();
        byte[] newBytes = rs.getBytes(2);
        assertEquals(arr.length, newBytes.length);
        for (int i = 0; i < arr.length; i++) {
            assertEquals(arr[i], newBytes[i]);
        }
    }

    @Test
    public void executeNumber() throws Throwable {
        PreparedStatement ps = prepareInsert();
        ps.execute();
        ResultSet rs = ps.executeQuery("select count(*) from ServerPrepareStatementParameters");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 1);
    }

    @Test
    public void executeBatchNumber() throws Throwable {
        PreparedStatement ps = prepareInsert();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select count(*) from ServerPrepareStatementParameters");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 3);
    }

    private PreparedStatement prepareInsert() throws Throwable {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementParameters");
        statement.execute("create table ServerPrepareStatementParameters (id int, id2 int)");
        PreparedStatement ps = connection.prepareStatement("INSERT INTO ServerPrepareStatementParameters(id, id2) VALUES (?,?)");
        ps.setByte(1, (byte) 1);
        ps.setShort(2, (short) 1);
        ps.addBatch();
        ps.setInt(1, Integer.MIN_VALUE);
        ps.setInt(2, Integer.MAX_VALUE);
        ps.addBatch();
        ps.setInt(1, Integer.MIN_VALUE);
        ps.setInt(2, Integer.MAX_VALUE);
        ps.addBatch();
        return ps;
    }

    @Test
    public void directExecuteNumber() throws Throwable {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementParameters");
        statement.execute("create table ServerPrepareStatementParameters (id int, id2 int)");
        PreparedStatement ps = connection.prepareStatement("INSERT INTO ServerPrepareStatementParameters(id, id2) VALUES (?,?)");
        ps.setByte(1, (byte) 1);
        ps.setShort(2, (short) 1);
        ps.execute();
        ResultSet rs = ps.executeQuery("select count(*) from ServerPrepareStatementParameters");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 1);
    }

    @Test
    public void directExecuteNumber2() throws Throwable {
        connection.createStatement().execute("drop table if exists streamtest2");
        connection.createStatement().execute("create table streamtest2 (id int primary key not null, strm text)");
        PreparedStatement stmt = connection.prepareStatement("insert into streamtest2 (id, strm) values (?,?)");
        stmt.setInt(1, 2);
        String toInsert = "\u00D8abcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader);
        stmt.execute();
        ResultSet rs = connection.createStatement().executeQuery("select * from streamtest2");
        rs.next();
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while ((ch = rdr.read()) != -1) {
            sb.append((char) ch);
        }
        assertEquals(toInsert, sb.toString());
    }

    @Test
    public void dataConformityTest2() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists preparetest");
        statement.execute("CREATE TABLE preparetest (" +
                "bit1 BIT(1)," +
                "bit2 BIT(2)," +
                "tinyint1 TINYINT(1)," +
                "tinyint2 TINYINT(2)," +
                "bool0 BOOL default 1," +
                "smallint0 SMALLINT default 1," +
                "smallint_unsigned SMALLINT UNSIGNED default 0," +
                "mediumint0 MEDIUMINT default 1," +
                "mediumint_unsigned MEDIUMINT UNSIGNED default 0," +
                "int0 INT default 1," +
                "int_unsigned INT UNSIGNED default 0," +
                "bigint0 BIGINT default 1," +
                "bigint_unsigned BIGINT UNSIGNED default 0," +
                "float0 FLOAT default 0," +
                "double0 DOUBLE default 1," +
                "decimal0 DECIMAL default 0," +
                "decimal1 DECIMAL(15,4) default 0," +
                "date0 DATE default '2001-01-01'," +
                "datetime0 DATETIME(6) default '2001-01-01 00:00:00'," +
                "timestamp0 TIMESTAMP(6) default  '2001-01-01 00:00:00'," +
                "timestamp1 TIMESTAMP(0) default  '2001-01-01 00:00:00'," +
                "timestamp_zero TIMESTAMP  null, " +
                "time0 TIME(6) default '22:11:00'," +
                "year2 YEAR(2) default 99," +
                "year4 YEAR(4) default 2011," +
                "char0 CHAR(1) default '0'," +
                "char_binary CHAR (1) binary default '0'," +
                "varchar0 VARCHAR(1) default '1'," +
                "varchar_binary VARCHAR(10) BINARY default 0x1," +
                "binary0 BINARY(10) default 0x1," +
                "varbinary0 VARBINARY(10) default 0x1" +
                ")");

        boolean bit1 = Boolean.FALSE;
        byte bit2 = (byte) 3;
        byte tinyint1 = (byte) 127;
        short tinyint2 = 127;
        boolean bool0 = Boolean.FALSE;
        short smallint0 = 5;
        short smallint_unsigned = Short.MAX_VALUE;
        int mediumint0 = 55000;
        int mediumint_unsigned = 55000;
        int int0 = Integer.MAX_VALUE;
        int int_unsigned = Integer.MAX_VALUE;
        long bigint0 = 5000l;
        BigInteger bigint_unsigned = new BigInteger("3147483647");
        float float0 = 3147483647.7527F;
        double double0 = 3147483647.8527D;
        BigDecimal decimal0 = new BigDecimal("3147483647");
        BigDecimal decimal1 = new BigDecimal("3147483647.9527");
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+00:00"));
        Date date0 = new Date(1441238400000l);
        Timestamp datetime0 = new Timestamp(-2124690212000l);
        datetime0.setNanos(392005000);
        Timestamp timestamp0 = new Timestamp(1441290349000l);
        timestamp0.setNanos(392005000);
        Timestamp timestamp1 = new Timestamp(1441290349000l);
        Time time0 = new Time(55549392);
        short year2 = 30;
        int year4 = 2050;

        String char0 = "\n";
        String char_binary = "\n";
        String varchar0 = "\b";
        String varchar_binary = "\b";
        byte[] binary0 = "1234567890".getBytes();
        byte[] varbinary0 = "azerty".getBytes();
        PreparedStatement ps = connection.prepareStatement("INSERT INTO preparetest (bit1,bit2,tinyint1,tinyint2,bool0,smallint0,smallint_unsigned,mediumint0,mediumint_unsigned,int0," +
                "int_unsigned,bigint0,bigint_unsigned, float0, double0, decimal0,decimal1, date0,datetime0, timestamp0,timestamp1,timestamp_zero, time0," +
                "year2,year4,char0, char_binary, varchar0, varchar_binary, binary0, varbinary0)  " +
                "VALUES (?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?)");
        ps.setBoolean(1, bit1);
        ps.setByte(2, bit2);
        ps.setByte(3, tinyint1);
        ps.setShort(4, tinyint2);
        ps.setBoolean(5, bool0);
        ps.setShort(6, smallint0);
        ps.setShort(7, smallint_unsigned);
        ps.setInt(8, mediumint0);
        ps.setInt(9, mediumint_unsigned);
        ps.setInt(10, int0);
        ps.setInt(11, int_unsigned);
        ps.setLong(12, bigint0);
        ps.setObject(13, bigint_unsigned);
        ps.setFloat(14, float0);
        ps.setDouble(15, double0);
        ps.setBigDecimal(16, decimal0);
        ps.setBigDecimal(17, decimal1);
        ps.setDate(18, date0);
        ps.setTimestamp(19, datetime0);
        ps.setTimestamp(20, timestamp0);
        ps.setTimestamp(21, timestamp1);
        ps.setTimestamp(22, null);
        ps.setTime(23, time0);
        ps.setShort(24, year2);
        ps.setInt(25, year4);
        ps.setString(26, char0);
        ps.setString(27, char_binary);
        ps.setString(28, varchar0);
        ps.setString(29, varchar_binary);
        ps.setBytes(30, binary0);
        ps.setBytes(31, varbinary0);

        ps.addBatch();
        ps.executeBatch();

        PreparedStatement prepStmt = connection.prepareStatement("SELECT * from preparetest where bit1 = ?");
        prepStmt.setBoolean(1, false);
        ResultSet rs = prepStmt.executeQuery();
        rs.next();
        assertEquals(rs.getBoolean(1), bit1);
        assertEquals(rs.getByte(2), bit2);
        assertEquals(rs.getByte(3), tinyint1);
        assertEquals(rs.getShort(4), tinyint2);
        assertEquals(rs.getBoolean(5), bool0);
        assertEquals(rs.getShort(6), smallint0);
        assertEquals(rs.getShort(7), smallint_unsigned);
        assertEquals(rs.getInt(8), mediumint0);
        assertEquals(rs.getInt(9), mediumint_unsigned);
        assertEquals(rs.getInt(10), int0);
        assertEquals(rs.getInt(11), int_unsigned);
        assertEquals(rs.getInt(12), bigint0);
        assertEquals(rs.getObject(13), bigint_unsigned);
        assertEquals(rs.getFloat(14), float0, 10000);
        assertEquals(rs.getDouble(15), double0, 10000);
        assertEquals(rs.getBigDecimal(16), decimal0);
        assertEquals(rs.getBigDecimal(17), decimal1);
        assertEquals(rs.getDate(18), date0);
        assertEquals(rs.getTimestamp(19), datetime0);
        assertEquals(rs.getTimestamp(20), timestamp0);
        assertEquals(rs.getTimestamp(21), timestamp1);
        assertNull(rs.getTimestamp(22));
        assertEquals(rs.getTime(23), time0);
        assertEquals(rs.getInt(24), year2);
        assertEquals(rs.getInt(25), year4);
        assertEquals(rs.getString(26), char0);
        assertEquals(rs.getString(27), char_binary);
        assertEquals(rs.getString(28), varchar0);
        assertEquals(rs.getString(29), varchar_binary);

        assertEquals(new String(rs.getBytes(30), StandardCharsets.UTF_8), new String(binary0, StandardCharsets.UTF_8));
        assertEquals(new String(rs.getBytes(31), StandardCharsets.UTF_8), new String(varbinary0, StandardCharsets.UTF_8));

    }

    protected class CreatePrepareDouble implements Runnable {
        String sql;
        Connection connection;
        long firstWaitTime;
        long secondWaitTime;


        public CreatePrepareDouble(String sql, Connection connection, long firstWaitTime, long secondWaitTime) {
            this.sql = sql;
            this.connection = connection;
            this.firstWaitTime = firstWaitTime;
            this.secondWaitTime = secondWaitTime;
        }

        public void run() {
            try {
                Protocol protocol = getProtocolFromConnection(connection);
                log.debug("before : contain key : " + protocol.prepareStatementCache().containsKey(sql));
                if (protocol.prepareStatementCache().containsKey(sql)) {
                    PrepareResult ps = protocol.prepareStatementCache().get(sql);
                    log.debug("before : ps : " + ps.getUseTime());
                }
                PreparedStatement ps = connection.prepareStatement(sql);
                log.debug("after : contain key : " + protocol.prepareStatementCache().containsKey(sql));
                if (protocol.prepareStatementCache().containsKey(sql)) {
                    PrepareResult ps2 = protocol.prepareStatementCache().get(sql);
                    log.debug("after : ps : " + ps2.getUseTime());
                }
                Thread.sleep(firstWaitTime);
                ps.setBoolean(1, true);
                ps.addBatch();
                ps.executeBatch();
                Thread.sleep(secondWaitTime);
                ps.close();
                log.debug("after close : contain key : " + protocol.prepareStatementCache().containsKey(sql));
                if (protocol.prepareStatementCache().containsKey(sql)) {
                    PrepareResult ps2 = protocol.prepareStatementCache().get(sql);
                    log.debug("after close : ps : " + ps2.getUseTime());
                }
            } catch (Throwable e) {
                fail();
            }
        }
    }
}

package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.internal.common.queryresults.PrepareResult;
import org.mariadb.jdbc.internal.mysql.MySQLProtocol;
import org.mariadb.jdbc.internal.mysql.MySQLType;
import org.mariadb.jdbc.internal.mysql.Protocol;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class ServerPrepareStatementTest extends BaseTest {

/*
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
        int cacheSize = protocol.prepareStatementCache().size() ;
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
        int prepareServerStatement =  rs.getInt(2);
        log.debug("server side : " + prepareServerStatement);

        PreparedStatement ps1 = connection2.prepareStatement("INSERT INTO ServerPrepareStatementCacheSize3(test) VALUES (?)");
        rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
        rs.next();
        int prepareServerStatement2 =  rs.getInt(2);
        log.debug("server side before closing: " + prepareServerStatement2);
        assertTrue(prepareServerStatement2 == prepareServerStatement + 1);
        connection2.close();

        rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
        rs.next();
        int prepareServerStatement3 =  rs.getInt(2);
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
        for (int i=0; i< 20; i++) {
            String sql = "INSERT INTO ServerPrepareStatementCacheSize(id, test) VALUES ("+(i+1)+",?)";
            log.debug(sql);
            sts[i] = connection.prepareStatement(sql);
        }
        //check max cache size
        Protocol protocol = getProtocolFromConnection(connection);
        assertTrue(protocol.prepareStatementCache().size() == 10);

        //check all prepared statement worked even if not cached
        for (int i=0; i< 20; i++) {
            sts[i].setInt(1, i);
            sts[i].addBatch();
            sts[i].execute();
        }
        assertTrue(protocol.prepareStatementCache().size() == 10);
        for (int i=0; i< 20; i++) {
            sts[i].close();
        }
        assertTrue(protocol.prepareStatementCache().size() == 0);
    }

*/


/*
    @Test
    public void dataConformityTest() throws SQLException {
        setConnection("&useServerPrepStmts=true");
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists preparetest");
        statement.execute("CREATE TABLE preparetest (" +

                "bigint_unsigned BIGINT UNSIGNED default 0" +
//                "float0 FLOAT default 0," +
//                "double0 DOUBLE default 1," +
//                "decimal0 DECIMAL default 0," +
//                "date0 DATE default '2001-01-01'," +
//                "datetime0 DATETIME default '2001-01-01 00:00:00'," +
//                "timestamp0 TIMESTAMP default  '2001-01-01 00:00:00'," +
//                "timestamp_zero TIMESTAMP default '0000-00-00 00:00:00'," +
//                "time0 TIME default '22:11:00'," +
//                "year2 YEAR(2) default 99," +
//                "year4 YEAR(4) default 2011," +
//                "char0 CHAR(1) default '0'," +
//                "char_binary CHAR (1) binary default '0'," +
//                "varchar0 VARCHAR(1) default '1'," +
//                "varchar_binary VARCHAR(10) BINARY default 0x1," +
//                "binary0 BINARY(10) default 0x1," +
//                "varbinary0 VARBINARY(10) default 0x1," +
//                "tinyblob0 TINYBLOB," +
//                "tinytext0 TINYTEXT," +
//                "blob0 BLOB," +
//                "text0 TEXT," +
//                "mediumblob0 MEDIUMBLOB," +
//                "mediumtext0 MEDIUMTEXT," +
//                "longblob0 LONGBLOB," +
//                "longtext0 LONGTEXT," +
//                "enum0 ENUM('a','b') default 'a'," +
//                "set0 SET('a','b') default 'a' " +
                ")");


        BigInteger bigint_unsigned = new BigInteger("3147483647");


        PreparedStatement ps = connection.prepareStatement("INSERT INTO preparetest (bigint_unsigned)  " +
                "VALUES (?)");
        ps.setObject(1, bigint_unsigned);


        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = connection.createStatement().executeQuery("SELECT * from preparetest");
        rs.next();

        assertEquals(rs.getObject(1), bigint_unsigned);


    }*/


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
                "datetime0 DATETIME default '2001-01-01 00:00:00'," +
                "timestamp0 TIMESTAMP(6) default  '2001-01-01 00:00:00'," +
                "timestamp1 TIMESTAMP(0) default  '2001-01-01 00:00:00'," +
                "timestamp_zero TIMESTAMP default '2001-01-01 00:00:00'," +
                "time0 TIME default '22:11:00'" +
//                "year2 YEAR(2) default 99," +
//                "year4 YEAR(4) default 2011," +
//                "char0 CHAR(1) default '0'," +
//                "char_binary CHAR (1) binary default '0'," +
//                "varchar0 VARCHAR(1) default '1'," +
//                "varchar_binary VARCHAR(10) BINARY default 0x1," +
//                "binary0 BINARY(10) default 0x1," +
//                "varbinary0 VARBINARY(10) default 0x1," +
//                "tinyblob0 TINYBLOB," +
//                "tinytext0 TINYTEXT," +
//                "blob0 BLOB," +
//                "text0 TEXT," +
//                "mediumblob0 MEDIUMBLOB," +
//                "mediumtext0 MEDIUMTEXT," +
//                "longblob0 LONGBLOB," +
//                "longtext0 LONGTEXT," +
//                "enum0 ENUM('a','b') default 'a'," +
//                "set0 SET('a','b') default 'a' " +
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
        GregorianCalendar gc = new GregorianCalendar();
        gc.set(Calendar.HOUR ,12);
        gc.set(Calendar.MINUTE ,0);
        gc.set(Calendar.SECOND ,0);
        gc.set(Calendar.MILLISECOND ,0);
        Date date0 = new Date(gc.getTime().getTime());

        GregorianCalendar gc2 = new GregorianCalendar();
        Calendar.getInstance();
        gc2.set(Calendar.YEAR , 1902);
        Timestamp datetime0 = new Timestamp(gc2.getTime().getTime());
        GregorianCalendar gc0 = new GregorianCalendar();
        gc0.set(Calendar.MILLISECOND ,0);

        Timestamp timestamp0 = new Timestamp(gc0.getTime().getTime());
        gc0.set(Calendar.MILLISECOND ,0);
        Timestamp timestamp1 = new Timestamp(gc0.getTime().getTime());

        String timestamp_zero = "0000-00-00 00:00:00";

        GregorianCalendar gc3 = new GregorianCalendar();
        gc3.set(Calendar.YEAR ,0);
        gc3.set(Calendar.MONTH ,0);
        gc3.set(Calendar.DAY_OF_MONTH ,0);
        Time time0 = new Time(gc3.getTime().getTime());

        PreparedStatement ps = connection.prepareStatement("INSERT INTO preparetest (bit1,bit2,tinyint1,tinyint2,bool0,smallint0,smallint_unsigned,mediumint0,mediumint_unsigned,int0," +
                "int_unsigned,bigint0,bigint_unsigned, float0, double0, decimal0,decimal1, date0,datetime0, timestamp0,timestamp1,timestamp_zero,time0)  " +
                "VALUES (?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?,?,?,?)");
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
        ps.setTimestamp(21, timestamp0);
        ps.setObject(22, timestamp_zero, Types.TIMESTAMP);
        ps.setTime(23, time0);

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
        assertEquals(rs.getDate(18), date0);
        assertEquals(rs.getTimestamp(19), datetime0);
        assertEquals(rs.getTimestamp(20), timestamp0);
        assertEquals(rs.getTimestamp(21), timestamp1);
        assertNull(rs.getTimestamp(22));
        assertEquals(rs.getTime(23), time0);



    }

/*

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
*/
    /*
    @Test
    public void blobTest() throws Throwable {
        BaseTest baseTest = new BaseTest();
        baseTest.setConnection("&prepStmtCacheSize=10");

        Statement statement = connection.createStatement();
        statement.execute("drop table if exists ServerPrepareStatementCacheSize3");
        statement.execute("create table ServerPrepareStatementCacheSize3 (id int not null primary key auto_increment, test blob)");

        PreparedStatement ps = connection.prepareStatement("INSERT INTO ServerPrepareStatementCacheSize3(test) VALUES (?)");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (URL root : Collections.list(Thread.currentThread().getContextClassLoader().getResources(""))) {
            System.out.println(root);
        }
        InputStream input = classLoader.getResourceAsStream("logback.xml");

        ps.setBlob(1, input);
        ps.addBatch();
        ps.executeBatch();
    }*/
/*
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
*/

}

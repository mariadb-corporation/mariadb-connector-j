package org.mariadb.jdbc;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DateTest extends BaseTest{
    static { Logger.getLogger("").setLevel(Level.OFF); }

    public DateTest() {

    }
    @Before
    public void beforeDateTest() throws SQLException {
        connection.createStatement().execute("drop table if exists dtest");
        connection.createStatement().execute("create table dtest (d date)");
    }

    @Test
    public void dateTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists date_test");
        stmt.executeUpdate("create table date_test (id int not null primary key auto_increment, d_test date,dt_test datetime, t_test time)");
        Date date = Date.valueOf("2009-01-17");
        Timestamp timestamp = Timestamp.valueOf("2009-01-17 15:41:01");
        Time time = Time.valueOf("23:59:59");
        PreparedStatement ps = connection.prepareStatement("insert into date_test (d_test, dt_test, t_test) values (?,?,?)");
        ps.setDate(1,date);
        ps.setTimestamp(2,timestamp);
        ps.setTime(3,time);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select d_test, dt_test, t_test from date_test");
        assertEquals(true,rs.next());
        Date date2 = rs.getDate(1);
        Date date3 = rs.getDate("d_test");
        Time time2=rs.getTime(3);
        Time time3=rs.getTime("t_test");
        Timestamp timestamp2=rs.getTimestamp(2);
        Timestamp timestamp3=rs.getTimestamp("dt_test");
        assertEquals(date.toString(), date2.toString());
        assertEquals(date.toString(), date3.toString());
        assertEquals(time.toString(), time2.toString());
        assertEquals(time.toString(), time3.toString());
        assertEquals(timestamp.toString(), timestamp2.toString());
        assertEquals(timestamp.toString(), timestamp3.toString());

    }
    @Test
    public void dateRangeTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists date_test");
        stmt.executeUpdate("create table date_test (id int not null primary key auto_increment, d_from datetime ,d_to datetime)");
        Timestamp timestamp1 = Timestamp.valueOf("2009-01-17 15:41:01");
        Timestamp timestamp2 = Timestamp.valueOf("2015-01-17 15:41:01");
        Timestamp timestamp3 = Timestamp.valueOf("2014-01-17 15:41:01");
        PreparedStatement ps = connection.prepareStatement("insert into date_test (id, d_from, d_to) values (1, ?,?)");
        ps.setTimestamp(1,timestamp1);
        ps.setTimestamp(2,timestamp2);
        ps.executeUpdate();
        PreparedStatement ps1= connection.prepareStatement("select d_from, d_to from date_test where d_from <= ? and d_to >= ?");
        ps1.setTimestamp(1,timestamp3);
        ps1.setTimestamp(2,timestamp3);
        ResultSet rs= ps1.executeQuery();
        assertEquals(true,rs.next());
        Timestamp ts1= rs.getTimestamp(1); 
        Timestamp ts2= rs.getTimestamp(2); 
        assertEquals(ts1.toString(), timestamp1.toString());
        assertEquals(ts2.toString(), timestamp2.toString());

    }

    @Test(expected = SQLException.class)
    public void dateTest2() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select 1");
        rs.next();
        rs.getDate(1);
    }

    @Test(expected = SQLException.class)
    public void dateTest3() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select 1 as a");
        rs.next();
        rs.getDate("a");
    }
    @Test(expected = SQLException.class)
    public void timeTest3() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select 'aaa' as a");
        rs.next();
        rs.getTimestamp("a");
    }

    @Test
    public void yearTest() throws SQLException {
        Assume.assumeTrue(isMariadbServer());
        connection.createStatement().execute("drop table if exists yeartest");
        connection.createStatement().execute("create table yeartest (y1 year, y2 year(2))");
        connection.createStatement().execute("insert into yeartest values (null, null), (1901, 70), (0, 0), (2155, 69)");
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from yeartest");

        Date[] data1 = new Date[] {null, Date.valueOf("1901-01-01"), Date.valueOf("0000-01-01"), Date.valueOf("2155-01-01")};
        Date[] data2 = new Date[] {null, Date.valueOf("1970-01-01"), Date.valueOf("2000-01-01"), Date.valueOf("1969-01-01")};
        int count = 0;
        while(rs.next()) {
            System.out.println("d1 " +data1[count]+" / "+rs.getObject(1));
            assertEquals(data1[count], rs.getObject(1));
            System.out.println("d2 " +data2[count]+" / "+rs.getObject(2));
            assertEquals(data2[count], rs.getObject(2));
        	count++;
        }
    }
    @Test
    public void timeTest() throws SQLException {
        connection.createStatement().execute("drop table if exists timetest");
        connection.createStatement().execute("create table timetest (t time)");
        connection.createStatement().execute("insert into timetest values (null), ('-838:59:59'), ('00:00:00'), ('838:59:59')");
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from timetest");
        Time[] data = new Time[] {null, Time.valueOf("-838:59:59"), Time.valueOf("00:00:00"), Time.valueOf("838:59:59")};
        int count = 0;
        while(rs.next()) {
        	assertEquals(data[count], rs.getObject(1));
        	count++;
        }
        rs.close();
        Calendar cal = Calendar.getInstance();
        rs = stmt.executeQuery("select '11:11:11'");
        rs.next();
        Time t = rs.getTime(1,cal);
        assertEquals(t.toString(), "11:11:11");
    }
    
    @Test
    public void timestampZeroTest() throws SQLException {
        connection.createStatement().execute("drop table if exists timestampzerotest");
        connection.createStatement().execute("create table timestampzerotest (ts timestamp, dt datetime, dd date)");
        String timestampZero = "0000-00-00 00:00:00";
        String dateZero = "0000-00-00";
        connection.createStatement().execute("insert into timestampzerotest values ('"
        		+ timestampZero + "', '" + timestampZero + "', '" + dateZero + "')");
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from timestampzerotest");
        Timestamp ts = null;
        Timestamp datetime = null;
        Date date = null;
        while(rs.next()) {
            assertEquals(null, rs.getObject(1));
            ts = rs.getTimestamp(1);
            assertEquals(rs.wasNull(), true);
            datetime = rs.getTimestamp(2);
            assertEquals(rs.wasNull(), true);
            date = rs.getDate(3);
            assertEquals(rs.wasNull(), true);
        }
        rs.close();
        assertEquals(ts, null);
        assertEquals(datetime, null);
        assertEquals(date, null);
    }

    @Test
    public void javaUtilDateInPreparedStatementAsTimeStamp() throws Exception {
        connection.createStatement().execute("drop table if exists dtest");
        connection.createStatement().execute("create table dtest (d datetime)");
        java.util.Date d = Calendar.getInstance(TimeZone.getDefault()).getTime();
        PreparedStatement ps = connection.prepareStatement("insert into dtest values(?)");
        ps.setObject(1, d, Types.TIMESTAMP);
        ps.executeUpdate();
        ResultSet rs = connection.createStatement().executeQuery("select * from dtest");
        rs.next();
        if (isMariadbServer()) {
            assertEquals(d.getTime(), rs.getTimestamp(1).getTime());
        } else {
            /* Check that time is correct, up to milliseconds precision */
            Assert.assertTrue(Math.abs((d.getTime() - rs.getTimestamp(1).getTime()) ) <= 1);
        }
    }
    
    @Test
    public void nullTimestampTest() throws SQLException {
        PreparedStatement ps = connection.prepareStatement("insert into dtest values(null)");
        ps.executeUpdate();
        ResultSet rs = connection.createStatement().executeQuery("select * from dtest where d is null");
        rs.next();
        Calendar cal = new GregorianCalendar();
        assertEquals(null, rs.getTimestamp(1, cal));
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void javaUtilDateInPreparedStatementAsDate() throws Exception {
          java.util.Date d = Calendar.getInstance(TimeZone.getDefault()).getTime();
          PreparedStatement ps = connection.prepareStatement("insert into dtest values(?)");
          ps.setObject(1, d, Types.DATE);
          ps.executeUpdate();
          ResultSet rs = connection.createStatement().executeQuery("select * from dtest");
          rs.next();
          /* Check that time is correct, up to seconds precision */
          assertEquals(d.getYear(),rs.getDate(1).getYear());
          assertEquals(d.getMonth(),rs.getDate(1).getMonth());
          assertEquals(d.getDay(),rs.getDate(1).getDay());
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void javaUtilDateInPreparedStatementAsTime() throws Exception {
          connection.createStatement().execute("drop table if exists dtest");
          connection.createStatement().execute("create table dtest (d  time)");
          java.util.Date d = Calendar.getInstance(TimeZone.getDefault()).getTime();
          PreparedStatement ps = connection.prepareStatement("insert into dtest values(?)");
          ps.setObject(1, d, Types.TIME);
          ps.executeUpdate();
          ResultSet rs = connection.createStatement().executeQuery("select * from dtest");
          rs.next();
          /* Check that time is correct, up to seconds precision */
          assertEquals(d.getHours(),rs.getTime(1).getHours());
          assertEquals(d.getMinutes(),rs.getTime(1).getMinutes());
          if (isMariadbServer()) {
              assertEquals(d.getSeconds(), rs.getTime(1).getSeconds());
          } else {
              //mysql 1 seconde precision
              Assert.assertTrue(Math.abs(d.getSeconds() - rs.getTime(1).getSeconds())<=1);
          }
    }

    @Test
    public void  serverTimezone() throws Exception {
        TimeZone tz = TimeZone.getDefault();
        //TimeZone gmt = TimeZone.getTimeZone("GMT");
        long offset = tz.getRawOffset();
        setConnection("&serverTimezone=GMT") ;
        java.util.Date now = new java.util.Date();
        offset = tz.getOffset(now.getTime());
        PreparedStatement ps = connection.prepareStatement("select now()");
        ResultSet rs = ps.executeQuery();
        rs.next();
        java.sql.Timestamp ts  =  rs.getTimestamp(1);
        long differenceToGMT = ts.getTime() - now.getTime();
        long diff = Math.abs(differenceToGMT - offset);
        assertTrue(diff < 2000); /* query take less than a second */

        ps = connection.prepareStatement("select utc_timestamp(), ?");
        ps.setObject(1,now);
        rs = ps.executeQuery();
        rs.next();
        ts  =  rs.getTimestamp(1);
        java.sql.Timestamp ts2 =  rs.getTimestamp(2);
        assertTrue(Math.abs(ts.getTime() - ts2.getTime()) < 1000); /* query take less than a second */
    }

    /**
     * CONJ-107
     * @throws SQLException
     */
    @Test
    public void timestampMillisecondsTest() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS tt");

        boolean isMariadbServer = isMariadbServer();
        if (isMariadbServer) {
            statement.execute("CREATE TABLE tt (id decimal(10), create_time datetime(6) default 0)");
            statement.execute("INSERT INTO tt (id, create_time) VALUES (1,'2013-07-18 13:44:22.123456')");
        } else {
            statement.execute("CREATE TABLE tt (id decimal(10), create_time datetime default 0)");
            statement.execute("INSERT INTO tt (id, create_time) VALUES (1,'2013-07-18 13:44:22')");
        }
        PreparedStatement ps = connection.prepareStatement("insert into tt (id, create_time) values (?,?)");
        ps.setInt(1, 2);
        Timestamp writeTs = new Timestamp(1273017612999L);
        Timestamp writeTsWithoutMilliSec = new Timestamp(1273017612999L);
        ps.setTimestamp(2, writeTs);
        ps.execute();
        ResultSet rs = statement.executeQuery("SELECT * FROM tt");
        assertTrue(rs.next());
        if (isMariadbServer) {
            assertTrue("2013-07-18 13:44:22.123456".equals(rs.getString(2)));
        } else {
            assertTrue("2013-07-18 13:44:22".equals(rs.getString(2)));
        }
        assertTrue(rs.next());
        Timestamp readTs = rs.getTimestamp(2);
        if (isMariadbServer) {
            assertEquals(writeTs, readTs);
        } else {
            assertEquals(writeTs, writeTsWithoutMilliSec);
        }
    }
}

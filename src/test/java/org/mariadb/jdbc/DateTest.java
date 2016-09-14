package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DateTest extends BaseTest {
    /**
     * Initialization.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("dtest", "d date");
        createTable("date_test2", "id int not null primary key auto_increment, d_from datetime ,d_to datetime");
        createTable("timetest", "t time");
        createTable("timetest2", "t time");
        createTable("timestampzerotest", "ts timestamp, dt datetime, dd date");
        createTable("dtest", "d datetime");
        createTable("dtest2", "d date");
        createTable("dtest3", "d date");
        createTable("dtest4", "d  time");
        createTable("date_test3", " x date");
        createTable("date_test4", "x date");
        createTable("timestampAsDate", "ts timestamp(6), dt datetime(6), dd date");
    }


    @Test
    public void dateTestLegacy() throws SQLException {
        dateTest(true);
    }

    @Test
    public void dateTestWithoutLegacy() throws SQLException {
        dateTest(false);
    }

    /**
     * Date testing.
     * @param useLegacy use legacy client side timezone or server side timezone.
     * @throws SQLException exception
     */
    public void dateTest(boolean useLegacy) throws SQLException {
        Assume.assumeFalse(sharedIsRewrite());
        Connection connection = null;
        try {
            connection = setConnection("&useLegacyDatetimeCode=" + useLegacy
                    + "&serverTimezone=+5:00&maximizeMysqlCompatibility=false&useServerPrepStmts=true");
            setSessionTimeZone(connection, "+5:00");
            createTable("date_test", "id int not null primary key auto_increment, d_test date,dt_test datetime, "
                    + "t_test time");
            Statement stmt = connection.createStatement();
            Date date = Date.valueOf("2009-01-17");
            Timestamp timestamp = Timestamp.valueOf("2009-01-17 15:41:01");
            Time time = Time.valueOf("23:59:59");
            PreparedStatement ps = connection.prepareStatement("insert into date_test (d_test, dt_test, t_test) "
                    + "values (?,?,?)");
            ps.setDate(1, date);
            ps.setTimestamp(2, timestamp);
            ps.setTime(3, time);
            ps.executeUpdate();
            ResultSet rs = stmt.executeQuery("select d_test, dt_test, t_test from date_test");
            assertEquals(true, rs.next());
            Date date2 = rs.getDate(1);
            Date date3 = rs.getDate("d_test");
            Time time2 = rs.getTime(3);
            assertEquals(date.toString(), date2.toString());
            assertEquals(date.toString(), date3.toString());
            assertEquals(time.toString(), time2.toString());
            Time time3 = rs.getTime("t_test");
            assertEquals(time.toString(), time3.toString());
            Timestamp timestamp2 = rs.getTimestamp(2);
            assertEquals(timestamp.toString(), timestamp2.toString());
            Timestamp timestamp3 = rs.getTimestamp("dt_test");
            assertEquals(timestamp.toString(), timestamp3.toString());
        } finally {
            connection.close();
        }

    }

    @Test
    public void dateRangeTest() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into date_test2 (id, d_from, d_to) values "
                + "(1, ?,?)");
        Timestamp timestamp1 = Timestamp.valueOf("2009-01-17 15:41:01");
        Timestamp timestamp2 = Timestamp.valueOf("2015-01-17 15:41:01");
        ps.setTimestamp(1, timestamp1);
        ps.setTimestamp(2, timestamp2);
        ps.executeUpdate();
        PreparedStatement ps1 = sharedConnection.prepareStatement("select d_from, d_to from date_test2 "
                + "where d_from <= ? and d_to >= ?");
        Timestamp timestamp3 = Timestamp.valueOf("2014-01-17 15:41:01");
        ps1.setTimestamp(1, timestamp3);
        ps1.setTimestamp(2, timestamp3);
        ResultSet rs = ps1.executeQuery();
        assertEquals(true, rs.next());
        Timestamp ts1 = rs.getTimestamp(1);
        Timestamp ts2 = rs.getTimestamp(2);
        assertEquals(ts1.toString(), timestamp1.toString());
        assertEquals(ts2.toString(), timestamp2.toString());

    }

    @Test(expected = SQLException.class)
    public void dateTest2() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select 1");
        rs.next();
        rs.getDate(1);
    }

    @Test(expected = SQLException.class)
    public void dateTest3() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select 1 as a");
        rs.next();
        rs.getDate("a");
    }

    @Test(expected = SQLException.class)
    public void timeTest3() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select 'aaa' as a");
        rs.next();
        rs.getTimestamp("a");
    }

    @Test
    public void yearTest() throws SQLException {
        Assume.assumeTrue(isMariadbServer());
        createTable("yeartest", "y1 year, y2 year(2)");
        sharedConnection.createStatement().execute("insert into yeartest values (null, null), (1901, 70), (0, 0), "
                + "(2155, 69)");
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from yeartest");

        Date[] data1 = new Date[]{null, Date.valueOf("1901-01-01"), Date.valueOf("0000-01-01"),
                Date.valueOf("2155-01-01")};
        Date[] data2 = new Date[]{null, Date.valueOf("1970-01-01"), Date.valueOf("2000-01-01"),
                Date.valueOf("2069-01-01")};
        checkDateResult(data1, data2, rs);

        //CONJ-282
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT * FROM yeartest");
        rs = preparedStatement.executeQuery();
        checkDateResult(data1, data2, rs);
    }

    private void checkDateResult(Date[] data1, Date[] data2, ResultSet rs) throws SQLException {
        int count = 0;
        while (rs.next()) {
            assertEquals(data1[count], rs.getObject(1));
            assertEquals(data2[count], rs.getObject(2));
            assertEquals(data1[count], rs.getDate(1));
            assertEquals(data2[count], rs.getDate(2));
            count++;
        }
    }

    @Test
    public void timeTestLegacy() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&useLegacyDatetimeCode=true&serverTimezone=+05:00");
            setSessionTimeZone(connection, "+05:00");
            connection.createStatement().execute("insert into timetest values (null), ('-838:59:59'), ('00:00:00'), "
                    + "('838:59:59')");
            Time[] data = new Time[]{null, Time.valueOf("-838:59:59"), Time.valueOf("00:00:00"),
                    Time.valueOf("838:59:59")};
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select * from timetest");
            testTime(rs, data);

            PreparedStatement pstmt = connection.prepareStatement("select * from timetest");
            testTime(pstmt.executeQuery(), data);

            rs = stmt.executeQuery("select '11:11:11'");
            testTime11(rs);

            PreparedStatement pstmt2 = connection.prepareStatement("select TIME('11:11:11') ");
            testTime11(pstmt2.executeQuery());
        } finally {
            connection.close();
        }
    }

    @Test
    public void timeTest() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&useLegacyDatetimeCode=false&serverTimezone=+5:00");
            setSessionTimeZone(connection, "+5:00");
            connection.createStatement().execute("insert into timetest2 values (null), ('00:00:00'), ('23:59:59')");
            Time[] data = new Time[]{null, Time.valueOf("00:00:00"), Time.valueOf("23:59:59")};

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select * from timetest2");
            testTime(rs, data);

            PreparedStatement pstmt = connection.prepareStatement("select * from timetest2");
            testTime(pstmt.executeQuery(), data);

            rs = stmt.executeQuery("select '11:11:11'");
            testTime11(rs);

            PreparedStatement pstmt2 = connection.prepareStatement("select TIME('11:11:11') ");
            testTime11(pstmt2.executeQuery());

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void testTime(ResultSet rs, Time[] data) throws SQLException {
        int count = 0;
        while (rs.next()) {
            Time t1 = data[count];
            Time t2 = (Time) rs.getObject(1);
            assertEquals(t1, t2);
            count++;
        }
        rs.close();
    }

    private void testTime11(ResultSet rs) throws SQLException {
        rs.next();
        Calendar cal = Calendar.getInstance();
        assertEquals("11:11:11", rs.getTime(1, cal).toString());
        rs.close();
    }

    @Test
    public void timestampZeroTest() throws SQLException {
        Assume.assumeTrue(isMariadbServer());
        String timestampZero = "0000-00-00 00:00:00";
        String dateZero = "0000-00-00";
        sharedConnection.createStatement().execute("insert into timestampzerotest values ('"
                + timestampZero + "', '" + timestampZero + "', '" + dateZero + "')");
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from timestampzerotest");
        Timestamp ts = null;
        Timestamp datetime = null;
        Date date = null;
        while (rs.next()) {
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
    public void timestampAsDate() throws SQLException {

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        Calendar cal2 = Calendar.getInstance();
        cal2.set(Calendar.YEAR, 1970);
        cal2.set(Calendar.MONTH, 0);
        cal2.set(Calendar.DAY_OF_YEAR, 1);

        Calendar cal3 = Calendar.getInstance();
        cal3.set(Calendar.HOUR_OF_DAY, 0);
        cal3.set(Calendar.MINUTE, 0);
        cal3.set(Calendar.SECOND, 0);
        cal3.set(Calendar.MILLISECOND, 0);
        cal3.set(Calendar.YEAR, 1970);
        cal3.set(Calendar.MONTH, 0);
        cal3.set(Calendar.DAY_OF_YEAR, 1);


        Timestamp currentTimeStamp = new Timestamp(System.currentTimeMillis());
        PreparedStatement preparedStatement1 = sharedConnection.prepareStatement("/*CLIENT*/ insert into timestampAsDate values (?, ?, ?)");
        preparedStatement1.setTimestamp(1, currentTimeStamp);
        preparedStatement1.setTimestamp(2, currentTimeStamp);
        preparedStatement1.setDate(3, new Date(currentTimeStamp.getTime()));
        preparedStatement1.addBatch();
        preparedStatement1.execute();

        Date dateWithoutTime = new Date(cal.getTimeInMillis());
        Time zeroTime = new Time(cal3.getTimeInMillis());

        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from timestampAsDate");
        checkResult(rs, currentTimeStamp, cal, dateWithoutTime, zeroTime);

        PreparedStatement pstmt = sharedConnection.prepareStatement("select * from timestampAsDate where 1 = ?");
        pstmt.setInt(1,1);
        pstmt.addBatch();
        rs = pstmt.executeQuery();
        checkResult(rs, currentTimeStamp, cal, dateWithoutTime, zeroTime);
    }

    private void checkResult(ResultSet rs, Timestamp currentTimeStamp, Calendar cal, Date dateWithoutTime, Time zeroTime) throws SQLException {
        if (rs.next()) {
            Assert.assertEquals(rs.getTimestamp(1), currentTimeStamp);
            Assert.assertEquals(rs.getTimestamp(2), currentTimeStamp);
            Assert.assertEquals(rs.getTimestamp(3), new Timestamp(cal.getTimeInMillis()));

            Assert.assertEquals(rs.getDate(1), new Date(currentTimeStamp.getTime()));
            Assert.assertEquals(rs.getDate(2), new Date(currentTimeStamp.getTime()));
            Assert.assertEquals(rs.getDate(3), dateWithoutTime);
            Assert.assertEquals(rs.getTime(1), new Time(currentTimeStamp.getTime()));
            Assert.assertEquals(rs.getTime(2), new Time(currentTimeStamp.getTime()));
            Assert.assertEquals(rs.getTime(3), zeroTime);
        } else {
            fail("Must have a result");
        }
        rs.close();

    }


    @Test
    public void javaUtilDateInPreparedStatementAsTimeStamp() throws Exception {
        java.util.Date currentDate = Calendar.getInstance(TimeZone.getDefault()).getTime();
        PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest values(?)");
        ps.setObject(1, currentDate, Types.TIMESTAMP);
        ps.executeUpdate();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from dtest");
        rs.next();
        /* Check that time is correct, up to seconds precision */
        Assert.assertTrue(Math.abs((currentDate.getTime() - rs.getTimestamp(1).getTime())) <= 1000);
    }

    @Test
    public void nullTimestampTest() throws SQLException {
        PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest2 values(null)");
        ps.executeUpdate();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from dtest2 where d is null");
        rs.next();
        Calendar cal = new GregorianCalendar();
        assertEquals(null, rs.getTimestamp(1, cal));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void javaUtilDateInPreparedStatementAsDate() throws Exception {
        java.util.Date currentDate = Calendar.getInstance(TimeZone.getDefault()).getTime();
        PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest3 values(?)");
        ps.setObject(1, currentDate, Types.DATE);
        ps.executeUpdate();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from dtest3");
        rs.next();
          /* Check that time is correct, up to seconds precision */
        assertEquals(currentDate.getYear(), rs.getDate(1).getYear());
        assertEquals(currentDate.getMonth(), rs.getDate(1).getMonth());
        assertEquals(currentDate.getDay(), rs.getDate(1).getDay());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void javaUtilDateInPreparedStatementAsTime() throws Exception {
        java.util.Date currentDate = Calendar.getInstance(TimeZone.getDefault()).getTime();
        PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest4 values(?)");
        ps.setObject(1, currentDate, Types.TIME);
        ps.executeUpdate();
        ResultSet rs = sharedConnection.createStatement().executeQuery("select * from dtest4");
        rs.next();

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currentDate);
        calendar.set(Calendar.YEAR, 1970);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);

        /* Check that time is correct, up to seconds precision */
        Assert.assertTrue(Math.abs(calendar.getTimeInMillis() - rs.getTime(1).getTime()) <= 1000);
    }


    @Test
    public void serverTimezone() throws Exception {
        TimeZone tz = TimeZone.getDefault();

        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=+5:00");
            setSessionTimeZone(connection, "+5:00");

            java.util.Date now = new java.util.Date();
            TimeZone canadaTimeZone = TimeZone.getTimeZone("GMT+5:00");

            long clientOffset = tz.getOffset(now.getTime());
            long serverOffset = canadaTimeZone.getOffset(System.currentTimeMillis());
            long totalOffset = serverOffset - clientOffset;
            PreparedStatement ps = connection.prepareStatement("select now()");
            ResultSet rs = ps.executeQuery();
            rs.next();
            Timestamp ts = rs.getTimestamp(1);
            long differenceToServer = ts.getTime() - now.getTime();
            long diff = Math.abs(differenceToServer - totalOffset);
            /* query take less than a second but taking in account server and client time second diff ... */
            assertTrue(diff < 5000);

            ps = connection.prepareStatement("select utc_timestamp(), ?");
            ps.setObject(1, now);
            rs = ps.executeQuery();
            rs.next();
            ts = rs.getTimestamp(1);
            Timestamp ts2 = rs.getTimestamp(2);
            long diff2 = Math.abs(ts.getTime() - ts2.getTime()) - clientOffset;
            assertTrue(diff2 < 5000); /* query take less than a second */
        } finally {
            connection.close();
        }
    }

    /**
     * Conj-107.
     *
     * @throws SQLException exception
     */
    @Test
    public void timestampMillisecondsTest() throws SQLException {
        Statement statement = sharedConnection.createStatement();

        boolean isMariadbServer = isMariadbServer();
        if (isMariadbServer) {
            createTable("tt", "id decimal(10), create_time datetime(6)");
            statement.execute("INSERT INTO tt (id, create_time) VALUES (1,'2013-07-18 13:44:22.123456')");
        } else {
            createTable("tt", "id decimal(10), create_time datetime");
            statement.execute("INSERT INTO tt (id, create_time) VALUES (1,'2013-07-18 13:44:22')");
        }
        PreparedStatement ps = sharedConnection.prepareStatement("insert into tt (id, create_time) values (?,?)");
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
            assertTrue("2013-07-18 13:44:22.0".equals(rs.getString(2)));
        }
        assertTrue(rs.next());
        Timestamp readTs = rs.getTimestamp(2);
        if (isMariadbServer) {
            assertEquals(writeTs, readTs);
        } else {
            assertEquals(writeTs, writeTsWithoutMilliSec);
        }
    }

    @Test
    public void dateTestWhenServerDifference() throws Throwable {
        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=UTC");

            PreparedStatement pst = connection.prepareStatement("insert into date_test3 values (?)");
            Date date = Date.valueOf("2013-02-01");
            pst.setDate(1, date);
            pst.execute();

            pst = connection.prepareStatement("select x from date_test3 WHERE x = ?");
            pst.setDate(1, date);
            ResultSet rs = pst.executeQuery();
            rs.next();
            Date dd = rs.getDate(1);
            assertEquals(dd, date);
        } finally {
            connection.close();
        }
    }


    @Test
    public void dateTestWhenServerDifferenceClient() throws Throwable {
        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=UTC");

            PreparedStatement pst = connection.prepareStatement("/*CLIENT*/insert into date_test4 values (?)");
            Date date = Date.valueOf("2013-02-01");
            pst.setDate(1, date);
            pst.execute();

            pst = connection.prepareStatement("/*CLIENT*/ select x from date_test4 WHERE x = ?");
            pst.setDate(1, date);
            ResultSet rs = pst.executeQuery();
            rs.next();
            Date dd = rs.getDate(1);
            assertEquals(dd, date);
        } finally {
            connection.close();
        }
    }


    /**
     * Conj-267 : null pointer exception getting zero date.
     */
    @Test
    public void nullDateString() throws Throwable {

        createTable("date_test5", "x date");
        Statement stmt = sharedConnection.createStatement();
        try {
            stmt.execute("INSERT INTO date_test5 (x) VALUES ('0000-00-00')");
            PreparedStatement pst = sharedConnection.prepareStatement("SELECT * FROM date_test5 WHERE 1 = ?");
            pst.setInt(1, 1);
            ResultSet rs = pst.executeQuery();
            Assert.assertTrue(rs.next());
            if (sharedUsePrepare()) {
                Assert.assertNull(rs.getString(1));
                Assert.assertNull(rs.getDate(1));
            } else {
                Assert.assertEquals("0000-00-00", rs.getString(1));
                Assert.assertNull(rs.getDate(1));
            }
        } catch (SQLDataException sqldataException) {
            //'0000-00-00' doesn't work anymore on mysql 5.7.
        }
    }

    /**
     * Conj-317 : null pointer exception on getDate on null timestamp.
     */
    @Test
    public void nullDateFromTimestamp() throws Throwable {

        createTable("nulltimestamp", "ts timestamp(6) NULL ");
        Statement stmt = sharedConnection.createStatement();
        try {
            stmt.execute("INSERT INTO nulltimestamp (ts) VALUES ('0000-00-00'), (null)");

            PreparedStatement pst = sharedConnection.prepareStatement("SELECT * FROM nulltimestamp WHERE 1 = ?");
            pst.setInt(1, 1);
            ResultSet rs = pst.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertNull(rs.getString(1));
            Assert.assertNull(rs.getDate(1));
            Assert.assertNull(rs.getTimestamp(1));
            Assert.assertNull(rs.getTime(1));

            Assert.assertTrue(rs.next());
            Assert.assertNull(rs.getString(1));
            Assert.assertNull(rs.getDate(1));
            Assert.assertNull(rs.getTimestamp(1));
            Assert.assertNull(rs.getTime(1));

        } catch (SQLDataException sqldataException) {
            //'0000-00-00' doesn't work anymore on mysql 5.7.
        }
    }

}

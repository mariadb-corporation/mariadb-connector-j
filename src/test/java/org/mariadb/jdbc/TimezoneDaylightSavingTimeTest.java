package org.mariadb.jdbc;

import org.junit.*;

import java.io.InputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Scanner;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class TimezoneDaylightSavingTimeTest extends BaseTest {

    private static Locale previousFormatLocale;
    private static TimeZone previousTimeZone;
    private static TimeZone utcTimeZone;
    private static SimpleDateFormat dateFormatISO8601;
    private static SimpleDateFormat utcDateFormatISO8601;
    private static SimpleDateFormat utcDateFormatSimple;
    private static TimeZone parisTimeZone;
    private static TimeZone canadaTimeZone;

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        if (testSingleHost) {
            Statement st = null;
            try {
                st = sharedConnection.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) from mysql.time_zone_name "
                        + "where Name in ('Europe/Paris','Canada/Atlantic')");
                rs.next();
                if (rs.getInt(1) == 0) {
                    ResultSet rs2 = st.executeQuery("SELECT DATABASE()");
                    rs2.next();
                    String currentDatabase = rs2.getString(1);
                    st.execute("USE mysql");

                    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                    importSql(sharedConnection, classLoader.getResourceAsStream("timezoneTest.sql"));
                    st.execute("USE " + currentDatabase);
                }

            } finally {
                if (st != null) {
                    st.close();
                }
            }

            //Save the previous FORMAT locate so we can restore it later
            previousFormatLocale = Locale.getDefault();
            //Save the previous timezone so we can restore it later
            previousTimeZone = TimeZone.getDefault();

            //I have tried to represent all times written in the code in the UTC time zone
            utcTimeZone = TimeZone.getTimeZone("UTC");

            parisTimeZone = TimeZone.getTimeZone("Europe/Paris");
            canadaTimeZone = TimeZone.getTimeZone("Canada/Atlantic");
            TimeZone.setDefault(parisTimeZone);


            //Use a date formatter for UTC timezone in ISO 8601 so users in different
            //timezones can compare the test results easier.
            dateFormatISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            utcDateFormatISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            utcDateFormatISO8601.setTimeZone(utcTimeZone);

            utcDateFormatSimple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            utcDateFormatSimple.setTimeZone(utcTimeZone);
            createTable("daylightMysql", " tt DATE");
            if (doPrecisionTest) {
                createTable("timeZoneTime", "id int, tt TIME(6)");
                createTable("ttimeTest", "id int not null primary key auto_increment, dd TIME(3), dd2 TIME(3)");
            }

        }

    }

    /**
     * Put the TimeZone to previous state.
     *
     * @throws SQLException exception
     */
    @AfterClass()
    public static void endClass() throws SQLException {
        if (testSingleHost) {
            TimeZone.setDefault(previousTimeZone);
            Locale.setDefault(previousFormatLocale);
        }
    }

    /**
     * Import some timeZone stuff for testing.
     *
     * @param conn current connection
     * @param in   inputStream
     * @throws SQLException exception
     */
    public static void importSql(Connection conn, InputStream in) throws SQLException {
        Scanner scanner = new Scanner(in);
        scanner.useDelimiter("(;(\r)?\n)|(--\n)");
        Statement st = null;
        try {
            st = conn.createStatement();
            while (scanner.hasNext()) {
                String line = scanner.next();
                if (line.startsWith("/*!") && line.endsWith("*/")) {
                    int spaceIndex = line.indexOf(' ');
                    line = line.substring(spaceIndex + 1, line.length() - " */".length());
                }
                if (line.trim().length() > 0) {
                    st.execute(line);
                }
            }
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    /**
     * Resetting default local time.
     */
    @After
    public void tearDown() {
        //Reset the FORMAT locate so other test cases are not disturbed.
        if (previousFormatLocale != null) {
            Locale.setDefault(previousFormatLocale);
        }
        //Reset the timezone so so other test cases are not disturbed.
        if (previousTimeZone != null) {
            TimeZone.setDefault(previousTimeZone);
        }
    }


    @Test
    public void testTimeStamp() throws SQLException {
        TimeZone.setDefault(parisTimeZone);
        try (Connection connection = setConnection("&serverTimezone=Europe/Paris&useServerPrepStmts=true")) {
            setSessionTimeZone(connection, "Europe/Paris");
            //timestamp timezone to parisTimeZone like server
            Timestamp currentTimeParis = new Timestamp(System.currentTimeMillis());
            PreparedStatement st = connection.prepareStatement("SELECT ?");
            st.setTimestamp(1, currentTimeParis);
            ResultSet rs = st.executeQuery();
            rs.next();
            assertEquals(rs.getTimestamp(1), currentTimeParis);
        }
    }

    @Test
    public void testTimeStampUtc() throws SQLException {

        TimeZone.setDefault(parisTimeZone);
        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=UTC&useServerPrepStmts=true");
            setSessionTimeZone(connection, "+00:00");
            //timestamp timezone to parisTimeZone like server
            Timestamp currentTimeParis = new Timestamp(System.currentTimeMillis());
            PreparedStatement st = connection.prepareStatement("SELECT ?");
            st.setTimestamp(1, currentTimeParis);
            ResultSet rs = st.executeQuery();
            rs.next();
            Timestamp t1 = rs.getTimestamp(1);
            assertEquals(t1, currentTimeParis);
        } finally {
            connection.close();
        }
    }

    @Test
    public void testTimeStampUtcNow() throws SQLException {
        TimeZone.setDefault(parisTimeZone);
        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=UTC&useServerPrepStmts=true");
            TimeZone.setDefault(parisTimeZone);
            setSessionTimeZone(connection, "+00:00");
            //timestamp timezone to parisTimeZone like server
            Timestamp currentTimeParis = new Timestamp(System.currentTimeMillis());
            PreparedStatement st = connection.prepareStatement("SELECT NOW()");
            ResultSet rs = st.executeQuery();
            rs.next();
            int offset = parisTimeZone.getOffset(System.currentTimeMillis());
            long timeDifference = currentTimeParis.getTime() - offset - rs.getTimestamp(1).getTime();

            assertTrue(timeDifference < 1000); // must have less than one second difference
        } finally {
            connection.close();
        }
    }


    @Test
    public void testDifferentTime() throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
        TimeZone.setDefault(parisTimeZone);
        Connection connection = null;
        try {
            connection = setConnection();

            PreparedStatement st = connection.prepareStatement("INSERT INTO timeZoneTime (tt) VALUES (?)");
            st.setString(1, "90:00:00.123456");
            st.addBatch();
            st.setString(1, "800:00:00");
            st.addBatch();
            st.setString(1, "800");
            st.addBatch();
            st.setString(1, "-22");
            st.addBatch();
            st.setString(1, "151413");
            st.addBatch();
            st.setString(1, "9:6:3");
            st.addBatch();
            st.setString(1, "00:00:01");
            st.addBatch();
            st.executeBatch();


            st = connection.prepareStatement("SELECT tt FROM timeZoneTime");
            st.executeQuery();
            ResultSet rs = st.getResultSet();
            rs.next();
            assertEquals("90:00:00.123456", rs.getString(1));
            Time tit = rs.getTime(1);
            Time tt2 = Time.valueOf("90:00:00");
            tt2.setTime(tt2.getTime() + 123);
            assertEquals(tit, tt2);
            int offset = 3600000;//paris timezone offset 1970-01-01
            assertEquals(tit.getTime(), (long) 90 * 3600000 + 123 - offset);
            assertEquals(rs.getTimestamp(1), new Timestamp(70, 0, 1, 90, 0, 0, 123456000));
            rs.next();
            assertEquals(rs.getString(1), "800:00:00.000000");
            assertEquals(rs.getTime(1).getTime(), (long) 800 * 3600000 - offset);
            assertEquals(rs.getTimestamp(1), new Timestamp(70, 0, 1, 800, 0, 0, 0));
            rs.next();
            assertEquals(rs.getString(1), "00:08:00.000000");
            assertEquals(rs.getTime(1).getTime(), (long) 8 * 60000 - offset);
            assertEquals(rs.getTimestamp(1), new Timestamp(70, 0, 1, 0, 8, 0, 0));
            rs.next();
            assertEquals(rs.getString(1), "-00:00:22.000000");
            assertEquals(rs.getTimestamp(1), new Timestamp(70, 0, 1, 0, 0, 22, 0));
            rs.next();
            assertEquals(rs.getString(1), "15:14:13.000000");
            assertEquals(rs.getTime(1).getTime(), (long) 15 * 3600000 + 14 * 60000 + 13 * 1000 - offset);
            assertEquals(rs.getTimestamp(1), new Timestamp(70, 0, 1, 15, 14, 13, 0));
            rs.next();
            assertEquals(rs.getString(1), "09:06:03.000000");
            assertEquals(rs.getTime(1).getTime(), (long) 9 * 3600000 + 6 * 60000 + 3 * 1000 - offset);
            assertEquals(rs.getTimestamp(1), new Timestamp(70, 0, 1, 9, 6, 3, 0));
            rs.next();
            assertEquals(rs.getString(1), "00:00:01.000000");
            Time tt = rs.getTime(1);
            assertEquals(tt.getTime(), (long) 1000 - offset);
        } finally {
            connection.close();
        }
    }


    @Test
    public void testTimeUtc() throws SQLException {
        Assume.assumeTrue(doPrecisionTest);

        TimeZone.setDefault(parisTimeZone);
        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=UTC");
            setSessionTimeZone(connection, "+00:00");
            Calendar initTime = Calendar.getInstance(TimeZone.getTimeZone("Europe/Paris"));
            initTime.clear();
            initTime.set(1970, 0, 1, 1, 45, 23);
            initTime.set(Calendar.MILLISECOND, 123);

            Time timeParis = new Time(initTime.getTimeInMillis());
            Time timeParis2 = Time.valueOf("01:45:23");
            timeParis2.setTime(timeParis2.getTime() + 123);

            PreparedStatement st1 = connection.prepareStatement("INSERT INTO ttimeTest (dd, dd2) values (?, ?)");
            st1.setTime(1, timeParis);
            st1.setTime(2, timeParis2);
            st1.execute();

            PreparedStatement st = connection.prepareStatement("SELECT dd, dd2 from ttimeTest");
            ResultSet rs = st.executeQuery();
            rs.next();
            assertEquals(rs.getTime(1).getTime(), timeParis.getTime());
            assertEquals(rs.getTime(1), timeParis);
            assertEquals(rs.getTime(2).getTime(), timeParis2.getTime());
            assertEquals(rs.getTime(2), timeParis2);

        } finally {
            connection.close();
        }
    }


    @Test
    public void testTimeUtcNow() throws SQLException {
        TimeZone.setDefault(parisTimeZone);
        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=UTC");
            setSessionTimeZone(connection, "+00:00");
            //time timezone to parisTimeZone like server
            Time currentTimeParis = new Time(System.currentTimeMillis());
            PreparedStatement st = sharedConnection.prepareStatement("SELECT NOW()");
            ResultSet rs = st.executeQuery();
            rs.next();
            int offset = parisTimeZone.getOffset(System.currentTimeMillis());
            long timeDifference = currentTimeParis.getTime() - offset - rs.getTimestamp(1).getTime();

            assertTrue(timeDifference < 1000); // must have less than one second difference
        } finally {
            connection.close();
        }
    }

    @Test
    public void testTimeOffsetNowUseServer() throws SQLException {

        Connection connection = null;
        try {
            connection = setConnection("&useLegacyDatetimeCode=false&serverTimezone=+5:00");
            setSessionTimeZone(connection, "+5:00");
            //timestamp timezone to parisTimeZone like server
            Time currentTimeParis = new Time(System.currentTimeMillis());
            PreparedStatement st = connection.prepareStatement("SELECT NOW()");
            ResultSet rs = st.executeQuery();
            rs.next();
            Timestamp nowServer = rs.getTimestamp(1);
            long timeDifference = currentTimeParis.getTime() - nowServer.getTime();
            assertTrue(timeDifference < 1000); // must have less than one second difference
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void testDifferentTimeZoneServer() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=UTC");
            setSessionTimeZone(sharedConnection, "+00:00");
            //timestamp timezone to parisTimeZone like server
            Time currentTimeParis = new Time(System.currentTimeMillis());
            PreparedStatement st = sharedConnection.prepareStatement("SELECT NOW()");
            ResultSet rs = st.executeQuery();
            rs.next();
            int offset = parisTimeZone.getOffset(System.currentTimeMillis());
            long timeDifference = currentTimeParis.getTime() - offset - rs.getTimestamp(1).getTime();
            assertTrue(timeDifference < 1000); // must have less than one second difference
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }


    @Test
    public void testTimeStampOffsetNowUseServer() throws SQLException {
        try (Connection connection = setConnection("&serverTimezone=Europe/Paris")) {
            //timestamp timezone to parisTimeZone like server
            Timestamp currentTimeParis = new Timestamp(System.currentTimeMillis());
            PreparedStatement st = connection.prepareStatement("SELECT NOW()");
            ResultSet rs = st.executeQuery();
            rs.next();
            int offset = parisTimeZone.getOffset(System.currentTimeMillis());
            long timeDifference = currentTimeParis.getTime() - offset - rs.getTimestamp(1).getTime();
            assertTrue(timeDifference < 1000); // must have less than one second difference
        }
    }

    @Test
    public void testDayLightLegacy() throws SQLException {
        testDayLight(true);
    }

    @Test
    public void testDayLight() throws SQLException {
        testDayLight(false);
    }


    private void testDayLight(boolean legacy) throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
        Assume.assumeTrue(hasSuperPrivilege("testDayLight") && !sharedIsRewrite());
        TimeZone.setDefault(parisTimeZone);
        Connection connection = null;
        try {
            connection = setConnection("&useLegacyDatetimeCode=" + legacy + "&serverTimezone=Canada/Atlantic");
            setSessionTimeZone(connection, "Canada/Atlantic");

            createTable("daylight", "id int, t1 TIMESTAMP(6), t2 TIME(6), t3 DATETIME(6) , t4 DATE");
            Calendar quarterBeforeChangingHour = Calendar.getInstance(TimeZone.getTimeZone("utc"));
            quarterBeforeChangingHour.clear();
            quarterBeforeChangingHour.set(2015, 2, 29, 0, 45, 0);
            int offsetBefore = parisTimeZone.getOffset(quarterBeforeChangingHour.getTimeInMillis());
            Assert.assertEquals(offsetBefore, 3600000);


            SimpleDateFormat dateFormatIso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            dateFormatIso8601.setTimeZone(canadaTimeZone);

            Calendar test = Calendar.getInstance(TimeZone.getTimeZone("Canada/Atlantic"));
            test.set(2015, 2, 28, 22, 45, 0);

            Calendar quarterAfterChangingHour = Calendar.getInstance(TimeZone.getTimeZone("utc"));
            quarterAfterChangingHour.clear();
            quarterAfterChangingHour.set(2015, 2, 29, 1, 15, 0);
            int offsetAfter = parisTimeZone.getOffset(quarterAfterChangingHour.getTimeInMillis());
            Assert.assertEquals(offsetAfter, 7200000);


            Timestamp vt1 = new Timestamp(quarterBeforeChangingHour.getTimeInMillis());
            vt1.setNanos(12340000);
            PreparedStatement pst = connection.prepareStatement("INSERT INTO daylight VALUES (?, ?, ?, ?, ?)");
            pst.setInt(1, 1);
            pst.setTimestamp(2, vt1);
            pst.setTime(3, new Time(quarterBeforeChangingHour.getTimeInMillis()));
            pst.setTimestamp(4, Timestamp.valueOf("2015-03-29 01:45:00"));
            pst.setDate(5, new Date(quarterBeforeChangingHour.getTimeInMillis()));
            pst.addBatch();

            pst.setInt(1, 2);
            pst.setTimestamp(2, Timestamp.valueOf("2015-03-29 03:15:00"));
            pst.setTime(3, new Time(quarterAfterChangingHour.getTimeInMillis()));
            pst.setTimestamp(4, Timestamp.valueOf("2015-03-29 03:15:00"));
            pst.setDate(5, new Date(quarterAfterChangingHour.getTimeInMillis()));
            pst.addBatch();
            pst.executeBatch();

            checkResult(true, connection);
            checkResult(false, connection);

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void checkResult(boolean binaryProtocol, Connection connection) throws SQLException {
        ResultSet rs;
        if (binaryProtocol) {
            PreparedStatement pst = connection.prepareStatement("SELECT * from daylight where 1 = ?");
            pst.setInt(1, 1);
            rs = pst.executeQuery();
        } else {
            rs = connection.createStatement().executeQuery("SELECT * from daylight");
        }

        rs.next();
        Timestamp t1 = rs.getTimestamp(2);

        assertEquals(dateFormatISO8601.format(t1), "2015-03-29T01:45:00+0100");

        Time t2 = rs.getTime(3);
        assertEquals(t2.getTime(), (long) 2700000);
        Timestamp vtt2 = new Timestamp(70, 0, 1, 1, 45, 0, 0);
        Timestamp tt2 = rs.getTimestamp(3);
        assertEquals(tt2, vtt2);
        assertEquals(dateFormatISO8601.format(t2), "1970-01-01T01:45:00+0100");
        Timestamp t3 = rs.getTimestamp(4);
        assertEquals(dateFormatISO8601.format(t3), "2015-03-29T01:45:00+0100");

        Date t4 = rs.getDate(5);
        assertEquals(t4, new Date(2015 - 1900, 2, 29));
        assertEquals(rs.getString(5), "2015-03-29");

        rs.next();
        t1 = rs.getTimestamp(2);
        t2 = rs.getTime(3);
        t3 = rs.getTimestamp(4);
        t4 = rs.getDate(5);

        assertEquals(t1.getTime(), t1.getTime());
        assertEquals(dateFormatISO8601.format(t1), "2015-03-29T03:15:00+0200");

        assertEquals(t2.getTime(), (long) 8100000);
        vtt2 = new Timestamp(70, 0, 1, 3, 15, 0, 0);
        tt2 = rs.getTimestamp(3);
        assertEquals(tt2, vtt2);
        assertEquals(dateFormatISO8601.format(t2), "1970-01-01T03:15:00+0100");

        assertEquals(t3.getTime(), t3.getTime());
        assertEquals(dateFormatISO8601.format(t3), "2015-03-29T03:15:00+0200");


        assertEquals(t4, new Date(2015 - 1900, 2, 29));
        assertEquals(rs.getString(5), "2015-03-29");

    }


    @Test
    public void testDayLightNotUtC() throws SQLException {
        Assume.assumeTrue(doPrecisionTest && hasSuperPrivilege("testDayLight") && !sharedIsRewrite());
        TimeZone.setDefault(canadaTimeZone);
        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=Europe/Paris");
            Statement st = connection.createStatement();
            String serverTimeZone = null;
            ResultSet rs = st.executeQuery("SHOW GLOBAL VARIABLES LIKE 'time_zone';");
            if (rs.next()) {
                serverTimeZone = rs.getString(2);
            }
            try {
                st.executeQuery("SET GLOBAL time_zone = 'Europe/Paris'");
                rs = st.executeQuery("SHOW GLOBAL VARIABLES LIKE 'time_zone';");
                rs.next();
                createTable("daylightCanada", "id int, tt TIMESTAMP(6)");

                Calendar quarterBeforeChangingHour = Calendar.getInstance(TimeZone.getTimeZone("Canada/Atlantic"));
                quarterBeforeChangingHour.clear();
                quarterBeforeChangingHour.set(2015, 2, 28, 21, 45, 0);

                int offsetBefore = parisTimeZone.getOffset(quarterBeforeChangingHour.getTimeInMillis());
                Assert.assertEquals(offsetBefore, 3600000);

                int offsetBeforeCanada = canadaTimeZone.getOffset(quarterBeforeChangingHour.getTimeInMillis());
                Assert.assertEquals(offsetBeforeCanada, -10800000);

                Calendar quarterAfterChangingHour = Calendar.getInstance(TimeZone.getTimeZone("Canada/Atlantic"));
                quarterAfterChangingHour.clear();
                quarterAfterChangingHour.set(2015, 2, 28, 22, 15, 0);

                int offsetAfter = parisTimeZone.getOffset(quarterAfterChangingHour.getTimeInMillis());
                Assert.assertEquals(offsetAfter, 7200000);

                int offsetAfterCanada = canadaTimeZone.getOffset(quarterAfterChangingHour.getTimeInMillis());
                Assert.assertEquals(offsetAfterCanada, -10800000);

                PreparedStatement pst = connection.prepareStatement("INSERT INTO daylightCanada VALUES (?, ?)");
                pst.setInt(1, 1);
                pst.setTimestamp(2, new Timestamp(quarterBeforeChangingHour.getTimeInMillis()));
                pst.addBatch();
                pst.setInt(1, 2);
                pst.setTimestamp(2, new Timestamp(quarterAfterChangingHour.getTimeInMillis()));
                pst.addBatch();
                pst.executeBatch();

                rs = st.executeQuery("SELECT * from daylightCanada");
                rs.next();
                Timestamp tt = rs.getTimestamp(2);
                assertEquals(tt.getTime(), quarterBeforeChangingHour.getTimeInMillis());
                assertEquals(dateFormatISO8601.format(tt), "2015-03-29T01:45:00+0100");

                rs.next();
                tt = rs.getTimestamp(2);
                assertEquals(tt.getTime(), quarterAfterChangingHour.getTimeInMillis());
                assertEquals(dateFormatISO8601.format(tt), "2015-03-29T03:15:00+0200");

            } finally {
                if (serverTimeZone != null) {
                    st.executeQuery("SET GLOBAL time_zone = '" + serverTimeZone + "'");
                }
            }
        } finally {
            connection.close();
        }
    }


    @Test
    public void testDayLightWithClientTimeZoneDifferent() throws SQLException {
        Assume.assumeTrue(doPrecisionTest && !sharedIsRewrite());
        TimeZone.setDefault(parisTimeZone);
        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=UTC");

            Calendar quarterBeforeChangingHour = Calendar.getInstance(TimeZone.getTimeZone("utc"));
            quarterBeforeChangingHour.clear();
            quarterBeforeChangingHour.set(2015, 2, 29, 0, 45, 0);
            int offsetBefore = parisTimeZone.getOffset(quarterBeforeChangingHour.getTimeInMillis());
            Assert.assertEquals(offsetBefore, 3600000);

            Calendar quarterAfterChangingHour = Calendar.getInstance(TimeZone.getTimeZone("utc"));
            quarterAfterChangingHour.clear();
            quarterAfterChangingHour.set(2015, 2, 29, 1, 15, 0);
            int offsetAfter = parisTimeZone.getOffset(quarterAfterChangingHour.getTimeInMillis());
            Assert.assertEquals(offsetAfter, 7200000);

            createTable("daylight2", "id int, tt TIMESTAMP(6)");

            Timestamp tt = new Timestamp(quarterBeforeChangingHour.getTimeInMillis());
            tt.setNanos(123400000);
            PreparedStatement pst = connection.prepareStatement("INSERT INTO daylight2 VALUES (?, ?)");
            pst.setInt(1, 1);
            pst.setTimestamp(2, tt);
            pst.addBatch();
            pst.setInt(1, 2);
            pst.setTimestamp(2, new Timestamp(quarterAfterChangingHour.getTimeInMillis()));
            pst.addBatch();
            pst.executeBatch();

            //test with text protocol

            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT * from daylight2");
            rs.next();

            String timeBefore = dateFormatISO8601.format(rs.getTimestamp(2));
            assertEquals(timeBefore, "2015-03-29T01:45:00+0100");
            rs.next();
            String timeAfter = dateFormatISO8601.format(rs.getTimestamp(2));
            assertEquals(timeAfter, "2015-03-29T03:15:00+0200");

            //test with binary protocol
            pst = connection.prepareStatement("SELECT * from daylight2 where id = ?");
            pst.setInt(1, 1);
            pst.addBatch();
            rs = pst.executeQuery();
            rs.next();
            timeBefore = dateFormatISO8601.format(rs.getTimestamp(2));
            assertEquals(timeBefore, "2015-03-29T01:45:00+0100");

            pst.setInt(1, 2);
            pst.addBatch();
            rs = pst.executeQuery();
            rs.next();
            timeAfter = dateFormatISO8601.format(rs.getTimestamp(2));
            assertEquals(timeAfter, "2015-03-29T03:15:00+0200");
        } finally {
            connection.close();
        }
    }


    @Test
    public void testNoMysqlDayLightCompatibility() throws SQLException {
        Assume.assumeTrue(hasSuperPrivilege("testMysqlDayLightCompatibility"));
        TimeZone.setDefault(parisTimeZone);
        Connection connection = null;
        try {
            connection = setConnection("&maximizeMysqlCompatibility=false&useLegacyDatetimeCode=false"
                    + "&serverTimezone=Canada/Atlantic");
            setSessionTimeZone(connection, "Canada/Atlantic");
            Calendar quarterBeforeChangingHour = Calendar.getInstance(TimeZone.getTimeZone("utc"));
            quarterBeforeChangingHour.clear();
            quarterBeforeChangingHour.set(2015, 2, 29, 0, 45, 0);
            int offsetBefore = parisTimeZone.getOffset(quarterBeforeChangingHour.getTimeInMillis());
            Assert.assertEquals(offsetBefore, 3600000);


            Timestamp vt1 = new Timestamp(quarterBeforeChangingHour.getTimeInMillis());
            vt1.setNanos(12340000);
            PreparedStatement pst = connection.prepareStatement("INSERT INTO daylightMysql VALUES (?)");
            pst.setDate(1, new Date(quarterBeforeChangingHour.getTimeInMillis()));
            pst.addBatch();
            pst.executeBatch();

            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT * from daylightMysql");
            rs.next();
            Date t4 = rs.getDate(1);

            //2015-02-29 0h45 UTC -> 2015-02-28 21h45 Canada time
            java.util.Date dt = new Date(2015 - 1900, 2, 29);
            assertEquals(t4, dt);
            assertEquals(rs.getString(1), "2015-03-29");
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

    }

}
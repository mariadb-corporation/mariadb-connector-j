package org.mariadb.jdbc;

import org.junit.*;

import java.io.InputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimezoneDaylightSavingTimeTest extends BaseTest {

    private static Locale previousFormatLocale;
    private static TimeZone previousTimeZone;
    private static TimeZone utcTimeZone;
    private static SimpleDateFormat dateFormatISO8601;
    private static SimpleDateFormat utcDateFormatISO8601;
    private static SimpleDateFormat utcDateFormatSimple;
    private static TimeZone parisTimeZone;
    private static TimeZone canadaTimeZone;

    @BeforeClass()
    public static void initClass() throws SQLException {

        Statement st = null;
        try {
            st = sharedConnection.createStatement();
            ResultSet rs = st.executeQuery("SELECT count(*) from mysql.time_zone_name where Name in ('Europe/Paris','Canada/Atlantic')");
            rs.next();
            log.debug("time zone information : " + rs.getInt(1));
            if (rs.getInt(1) == 0) {
                ResultSet rs2 = st.executeQuery("SELECT DATABASE()");
                rs2.next();
                String currentDatabase = rs2.getString(1);
                st.execute("USE mysql");

                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                importSQL(sharedConnection, classLoader.getResourceAsStream("timezoneTest.sql"));
                st.execute("USE " + currentDatabase);
            }

        } finally {
            if (st != null) st.close();
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
    }


    public static void importSQL(Connection conn, InputStream in) throws SQLException {
        Scanner s = new Scanner(in);
        s.useDelimiter("(;(\r)?\n)|(--\n)");
        Statement st = null;
        try {
            st = conn.createStatement();
            while (s.hasNext()) {
                String line = s.next();
                if (line.startsWith("/*!") && line.endsWith("*/")) {
                    int i = line.indexOf(' ');
                    line = line.substring(i + 1, line.length() - " */".length());
                }
                if (line.trim().length() > 0) st.execute(line);
            }
        } finally {
            if (st != null) st.close();
        }
    }

    @After
    public void tearDown() {
        //Reset the FORMAT locate so other test cases are not disturbed.
        if (previousFormatLocale != null) Locale.setDefault(previousFormatLocale);
        //Reset the timezone so so other test cases are not disturbed.
        if (previousTimeZone != null) TimeZone.setDefault(previousTimeZone);
    }



    private void setSessionTimeZone(Connection connection, String timeZone) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("set @@session.time_zone = '" + timeZone + "'");
        statement.close();
    }


    @Test
    public void testTimeStamp() throws SQLException {
        TimeZone.setDefault(parisTimeZone);
        setSessionTimeZone(sharedConnection, "Europe/Paris");
        Timestamp currentTimeParis = new Timestamp(System.currentTimeMillis()); //timestamp timezone to parisTimeZone like server
        PreparedStatement st = sharedConnection.prepareStatement("SELECT ?");
        st.setTimestamp(1, currentTimeParis);
        ResultSet rs = st.executeQuery();
        rs.next();
        assertEquals(rs.getTimestamp(1), currentTimeParis);
    }

    @Test
    public void testTimeStampUTC() throws SQLException {
        TimeZone.setDefault(parisTimeZone);
        setSessionTimeZone(sharedConnection, "+00:00");
        Timestamp currentTimeParis = new Timestamp(System.currentTimeMillis()); //timestamp timezone to parisTimeZone like server
        PreparedStatement st = sharedConnection.prepareStatement("SELECT ?");
        st.setTimestamp(1, currentTimeParis);
        ResultSet rs = st.executeQuery();
        rs.next();
        assertEquals(rs.getTimestamp(1), currentTimeParis);
    }

    @Test
    public void testTimeStampUTCNow() throws SQLException {
        TimeZone.setDefault(parisTimeZone);
        setSessionTimeZone(sharedConnection, "+00:00");
        Timestamp currentTimeParis = new Timestamp(System.currentTimeMillis()); //timestamp timezone to parisTimeZone like server
        PreparedStatement st = sharedConnection.prepareStatement("SELECT NOW()");
        ResultSet rs = st.executeQuery();
        rs.next();
        int offset = parisTimeZone.getOffset(System.currentTimeMillis());
        long timeDifference = currentTimeParis.getTime() - offset - rs.getTimestamp(1).getTime();

        assertTrue(timeDifference < 1000); // must have less than one second difference
    }


    @Test
    public void testTimeStampOffsetNowUseServer() throws SQLException {
        setConnection("&serverTimezone=Europe/Paris");
        Timestamp currentTimeParis = new Timestamp(System.currentTimeMillis()); //timestamp timezone to parisTimeZone like server
        PreparedStatement st = sharedConnection.prepareStatement("SELECT NOW()");
        ResultSet rs = st.executeQuery();
        rs.next();
        int offset = parisTimeZone.getOffset(System.currentTimeMillis());
        long timeDifference = currentTimeParis.getTime() - offset - rs.getTimestamp(1).getTime();
        assertTrue(timeDifference < 1000); // must have less than one second difference
    }
    @Test
    public void testDayLight() throws SQLException {

        Assume.assumeTrue(hasSuperPrivilege("testDayLight"));
        TimeZone.setDefault(parisTimeZone);
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
                if (rs.next()) {
                    System.out.println("new time_zone =" + rs.getString(2) + " was " + serverTimeZone);
                }
                createTable("daylight", "id int, tt TIMESTAMP(6)");
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


                PreparedStatement pst = connection.prepareStatement("INSERT INTO daylight VALUES (?, ?)");
                pst.setInt(1, 1);
                pst.setTimestamp(2, new Timestamp(quarterBeforeChangingHour.getTimeInMillis()));
                pst.addBatch();
                pst.setInt(1, 2);
                pst.setTimestamp(2, new Timestamp(quarterAfterChangingHour.getTimeInMillis()));
                pst.addBatch();
                pst.executeBatch();

                rs = st.executeQuery("SELECT * from daylight");
                rs.next();
                Timestamp tt = rs.getTimestamp(2);
                assertEquals(tt.getTime(), quarterBeforeChangingHour.getTimeInMillis());
                assertEquals(dateFormatISO8601.format(tt), "2015-03-29T01:45:00+0100");
                rs.next();
                tt = rs.getTimestamp(2);
                assertEquals(tt.getTime(), quarterAfterChangingHour.getTimeInMillis());
                assertEquals(dateFormatISO8601.format(tt), "2015-03-29T03:15:00+0200");
            } finally {
                if (serverTimeZone != null)
                    st.executeQuery("SET GLOBAL time_zone = '" + serverTimeZone + "'");

            }
        } finally {
            connection.close();
        }
    }

    @Test
    public void testDayLightnotUtC() throws SQLException {

        Assume.assumeTrue(hasSuperPrivilege("testDayLight"));
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
                if (rs.next()) {
                    System.out.println("new time_zone =" + rs.getString(2) + " was " + serverTimeZone);
                }

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
                if (serverTimeZone != null)
                    st.executeQuery("SET GLOBAL time_zone = '" + serverTimeZone + "'");

            }
        } finally {
            connection.close();
        }
    }

    @Test
    public void testDayLightWithClientTimeZoneDifferent() throws SQLException {
        TimeZone.setDefault(parisTimeZone);
        Connection connection = null;
        try {
            connection = setConnection("&serverTimezone=UTC");
            Statement st = connection.createStatement();

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

            PreparedStatement pst = connection.prepareStatement("INSERT INTO daylight2 VALUES (?, ?)");
            pst.setInt(1, 1);
            pst.setTimestamp(2, new Timestamp(quarterBeforeChangingHour.getTimeInMillis()));
            pst.addBatch();
            pst.setInt(1, 2);
            pst.setTimestamp(2, new Timestamp(quarterAfterChangingHour.getTimeInMillis()));
            pst.addBatch();
            pst.executeBatch();

            //test with text protocol
            ResultSet rs = st.executeQuery("SELECT * from daylight2");
            rs.next();


            String tBefore = dateFormatISO8601.format(rs.getTimestamp(2));
            assertEquals(tBefore, "2015-03-29T01:45:00+0100");
            rs.next();
            String tAfter = dateFormatISO8601.format(rs.getTimestamp(2));
            assertEquals(tAfter, "2015-03-29T03:15:00+0200");

            //test with binary protocol
            pst = connection.prepareStatement("SELECT * from daylight2 where id = ?");
            pst.setInt(1, 1);
            pst.addBatch();
            rs = pst.executeQuery();
            rs.next();
            tBefore = dateFormatISO8601.format(rs.getTimestamp(2));
            assertEquals(tBefore, "2015-03-29T01:45:00+0100");

            pst.setInt(1, 2);
            pst.addBatch();
            rs = pst.executeQuery();
            rs.next();
            tAfter = dateFormatISO8601.format(rs.getTimestamp(2));
            assertEquals(tAfter, "2015-03-29T03:15:00+0200");
        } finally {
            connection.close();
        }
    }


}
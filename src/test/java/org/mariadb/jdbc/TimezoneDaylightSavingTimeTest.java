/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;


import org.junit.*;

import java.io.InputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.Calendar;
import java.util.Locale;
import java.util.Scanner;
import java.util.TimeZone;

import static org.junit.Assert.*;

@SuppressWarnings("deprecation")
public class TimezoneDaylightSavingTimeTest extends BaseTest {

    public static SimpleDateFormat formatter;
    public static SimpleDateFormat utcDateFormatISO8601;
    public static SimpleDateFormat utcDateFormatSimple;
    public static TimeZone parisTimeZone;
    public static TimeZone canadaTimeZone;
    private static Locale previousFormatLocale;
    private static TimeZone previousTimeZone;
    private static TimeZone utcTimeZone;

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        if (testSingleHost || !"true".equals(System.getenv("AURORA"))) {
            try (Statement st = sharedConnection.createStatement()) {
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
            formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
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
        if (testSingleHost || !"true".equals(System.getenv("AURORA"))) {
            TimeZone.setDefault(previousTimeZone);
            if (previousFormatLocale != null) Locale.setDefault(previousFormatLocale);
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
        try (Statement st = conn.createStatement()) {
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
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
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
        try (Connection connection = setConnection("&serverTimezone=UTC&useServerPrepStmts=true")) {
            setSessionTimeZone(connection, "+00:00");
            //timestamp timezone to parisTimeZone like server
            Timestamp currentTimeParis = new Timestamp(System.currentTimeMillis());
            PreparedStatement st = connection.prepareStatement("SELECT ?");
            st.setTimestamp(1, currentTimeParis);
            ResultSet rs = st.executeQuery();
            rs.next();
            Timestamp t1 = rs.getTimestamp(1);
            assertEquals(t1, currentTimeParis);
        }
    }

    @Test
    public void testTimeStampUtcNow() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        TimeZone.setDefault(parisTimeZone);
        try (Connection connection = setConnection("&serverTimezone=UTC&useServerPrepStmts=true")) {
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
        }
    }


    @Test
    public void testDifferentTime() throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
        TimeZone.setDefault(parisTimeZone);
        try (Connection connection = setConnection()) {
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
        }
    }


    @Test
    public void testTimeUtc() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        Assume.assumeTrue(doPrecisionTest);

        TimeZone.setDefault(parisTimeZone);
        try (Connection connection = setConnection("&serverTimezone=UTC")) {
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
        }
    }


    @Test
    public void testTimeUtcNow() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        TimeZone.setDefault(parisTimeZone);
        try (Connection connection = setConnection("&serverTimezone=UTC")) {
            setSessionTimeZone(connection, "+00:00");
            //time timezone to parisTimeZone like server
            Time currentTimeParis = new Time(System.currentTimeMillis());
            PreparedStatement st = sharedConnection.prepareStatement("SELECT NOW()");
            ResultSet rs = st.executeQuery();
            rs.next();
            int offset = parisTimeZone.getOffset(System.currentTimeMillis());
            long timeDifference = currentTimeParis.getTime() - offset - rs.getTimestamp(1).getTime();

            assertTrue(timeDifference < 1000); // must have less than one second difference
        }
    }

    @Test
    public void testTimeOffsetNowUseServer() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        try (Connection connection = setConnection("&useLegacyDatetimeCode=false&serverTimezone=+5:00")) {
            setSessionTimeZone(connection, "+5:00");
            //timestamp timezone to parisTimeZone like server
            Time currentTimeParis = new Time(System.currentTimeMillis());
            PreparedStatement st = connection.prepareStatement("SELECT NOW()");
            ResultSet rs = st.executeQuery();
            rs.next();
            Timestamp nowServer = rs.getTimestamp(1);
            long timeDifference = currentTimeParis.getTime() - nowServer.getTime();
            assertTrue(timeDifference < 1000); // must have less than one second difference
        }
    }

    @Test
    public void testDifferentTimeZoneServer() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        try (Connection connection = setConnection("&serverTimezone=UTC")) {
            setSessionTimeZone(sharedConnection, "+00:00");
            //timestamp timezone to parisTimeZone like server
            Time currentTimeParis = new Time(System.currentTimeMillis());
            PreparedStatement st = sharedConnection.prepareStatement("SELECT NOW()");
            ResultSet rs = st.executeQuery();
            rs.next();
            int offset = parisTimeZone.getOffset(System.currentTimeMillis());
            long timeDifference = currentTimeParis.getTime() - offset - rs.getTimestamp(1).getTime();
            assertTrue(timeDifference < 1000); // must have less than one second difference
        }
    }


    @Test
    public void testTimeStampOffsetNowUseServer() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
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
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        Assume.assumeTrue(doPrecisionTest);
        Assume.assumeTrue(hasSuperPrivilege("testDayLight") && !sharedIsRewrite());
        TimeZone.setDefault(parisTimeZone);
        try (Connection connection = setConnection("&useLegacyDatetimeCode=" + legacy
                + "&serverTimezone=Canada/Atlantic&sessionVariables=time_zone='Canada/Atlantic'")) {

            createTable("daylight", "id int, t1 TIMESTAMP(6) NULL, t2 TIME(6), t3 DATETIME(6) , t4 DATE");

            Calendar quarterBeforeChangingHour = Calendar.getInstance(TimeZone.getTimeZone("utc"));
            quarterBeforeChangingHour.clear();
            quarterBeforeChangingHour.set(2015, 2, 29, 0, 45, 0);

            //check that paris is UTC+1, canada is UTC-3
            assertEquals(3600000, parisTimeZone.getOffset(quarterBeforeChangingHour.getTimeInMillis()));
            assertEquals(-3 * 3600000, canadaTimeZone.getOffset(quarterBeforeChangingHour.getTimeInMillis()));

            SimpleDateFormat dateFormatIso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            dateFormatIso8601.setTimeZone(canadaTimeZone);

            Calendar quarterAfterChangingHour = Calendar.getInstance(TimeZone.getTimeZone("utc"));
            quarterAfterChangingHour.clear();
            quarterAfterChangingHour.set(2015, 2, 29, 1, 15, 0);

            //check that paris is UTC+2, canada is UTC-3
            assertEquals(2 * 3600000, parisTimeZone.getOffset(quarterAfterChangingHour.getTimeInMillis()));
            assertEquals(-3 * 3600000, canadaTimeZone.getOffset(quarterAfterChangingHour.getTimeInMillis()));

            Timestamp vt1 = new Timestamp(quarterBeforeChangingHour.getTimeInMillis());
            vt1.setNanos(12340000);

            PreparedStatement pst = connection.prepareStatement("INSERT INTO daylight VALUES (?, ?, ?, ?, ?)");
            pst.setInt(1, 1);
            pst.setTimestamp(2, vt1);
            pst.setTime(3, new Time(quarterBeforeChangingHour.getTimeInMillis()));
            pst.setTimestamp(4, Timestamp.valueOf("2015-03-29 01:45:00.01234"));
            pst.setDate(5, new Date(quarterBeforeChangingHour.getTimeInMillis()));
            pst.addBatch();

            Timestamp vt2 = new Timestamp(quarterAfterChangingHour.getTimeInMillis());
            vt2.setNanos(12340000);

            pst.setInt(1, 2);
            pst.setTimestamp(2, vt2);
            pst.setTime(3, new Time(quarterAfterChangingHour.getTimeInMillis()));
            pst.setTimestamp(4, Timestamp.valueOf("2015-03-29 03:15:00.01234"));
            pst.setDate(5, new Date(quarterAfterChangingHour.getTimeInMillis()));
            pst.addBatch();

            pst.setInt(1, 3);
            pst.setTimestamp(2, null);
            pst.setTime(3, null);
            pst.setTimestamp(4, null);
            pst.setDate(5, null);
            pst.addBatch();

            pst.executeBatch();

            //check data inserted in DB :
            String req = "SELECT"
                    + " t1 = STR_TO_DATE(" + (legacy ? "'2015-03-29 01:45:00.01234'" : "'2015-03-28 21:45:00.01234'") + ",'%Y-%m-%d %T.%f')"
                    + ", t2 = TIME_FORMAT('01:45:00.00000','%T.%f')"
                    + ", t3 = STR_TO_DATE(" + (legacy ? "'2015-03-29 01:45:00.01234'" : "'2015-03-28 21:45:00.01234'") + ",'%Y-%m-%d %T.%f')"
                    + ", t4 = STR_TO_DATE('2015-03-29','%Y-%m-%d')"
                    + " FROM daylight  WHERE id = 1";
            ResultSet rs1 = connection.createStatement().executeQuery(req);
            rs1.next();
            assertTrue(rs1.getBoolean(1));
            assertTrue(rs1.getBoolean(2));
            assertTrue(rs1.getBoolean(3));
            assertTrue(rs1.getBoolean(4));

            req = "SELECT"
                    + " t1 = STR_TO_DATE(" + (legacy ? "'2015-03-29 03:15:00.01234'" : "'2015-03-28 22:15:00.01234'") + ",'%Y-%m-%d %T.%f')"
                    + ", t2 = TIME_FORMAT('03:15:00.00000','%T.%f')"
                    + ", t3 = STR_TO_DATE(" + (legacy ? "'2015-03-29 03:15:00.01234'" : "'2015-03-28 22:15:00.01234'") + ",'%Y-%m-%d %T.%f')"
                    + ", t4 = STR_TO_DATE('2015-03-29','%Y-%m-%d')"
                    + " FROM daylight  WHERE id = 2";
            rs1 = connection.createStatement().executeQuery(req);
            rs1.next();
            assertTrue(rs1.getBoolean(1));
            assertTrue(rs1.getBoolean(2));
            assertTrue(rs1.getBoolean(3));
            assertTrue(rs1.getBoolean(4));

            req = "SELECT"
                    + " t1 IS NULL, t2 IS NULL, t3 IS NULL, t4 IS NULL"
                    + " FROM daylight  WHERE id = 3";
            rs1 = connection.createStatement().executeQuery(req);
            rs1.next();
            assertTrue(rs1.getBoolean(1));
            assertTrue(rs1.getBoolean(2));
            assertTrue(rs1.getBoolean(3));
            assertTrue(rs1.getBoolean(4));


            checkResult(legacy, true, connection);
            checkResult(legacy, false, connection);

        }
    }

    /**
     * Check results are accurates.
     *
     * @param legacy         is in legacy mode
     * @param binaryProtocol binary protocol
     * @param connection     connection
     * @return current resultset
     * @throws SQLException if connection error occur.
     */
    public ResultSet checkResult(boolean legacy, boolean binaryProtocol, Connection connection) throws SQLException {
        ResultSet rs;
        PreparedStatement pst;
        if (binaryProtocol) {
            pst = connection.prepareStatement("SELECT * from daylight where 1 = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } else {
            MariaDbConnection mariaDbConnection = (MariaDbConnection) connection;
            pst = new MariaDbPreparedStatementClient(mariaDbConnection, "SELECT * from daylight where 1 = ?",
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        }
        pst.setInt(1, 1);
        rs = pst.executeQuery();

        rs.next();


        //test timestamp(6)
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getTimestamp(2)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getObject(2, Timestamp.class)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getObject(2, Time.class)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getObject(2, Date.class)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getObject(2, Calendar.class).getTime()));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getObject(2, java.util.Date.class)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getTime(2)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getDate(2)));

        //test time(6)
        assertEquals("1970-01-01T01:45:00.000+0100", formatter.format(rs.getTimestamp(3)));
        assertEquals("1970-01-01T01:45:00.000+0100", formatter.format(rs.getObject(3, Timestamp.class)));
        assertEquals("1970-01-01T01:45:00.000+0100", formatter.format(rs.getObject(3, Time.class)));
        assertEquals("1970-01-01T01:45:00.000+0100", formatter.format(rs.getObject(3, Calendar.class).getTime()));
        assertEquals("1970-01-01T01:45:00.000+0100", formatter.format(rs.getObject(3, java.util.Date.class)));
        try {
            formatter.format(rs.getObject(3, Date.class));
            fail();
        } catch (SQLException e) {
            //expected exception
        }
        assertEquals("1970-01-01T01:45:00.000+0100", formatter.format(rs.getTime(3)));
        assertEquals((long) 2700000, rs.getTime(3).getTime());
        try {
            formatter.format(rs.getDate(3));
            fail();
        } catch (SQLException e) {
            //expected exception
        }

        //test datetime(6)
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getTimestamp(4)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getObject(4, Timestamp.class)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getObject(4, Calendar.class).getTime()));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getObject(4, java.util.Date.class)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getObject(4, Time.class)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getObject(4, Date.class)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getTime(4)));
        assertEquals("2015-03-29T01:45:00.012+0100", formatter.format(rs.getDate(4)));

        //test date(6)
        assertEquals("2015-03-29T00:00:00.000+0100", formatter.format(rs.getTimestamp(5)));
        assertEquals("2015-03-29T00:00:00.000+0100", formatter.format(rs.getObject(5, Timestamp.class)));
        try {
            formatter.format(rs.getObject(5, Time.class));
            fail();
        } catch (SQLException e) {
            //expected exception
        }
        assertEquals("2015-03-29T00:00:00.000+0100", formatter.format(rs.getObject(5, Date.class)));
        try {
            formatter.format(rs.getTime(5));
            fail();
        } catch (SQLException e) {
            //expected exception
        }
        assertEquals("2015-03-29T00:00:00.000+0100", formatter.format(rs.getDate(5)));
        assertEquals(new Date(2015 - 1900, 2, 29), rs.getDate(5));
        assertEquals(rs.getString(5), "2015-03-29");


        rs.next();
        //test timestamp(6)
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getTimestamp(2)));
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getObject(2, Timestamp.class)));
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getObject(2, Time.class)));
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getObject(2, Date.class)));
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getTime(2)));
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getDate(2)));

        //test time(6)
        assertEquals("1970-01-01T03:15:00.000+0100", formatter.format(rs.getTimestamp(3)));
        assertEquals("1970-01-01T03:15:00.000+0100", formatter.format(rs.getObject(3, Timestamp.class)));
        assertEquals("1970-01-01T03:15:00.000+0100", formatter.format(rs.getObject(3, Time.class)));
        try {
            formatter.format(rs.getObject(3, Date.class));
            fail();
        } catch (SQLException e) {
            //expected exception
        }
        assertEquals("1970-01-01T03:15:00.000+0100", formatter.format(rs.getTime(3)));
        assertEquals((long) 8100000, rs.getTime(3).getTime());
        try {
            formatter.format(rs.getDate(3));
            fail();
        } catch (SQLException e) {
            //expected exception
        }

        //test datetime(6)
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getTimestamp(4)));
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getObject(4, Timestamp.class)));
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getObject(4, Time.class)));
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getObject(4, Date.class)));
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getTime(4)));
        assertEquals("2015-03-29T03:15:00.012+0200", formatter.format(rs.getDate(4)));

        //test date(6)
        assertEquals("2015-03-29T00:00:00.000+0100", formatter.format(rs.getTimestamp(5)));
        assertEquals("2015-03-29T00:00:00.000+0100", formatter.format(rs.getObject(5, Timestamp.class)));
        try {
            formatter.format(rs.getObject(5, Time.class));
        } catch (SQLException e) {
            //expected exception
        }
        assertEquals("2015-03-29T00:00:00.000+0100", formatter.format(rs.getObject(5, Date.class)));
        try {
            formatter.format(rs.getTime(5));
        } catch (SQLException e) {
            //expected exception
        }
        assertEquals("2015-03-29T00:00:00.000+0100", formatter.format(rs.getDate(5)));
        assertEquals(new Date(2015 - 1900, 2, 29), rs.getDate(5));
        assertEquals(rs.getString(5), "2015-03-29");


        rs.next();
        //test timestamp(6)
        for (int i = 2; i < 6; i++) {
            assertNull(rs.getTimestamp(i));
            assertNull(rs.getTime(i));
            assertNull(rs.getDate(i));
            assertNull(rs.getObject(i, Timestamp.class));
            assertNull(rs.getObject(i, Time.class));
            assertNull(rs.getObject(i, Date.class));
            assertNull(rs.getObject(i, Calendar.class));
            assertNull(rs.getObject(i, java.util.Date.class));
        }

        rs.first();

        //additional tests for java 8 objects
        assertEquals("2015-03-29T01:45:00.012340", rs.getTimestamp(2).toLocalDateTime().toString());
        assertEquals("2015-03-29T01:45:00.012340", rs.getObject(2, LocalDateTime.class).toString());
        if (legacy) {
            assertEquals("2015-03-29T01:45:00.012340+01:00[Europe/Paris]", rs.getObject(2, ZonedDateTime.class).toString());
            assertEquals("2015-03-29T01:45:00.012340+01:00", rs.getObject(2, OffsetDateTime.class).toString());
        } else {
            assertEquals("2015-03-28T21:45:00.012340-03:00[Canada/Atlantic]", rs.getObject(2, ZonedDateTime.class).toString());
            assertEquals("2015-03-28T21:45:00.012340-03:00", rs.getObject(2, OffsetDateTime.class).toString());
        }
        assertEquals("01:45:00.012340", rs.getObject(2, LocalTime.class).toString());
        assertEquals("2015-03-29", rs.getObject(2, LocalDate.class).toString());

        rs.next();
        //additional tests for java 8 objects
        assertEquals("2015-03-29T03:15:00.012340", rs.getTimestamp(2).toLocalDateTime().toString());
        assertEquals("2015-03-29T03:15:00.012340", rs.getObject(2, LocalDateTime.class).toString());
        if (legacy) {
            assertEquals("2015-03-29T03:15:00.012340+02:00[Europe/Paris]", rs.getObject(2, ZonedDateTime.class).toString());
            assertEquals("2015-03-29T03:15:00.012340+02:00", rs.getObject(2, OffsetDateTime.class).toString());
        } else {
            assertEquals("2015-03-28T22:15:00.012340-03:00[Canada/Atlantic]", rs.getObject(2, ZonedDateTime.class).toString());
            assertEquals("2015-03-28T22:15:00.012340-03:00", rs.getObject(2, OffsetDateTime.class).toString());
        }
        assertEquals("03:15:00.012340", rs.getObject(2, LocalTime.class).toString());
        assertEquals("2015-03-29", rs.getObject(2, LocalDate.class).toString());

        rs.next();
        //test timestamp(6)
        for (int i = 2; i < 6; i++) {
            assertNull(rs.getObject(i, LocalDateTime.class));
            assertNull(rs.getObject(i, OffsetDateTime.class));
            assertNull(rs.getObject(i, ZonedDateTime.class));
            assertNull(rs.getObject(i, LocalDate.class));
            assertNull(rs.getObject(i, LocalTime.class));
            assertNull(rs.getObject(i, OffsetTime.class));
        }

        return rs;

    }


    @Test
    public void testDayLightNotUtC() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        Assume.assumeTrue(doPrecisionTest && hasSuperPrivilege("testDayLight") && !sharedIsRewrite());
        TimeZone.setDefault(canadaTimeZone);
        try (Connection connection = setConnection("&serverTimezone=Europe/Paris")) {
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
                assertEquals(offsetBefore, 3600000);

                int offsetBeforeCanada = canadaTimeZone.getOffset(quarterBeforeChangingHour.getTimeInMillis());
                assertEquals(offsetBeforeCanada, -10800000);

                Calendar quarterAfterChangingHour = Calendar.getInstance(TimeZone.getTimeZone("Canada/Atlantic"));
                quarterAfterChangingHour.clear();
                quarterAfterChangingHour.set(2015, 2, 28, 22, 15, 0);

                int offsetAfter = parisTimeZone.getOffset(quarterAfterChangingHour.getTimeInMillis());
                assertEquals(offsetAfter, 7200000);

                int offsetAfterCanada = canadaTimeZone.getOffset(quarterAfterChangingHour.getTimeInMillis());
                assertEquals(offsetAfterCanada, -10800000);

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
                assertEquals("2015-03-29T01:45:00.000+0100", formatter.format(tt));

                rs.next();
                tt = rs.getTimestamp(2);
                assertEquals(tt.getTime(), quarterAfterChangingHour.getTimeInMillis());
                assertEquals("2015-03-29T03:15:00.000+0200", formatter.format(tt));

            } finally {
                if (serverTimeZone != null) {
                    st.executeQuery("SET GLOBAL time_zone = '" + serverTimeZone + "'");
                }
            }
        }
    }


    @Test
    public void testDayLightWithClientTimeZoneDifferent() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        Assume.assumeTrue(doPrecisionTest && !sharedIsRewrite());
        TimeZone.setDefault(parisTimeZone);
        try (Connection connection = setConnection("&serverTimezone=UTC")) {
            Calendar quarterBeforeChangingHour = Calendar.getInstance(TimeZone.getTimeZone("utc"));
            quarterBeforeChangingHour.clear();
            quarterBeforeChangingHour.set(2015, 2, 29, 0, 45, 0);
            int offsetBefore = parisTimeZone.getOffset(quarterBeforeChangingHour.getTimeInMillis());
            assertEquals(offsetBefore, 3600000);

            Calendar quarterAfterChangingHour = Calendar.getInstance(TimeZone.getTimeZone("utc"));
            quarterAfterChangingHour.clear();
            quarterAfterChangingHour.set(2015, 2, 29, 1, 15, 0);
            int offsetAfter = parisTimeZone.getOffset(quarterAfterChangingHour.getTimeInMillis());
            assertEquals(offsetAfter, 7200000);

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
            assertEquals("2015-03-29T01:45:00.123+0100", formatter.format(rs.getTimestamp(2)));

            rs.next();
            assertEquals("2015-03-29T03:15:00.000+0200", formatter.format(rs.getTimestamp(2)));

            //test with binary protocol
            pst = connection.prepareStatement("SELECT * from daylight2 where id = ?");
            pst.setInt(1, 1);
            rs = pst.executeQuery();
            rs.next();
            assertEquals("2015-03-29T01:45:00.123+0100", formatter.format(rs.getTimestamp(2)));

            pst.setInt(1, 2);
            rs = pst.executeQuery();
            rs.next();
            assertEquals("2015-03-29T03:15:00.000+0200", formatter.format(rs.getTimestamp(2)));
        }
    }


    @Test
    public void testNoMysqlDayLightCompatibility() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        Assume.assumeTrue(hasSuperPrivilege("testMysqlDayLightCompatibility"));
        TimeZone.setDefault(parisTimeZone);
        try (Connection connection = setConnection("&maximizeMysqlCompatibility=false&useLegacyDatetimeCode=false"
                + "&serverTimezone=Canada/Atlantic")) {
            setSessionTimeZone(connection, "Canada/Atlantic");
            Calendar quarterBeforeChangingHour = Calendar.getInstance(TimeZone.getTimeZone("utc"));
            quarterBeforeChangingHour.clear();
            quarterBeforeChangingHour.set(2015, 2, 29, 0, 45, 0);
            int offsetBefore = parisTimeZone.getOffset(quarterBeforeChangingHour.getTimeInMillis());
            assertEquals(offsetBefore, 3600000);


            Timestamp vt1 = new Timestamp(quarterBeforeChangingHour.getTimeInMillis());
            vt1.setNanos(12340000);
            PreparedStatement pst = connection.prepareStatement("INSERT INTO daylightMysql VALUES (?)");
            pst.setDate(1, new Date(quarterBeforeChangingHour.getTimeInMillis()));
            pst.addBatch();
            pst.executeBatch();

            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT * from daylightMysql");
            assertTrue(rs.next());
            Date t4 = rs.getDate(1);

            //2015-02-29 0h45 UTC -> 2015-02-28 21h45 Canada time
            java.util.Date dt = new Date(2015 - 1900, 2, 29);
            assertEquals(t4, dt);
            assertEquals(rs.getString(1), "2015-03-29");
        }

    }

    @Test
    public void checkSetLocalDateTimeNoOffset() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        Assume.assumeFalse(!isMariadbServer() && strictBeforeVersion(5,6));
        checkSetLocalDateTime(true, true, "Europe/Paris");
        checkSetLocalDateTime(true, false, "Europe/Paris");
        checkSetLocalDateTime(false, true, "Europe/Paris");
        checkSetLocalDateTime(false, false, "Europe/Paris");
    }

    @Test
    public void checkSetLocalDateTimeOffset() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        Assume.assumeFalse(!isMariadbServer() && strictBeforeVersion(5,6));
        checkSetLocalDateTime(true, true, "+2:00");
        checkSetLocalDateTime(true, false, "+2:00");
        checkSetLocalDateTime(false, true, "+2:00");
        checkSetLocalDateTime(false, false, "+2:00");
    }


    /**
     * Goal is to check that with default Timezone canada (UTC -4) and database set to Europe/Paris ( UTC+1)
     * if using legacy, Driver use java default time zone (=canada).
     * if using !legacy, Driver use server time zone (=paris).
     *
     * @param legacy          flag indicator
     * @param useBinaryFormat use binary format
     * @throws SQLException if connection error occur
     */
    public void checkSetLocalDateTime(boolean legacy, boolean useBinaryFormat, String timeZone) throws SQLException {
        createTable("checkLocalDateTime" + legacy + useBinaryFormat, " id int, tt DATETIME(6), tt2 TIME(6), tt3 varchar(100), tt4 varchar(100) ");
        TimeZone initialTimeZone = TimeZone.getDefault();
        boolean isOffset = timeZone.startsWith("+");
        if (isOffset) {
            TimeZone.setDefault(TimeZone.getTimeZone("GMT-3:00"));
        } else {
            TimeZone.setDefault(canadaTimeZone);
        }


        LocalDateTime localDateTime = LocalDateTime.parse("2015-03-28T22:15:30.123456");
        try (Connection connection = setConnection("&useLegacyDatetimeCode=" + legacy
                + "&sessionVariables=time_zone='" + timeZone + "'&useServerPrepStmts=" + useBinaryFormat)) {
            PreparedStatement st = connection.prepareStatement("INSERT INTO checkLocalDateTime"
                    + legacy + useBinaryFormat + "(id, tt, tt2, tt3, tt4) VALUES (?, ?, ?, ?, ?)");
            st.setObject(1, 1);
            st.setObject(2, localDateTime);
            st.setObject(3, localDateTime);
            st.setObject(4, "2015-03-29 03:15:30.123456+02:00");
            st.setObject(5, "03:15:30.123456+02:00");
            st.execute();

            st.setObject(1, 2);
            st.setObject(2, "2015-03-28 22:15:30.123456", Types.TIMESTAMP);
            st.setObject(3, "2015-03-28 22:15:30.123456", Types.TIMESTAMP);
            st.setObject(4, "2015-03-29 03:15:30.123456+02:00");
            st.setObject(5, "03:15:30.123456+02:00");
            st.execute();

            st.setObject(1, 3);
            st.setObject(2, "2015-03-29 02:15:30.123456+01:00", Types.TIMESTAMP_WITH_TIMEZONE);
            st.setObject(3, "2015-03-29 02:15:30.123456+01:00", Types.TIMESTAMP_WITH_TIMEZONE);
            st.setObject(4, "2015-03-29 03:15:30.123456+02:00");
            st.setObject(5, "03:15:30.123456+02:00");
            st.execute();

            st.setObject(1, 4);
            st.setObject(2, "2015-03-29 03:15:30.123456+02:00", Types.TIMESTAMP_WITH_TIMEZONE);
            st.setObject(3, "2015-03-29 03:15:30.123456+02:00", Types.TIMESTAMP_WITH_TIMEZONE);
            st.setObject(4, "2015-03-29 03:15:30.123456+02:00");
            st.setObject(5, "03:15:30.123456+02:00");
            st.execute();

            String req = "SELECT"
                    + " tt = STR_TO_DATE(" + (legacy ? "'2015-03-28 22:15:30.123456'" : "'2015-03-29 03:15:30.123456'") + ",'%Y-%m-%d %T.%f')"
                    + ", tt2 = TIME_FORMAT(" + (legacy ? "'22:15:30.123456'" : "'03:15:30.123456'") + ",'%T.%f')"
                    + " FROM checkLocalDateTime" + legacy + useBinaryFormat;
            ResultSet rs1 = connection.createStatement().executeQuery(req);

            rs1.next();
            assertTrue(rs1.getBoolean(1));
            assertTrue(rs1.getBoolean(2));
            rs1.next();
            assertTrue(rs1.getBoolean(1));
            assertTrue(rs1.getBoolean(2));
            rs1.next();
            assertTrue(rs1.getBoolean(1));
            assertTrue(rs1.getBoolean(2));
            rs1.next();
            assertTrue(rs1.getBoolean(1));
            assertTrue(rs1.getBoolean(2));

            ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM checkLocalDateTime" + legacy + useBinaryFormat);
            assertTrue(rs.next());
            checkResultSetLocalDateTime(rs, legacy, isOffset);

            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM checkLocalDateTime" + legacy + useBinaryFormat);
            rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            checkResultSetLocalDateTime(rs, legacy, isOffset);

        } finally {
            TimeZone.setDefault(initialTimeZone);
        }
    }

    private void checkResultSetLocalDateTime(ResultSet rs, boolean useLegacy, boolean isOffset) throws SQLException {

        assertEquals("22:15:30", rs.getTime(2).toString());
        try {
            rs.getObject(2, Instant.class);
            fail();
        } catch (SQLFeatureNotSupportedException e) {
            //expected exception
        }
        catchException(rs, 3, LocalDate.class, "Cannot read LocalDate using a Types.TIME field");
        if (!isOffset) {

            catchException(rs, 2, OffsetTime.class, "Cannot return an OffsetTime for a TIME field");
            catchException(rs, 3, OffsetTime.class, "Cannot return an OffsetTime for a TIME field");
            catchException(rs, 4, OffsetTime.class, "Cannot return an OffsetTime for a TIME field");
            catchException(rs, 5, OffsetTime.class, "Cannot return an OffsetTime for a TIME field");
        } else {
            if (useLegacy) {
                assertEquals("22:15:30.123456-03:00", rs.getObject(2, OffsetTime.class).toString());
                assertEquals("22:15:30.123456-03:00", rs.getObject(3, OffsetTime.class).toString());
            } else {
                assertEquals("03:15:30.123456+02:00", rs.getObject(2, OffsetTime.class).toString());
                assertEquals("03:15:30.123456+02:00", rs.getObject(3, OffsetTime.class).toString());
            }
            assertEquals("03:15:30.123456+02:00", rs.getObject(5, OffsetTime.class).toString());
        }
        catchException(rs, 3, LocalDateTime.class, "Cannot read java.time.LocalDateTime using a Types.TIME field");
        catchException(rs, 3, OffsetDateTime.class, "Cannot read java.time.OffsetDateTime using a Types.TIME field");
        catchException(rs, 3, ZonedDateTime.class, "Cannot read java.time.ZonedDateTime using a Types.TIME field");


        assertEquals("2015-03-28", rs.getObject(2, LocalDate.class).toString());
        assertEquals("2015-03-28T22:15:30.123456", rs.getObject(2, LocalDateTime.class).toString());
        assertEquals("2015-03-29T03:15:30.123456+02:00", rs.getObject(4, OffsetDateTime.class).toString());
        assertEquals("2015-03-29T03:15:30.123456+02:00", rs.getObject(4, ZonedDateTime.class).toString());

        if (useLegacy) {
            assertEquals("2015-03-28T22:15:30.123456-03:00", rs.getObject(2, OffsetDateTime.class).toString());
            if (isOffset) {
                assertEquals("2015-03-28T22:15:30.123456-03:00[GMT-03:00]", rs.getObject(2, ZonedDateTime.class).toString());
            } else {
                assertEquals("2015-03-28T22:15:30.123456-03:00[Canada/Atlantic]", rs.getObject(2, ZonedDateTime.class).toString());
            }
        } else {
            assertEquals("2015-03-29T03:15:30.123456+02:00", rs.getObject(2, OffsetDateTime.class).toString());
            if (isOffset) {
                assertEquals("2015-03-29T03:15:30.123456+02:00[GMT+02:00]", rs.getObject(2, ZonedDateTime.class).toString());
            } else {
                assertEquals("2015-03-29T03:15:30.123456+02:00[Europe/Paris]", rs.getObject(2, ZonedDateTime.class).toString());
            }
        }
    }

    private void catchException(ResultSet rs, int position, Class<?> clazz, String expectedMsg) {
        try {
            Object obj = rs.getObject(position, clazz).toString();
            fail("Error, must have thrown exception, but result object is : " + obj);
        } catch (SQLException sqle) {
            assertTrue("msg:" + sqle.getMessage() + "-exp:" + expectedMsg, sqle.getMessage().contains(expectedMsg));
        }

    }


}
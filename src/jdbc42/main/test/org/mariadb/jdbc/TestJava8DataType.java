package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.time.*;
import java.util.TimeZone;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

public class TestJava8DataType extends TimezoneDaylightSavingTimeTest {

    @Test
    public void checkSetLocalDateTimeNoOffset() throws SQLException {
        checkSetLocalDateTime(true, true, "Europe/Paris");
        checkSetLocalDateTime(true, false, "Europe/Paris");
        checkSetLocalDateTime(false, true, "Europe/Paris");
        checkSetLocalDateTime(false, false, "Europe/Paris");
    }

    @Test
    public void checkSetLocalDateTimeOffset() throws SQLException {
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
     * @param legacy flag indicator
     * @param useBinaryFormat       use binary format
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
            Assert.assertTrue(rs.next());
            checkResultSetLocalDateTime(rs, legacy, isOffset);

            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM checkLocalDateTime" + legacy + useBinaryFormat);
            rs = preparedStatement.executeQuery();
            Assert.assertTrue(rs.next());
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

    private  void catchException(ResultSet rs, int position, Class<?> clazz, String expectedMsg) {
        try {
            Object obj = rs.getObject(position, clazz).toString();
            fail("Error, must have thrown exception, but result object is : " + obj);
        } catch (SQLException sqle) {
            assertTrue("msg:" + sqle.getMessage() + "-exp:" + expectedMsg, sqle.getMessage().contains(expectedMsg));
        }

    }


    @Override
    public ResultSet checkResult(boolean legacy, boolean binaryProtocol, Connection connection) throws SQLException {
        ResultSet rs = super.checkResult(legacy, binaryProtocol, connection);
        rs.absolute(1);

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
        for ( int i = 2; i < 6; i++) {
            assertNull(rs.getObject(i, LocalDateTime.class));
            assertNull(rs.getObject(i, OffsetDateTime.class));
            assertNull(rs.getObject(i, ZonedDateTime.class));
            assertNull(rs.getObject(i, LocalDate.class));
            assertNull(rs.getObject(i, LocalTime.class));
            assertNull(rs.getObject(i, OffsetTime.class));
        }
        return rs;
    }



}

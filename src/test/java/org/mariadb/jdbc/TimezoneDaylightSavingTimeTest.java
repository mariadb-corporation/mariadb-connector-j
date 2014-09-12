package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TimezoneDaylightSavingTimeTest extends BaseTest {

    private Locale previousFormatLocale;
    private TimeZone previousTimeZone;
    private TimeZone utcTimeZone;
    private SimpleDateFormat utcDateFormatISO8601;
    private SimpleDateFormat utcDateFormatSimple;
    private TimeZone istanbulTimeZone;

    @Before
    public void setUp() throws SQLException {
        //Save the previous FORMAT locate so we can restore it later
        previousFormatLocale = Locale.getDefault();
        //Save the previous timezone so we can restore it later
        previousTimeZone = TimeZone.getDefault();
        
        //I have tried to represent all times written in the code in the UTC time zone
        utcTimeZone = TimeZone.getTimeZone("UTC");
        
        //For this test case I choose the Istanbul timezone because it show the fault in a good way.
        istanbulTimeZone = TimeZone.getTimeZone("Europe/Istanbul");
        TimeZone.setDefault(istanbulTimeZone);
        
        //Use a date formatter for UTC timezone in ISO 8601 so users in different
        //timezones can compare the test results easier.
        utcDateFormatISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        utcDateFormatISO8601.setTimeZone(utcTimeZone);
        
        utcDateFormatSimple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        utcDateFormatSimple.setTimeZone(utcTimeZone);
        
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists timestamptest");
        statement.execute("create table timestamptest (id int not null primary key auto_increment, tm timestamp)");
    }
    
    @After
    public void tearDown() {
        //Reset the FORMAT locate so other test cases are not disturbed.
        Locale.setDefault(previousFormatLocale);
        //Reset the timezone so so other test cases are not disturbed.
        TimeZone.setDefault(previousTimeZone);
    }
    
    
    /**
     * This method is designed to correctly insert a timestamp in the database. It manually forces
     * the session to be in UTC and transfers the timestamp as a String to minimize the impact the
     * JDBC code may have over it.
     */
    private void insertTimestampInWithUTCSessionTimeZone(Calendar timestamp) throws SQLException {
        //Totally reset of the connection to be sure everything is clean
        connection.close();
        
        before(); //Force a new Connection (may be replaced by a getConnection() method if CONJ-112 is implemented
        Statement statement = connection.createStatement();

        setSessionTimeZone(connection, "+00:00");
        
        //Use the "wrong" way to insert a timestamp: as a string. This is done to avoid possible bugs
        //in the PreparedStatement.setTimestamp(int parameterIndex, Timestamp x) method and to make
        //sure the value is a correct UTC time.
        String _timestamp = utcDateFormatSimple.format(new Date(timestamp.getTimeInMillis()));
        statement.execute("insert into timestamptest values(null, '" 
                        + _timestamp + "')");
        
        //Totally reset of the connection to be sure everything is clean
        statement.close();
        connection.close();
        before();
    }
    
    @Test
    public void testGetTimestampWhenDaylightSavingRemovesHour() throws SQLException {
        Calendar _0015 = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        _0015.clear();
        _0015.set(2014, 2, 30, 0, 15, 0);
        String _0015String = utcDateFormatISO8601.format(new Date(_0015.getTimeInMillis()));
        
        insertTimestampInWithUTCSessionTimeZone(_0015);

        Statement statement = connection.createStatement();
        setSessionTimeZone(connection, istanbulTimeZone, _0015);

        //Verify with ResultSet.getTimestamp() that it is correct
        ResultSet rs = statement.executeQuery("select * from timestamptest");

        assertTrue(rs.next());
        System.out.println(rs.getString("tm"));
        Timestamp timestamp = rs.getTimestamp("tm");
        String _timestampString = utcDateFormatISO8601.format(timestamp);
        assertEquals(_0015String, _timestampString);
    }
    
    @Test
    public void testGetTimestampWithoutDaylightSavingIssue() throws SQLException {
        Calendar _0115 = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        _0115.clear();
        _0115.set(2014, 2, 30, 1, 15, 0);
        String _0115String =  utcDateFormatISO8601.format(new Date(_0115.getTimeInMillis()));
        
        insertTimestampInWithUTCSessionTimeZone(_0115);

        Statement statement = connection.createStatement();
        setSessionTimeZone(connection, istanbulTimeZone, _0115);

        //Verify with ResultSet.getTimestamp() that it is correct
        ResultSet rs = statement.executeQuery("select * from timestamptest");

        assertTrue(rs.next());
        System.out.println(rs.getString("tm"));
        Timestamp timestamp = rs.getTimestamp("tm");
        assertEquals(_0115String, utcDateFormatISO8601.format(timestamp));
    }
    
    /**
     * Return the session timezone in a "+02:00" or "-05:00" format. 
     */
    private String getSessionTimeZone() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(
            "select CONVERT(timediff(now(),convert_tz(now(),@@session.time_zone,'+00:00')), CHAR)");
        
        resultSet.next();
        
        String timeZoneString = resultSet.getString(1);
        
        timeZoneString = timeZoneString.replaceAll(":00$", "");
        
        if (!timeZoneString.startsWith("-")) {
            timeZoneString = "+" + timeZoneString;
        }
        statement.close();
        
        return timeZoneString;
    }
    
    private void setSessionTimeZone(Connection connection, String timeZone) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("set @@session.time_zone = '" + timeZone + "'");
        statement.close();
    }

    private void setSessionTimeZone(Connection connection, TimeZone timeZone, Calendar cal) throws SQLException {
        //int offsetInMs = timeZone.getOffset(System.currentTimeMillis());
    	int offsetInMs = timeZone.getOffset(cal.get(Calendar.ERA), cal.get(Calendar.YEAR), cal.get(Calendar.MONTH),
    			cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.DAY_OF_WEEK), cal.get(Calendar.MILLISECOND));
        int offsetInHours = offsetInMs/3600/1000;
        
        boolean leadingZero = false;
        if (Math.abs(offsetInHours) < 10) {
            //Add a leading 0
             leadingZero = true;
        }
        
        boolean positive = true;
        if (offsetInHours < 0) {
            positive = false; 
        }
        
        String offsetString =  (positive ? "+" : "-") +
                        (leadingZero ? "0" : "") +
                        Math.abs(offsetInHours) +
                        ":00";

        setSessionTimeZone(connection, offsetString);
    }
    
}
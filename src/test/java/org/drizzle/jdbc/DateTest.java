package org.drizzle.jdbc;

import org.junit.Test;

import java.sql.*;

import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 1, 2009
 * Time: 10:01:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class DateTest {
    @Test
    public void dateTest() throws SQLException {
        try {
            Class.forName("org.drizzle.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("Could not load driver");
        }
        Connection connection = DriverManager.getConnection("jdbc:drizzle://localhost:4427/test_units_jdbc");

        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists date_test");
        stmt.executeUpdate("create table date_test (d_test date,t_test time, dt_test datetime)");
        Date date = Date.valueOf("2009-01-17");
        Time time = Time.valueOf("15:41:01");
        Timestamp timestamp = Timestamp.valueOf("2009-01-17 15:41:01");
        PreparedStatement ps = connection.prepareStatement("insert into date_test values (?,?,?)");
        ps.setDate(1,date);
        ps.setTime(2,time);
        ps.setTimestamp(3,timestamp);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select * from date_test");
        assertEquals(true,rs.next());
        Date date2 = rs.getDate(1);
        Time time2=rs.getTime(2);
        Timestamp timestamp2=rs.getTimestamp(3);
        assertEquals(date.toString(), date2.toString());
        assertEquals(time.toString(), time2.toString());
        assertEquals(timestamp.toString(), timestamp2.toString());

    }
}

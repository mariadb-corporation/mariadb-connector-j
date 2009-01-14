package org.drizzle.jdbc;

import org.junit.Test;

import java.sql.SQLException;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:58:11 AM
 */
public class DriverTest {
    @Test(expected=SQLException.class)
    public void connect() throws SQLException {
        Driver dr = new Driver();
        dr.connect("eh",null);

    }
}

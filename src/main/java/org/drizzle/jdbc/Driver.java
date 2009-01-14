package org.drizzle.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverPropertyInfo;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:46:09 AM
 */
public class Driver implements java.sql.Driver {
    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            throw new RuntimeException("Could not register driver",e);
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        throw new SQLException("Could not connect");
    }

    public boolean acceptsURL(String url) throws SQLException {
        return false;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    public int getMajorVersion() {
        return 0;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return false;
    }
}

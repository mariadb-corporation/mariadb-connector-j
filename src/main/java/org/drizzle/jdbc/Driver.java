package org.drizzle.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverPropertyInfo;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:46:09 AM
 */
public class Driver implements java.sql.Driver {
    private String hostname;
    private int port;
    private String username;
    private String password;
    private String database;

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            throw new RuntimeException("Could not register driver",e);
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        this.parseUrl(url);
        System.out.println(info);
        return new DrizzleConnection(hostname,port,"","",database);
        //throw new SQLException("Could not connect");
    }

    private void parseUrl(String url) throws SQLException {
        Pattern p = Pattern.compile("^jdbc:drizzle://([^/:]+)(:(\\d+))?(/(\\w+))");
        Matcher m=p.matcher(url);
        if(m.find()) {
            this.hostname = m.group(1);
            if(m.group(3) != null) {
                this.port = Integer.parseInt(m.group(3));
            } else {
                this.port=4427;
            }
            this.database = m.group(5);
        } else {
            throw new SQLException("Could not parse connection string...");
        }
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

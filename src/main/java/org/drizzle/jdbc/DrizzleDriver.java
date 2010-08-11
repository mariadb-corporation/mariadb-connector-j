/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.SQLExceptionMapper;
import org.drizzle.jdbc.internal.common.Protocol;
import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.query.DrizzleQueryFactory;
import org.drizzle.jdbc.internal.mysql.MySQLProtocol;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The base SQL DrizzleDriver class. User: marcuse Date: Jan 14, 2009 Time: 7:46:09 AM
 */
public final class DrizzleDriver implements java.sql.Driver {
    /**
     * the logger.
     */
    private static final Logger log = Logger.getLogger(DrizzleDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new DrizzleDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Could not register driver", e);
        }
    }

    /**
     * Connect to the given connection string.
     * <p/>
     * the properties are currently ignored
     *
     * @param url  the url to connect to
     * @param info the properties of the connection - ignored at the moment
     * @return a connection
     * @throws SQLException if it is not possible to connect
     */
    public Connection connect(final String url, final Properties info) throws SQLException {
        // TODO: handle the properties!
        // TODO: define what props we support!

        String baseUrl = url;
        int idx = url.lastIndexOf("?");        
        if(idx > 0) {
            baseUrl = url.substring(0,idx);
            String urlParams = url.substring(idx+1);
            setURLParameters(urlParams, info);
        }

        log.finest("Connecting to: " + url);
        try {
            final JDBCUrl jdbcUrl = JDBCUrl.parse(baseUrl);
            if(jdbcUrl == null) {
                return null;
            }
            String userName = info.getProperty("user",jdbcUrl.getUsername());
            String password = info.getProperty("password",jdbcUrl.getPassword());

            final Protocol protocol = new MySQLProtocol(jdbcUrl.getHostname(),
                        jdbcUrl.getPort(),
                        jdbcUrl.getDatabase(),
                        userName,
                        password,
                        info);

            return new DrizzleConnection(protocol, new DrizzleQueryFactory());
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        }
    }

    private void setURLParameters(String urlParameters, Properties info) {
        String [] parameters = urlParameters.split("&");
        for(String param : parameters) {
            String [] keyVal = param.split("=");
            info.setProperty(keyVal[0], keyVal[1]);
        }
    }

    /**
     * returns true if the driver can accept the url.
     *
     * @param url the url to test
     * @return true if the url is valid for this driver
     */
    public boolean acceptsURL(final String url) {
        return url.startsWith("jdbc:drizzle://")
                || url.startsWith("jdbc:mysql:thin://");
    }

    /**
     * get the property info. TODO: not implemented!
     *
     * @param url  the url to get properties for
     * @param info the info props
     * @return something - not implemented
     * @throws SQLException if there is a problem getting the property info
     */
    public DriverPropertyInfo[] getPropertyInfo(final String url,
                                                final Properties info)
            throws SQLException {
        return new DriverPropertyInfo[0];
    }

    /**
     * gets the major version of the driver.
     *
     * @return the major versions
     */
    public int getMajorVersion() {
        return 0;
    }

    /**
     * gets the minor version of the driver.
     *
     * @return the minor version
     */
    public int getMinorVersion() {
        return 1;
    }

    /**
     * checks if the driver is jdbc compliant (not yet!).
     *
     * @return false since the driver is not compliant
     */
    public boolean jdbcCompliant() {
        return false;
    }
}

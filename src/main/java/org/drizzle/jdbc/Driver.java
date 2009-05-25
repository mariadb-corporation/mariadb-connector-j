/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.drizzle.DrizzleProtocol;
import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.Protocol;
import org.drizzle.jdbc.internal.common.query.DrizzleQueryFactory;
import org.drizzle.jdbc.internal.mysql.MySQLProtocol;
import org.drizzle.jdbc.internal.SQLExceptionMapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverPropertyInfo;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:46:09 AM
 */
public class Driver implements java.sql.Driver {
    private static final Logger log = Logger.getLogger(Driver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            throw new RuntimeException("Could not register driver",e);
        }
    }

    /**
     * Connect to the given connection string
     *
     * the properties are currently ignored
     *
     * @param url the url to connect to
     * @param info the properties of the connection - ignored at the moment
     * @return a connection
     * @throws SQLException if it is not possible to connect
     */
    public Connection connect(String url, Properties info) throws SQLException {
        // TODO: handle the properties!
        // TODO: define what props we support!
        log.finest("Connecting to: "+url);

        try {
            JDBCUrl jdbcUrl = new JDBCUrl(url);
            Protocol protocol;
            if(jdbcUrl.getDBType()==JDBCUrl.DBType.DRIZZLE)
                protocol = new DrizzleProtocol(jdbcUrl.getHostname(), jdbcUrl.getPort(), jdbcUrl.getDatabase(), jdbcUrl.getUsername(), jdbcUrl.getPassword());
            else
                protocol = new MySQLProtocol(jdbcUrl.getHostname(), jdbcUrl.getPort(), jdbcUrl.getDatabase(), jdbcUrl.getUsername(), jdbcUrl.getPassword());
            return new DrizzleConnection(protocol, new DrizzleQueryFactory());
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        }
    }

    /**
     * returns true if the driver can accept the url
     * @param url the url to test
     * @return true if the url is valid for this driver
     * @throws SQLException not in this implementation
     */
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith("jdbc:drizzle://") || url.startsWith("jdbc:mysql:thin://");
    }

    /**
     * get the property info
     * @param url
     * @param info
     * @return
     * @throws SQLException
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }


    public int getMajorVersion() {
        return 0;
    }

    public int getMinorVersion() {
        return 1;
    }

    public boolean jdbcCompliant() {
        return false;
    }
}

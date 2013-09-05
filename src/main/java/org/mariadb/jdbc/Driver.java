/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.mysql.MySQLProtocol;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;


public final class Driver implements java.sql.Driver {
    /**
     * the logger.
     */
    private static final Logger log = Logger.getLogger(Driver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new Driver());
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

            MySQLProtocol protocol = new MySQLProtocol(jdbcUrl, userName,  password,  info);

            return MySQLConnection.newConnection(protocol);
        } catch (QueryException e) {
            SQLExceptionMapper.throwException(e, null, null);
            return null;
        }
    }

    private void setURLParameters(String urlParameters, Properties info) {
        String [] parameters = urlParameters.split("&");
        for(String param : parameters) {
            int pos = param.indexOf('=');
            if (pos == -1)  {
                throw new IllegalArgumentException("Invalid connection URL, expected key=value pairs, found " + param);
            }
            info.setProperty(param.substring(0, pos), param.substring(pos + 1));
        }
    }

    /**
     * returns true if the driver can accept the url.
     *
     * @param url the url to test
     * @return true if the url is valid for this driver
     */
    public boolean acceptsURL(String url) {
        return  JDBCUrl.acceptsURL(url);
    }

    /**
     * get the property info. TODO: not implemented!
     *
     * @param url  the url to get properties for
     * @param info the info props
     * @return something - not implemented
     * @throws SQLException if there is a problem getting the property info
     */
    public DriverPropertyInfo[] getPropertyInfo(String url,
                                                Properties info)
            throws SQLException {
        return new DriverPropertyInfo[0];
    }

    /**
     * gets the major version of the driver.
     *
     * @return the major versions
     */
    public int getMajorVersion() {
        return 1;
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

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}

    /*
        Provide a "cleanup" method that can be called after unloading driver, to fix Tomcat's obscure classpath handling.

        See CONJ-61.
     */
    public static void unloadDriver() {
        MySQLStatement.unloadDriver();
    }
}

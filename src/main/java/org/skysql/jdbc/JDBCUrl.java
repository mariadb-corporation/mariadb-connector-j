/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.skysql.jdbc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a jdbc url.
 * <p/>
 * User: marcuse Date: Apr 21, 2009 Time: 9:32:34 AM
 */
public class JDBCUrl {
    private DBType dbType;
    private String username;
    private String password;
    private String database;
    private HostAddress addresses[];

    public enum DBType {
        DRIZZLE, MYSQL
    }

    private JDBCUrl(DBType dbType, String username, String password, String database, HostAddress addresses[]) {
        this.dbType = dbType;
        this.username = username;
        this.password = password;
        this.database = database;
        this.addresses = addresses;
    }

    /*
    Parse ConnectorJ compatible urls
    jdbc:mysql://host:port/database

     */
    private static JDBCUrl parseConnectorJUrl(String url) {
        Pattern p = Pattern.compile("^jdbc:mysql://([A-Za-z0-9._,-:]+)?(/\\w+)?");
        Matcher m = p.matcher(url);
        if (m.find()){
           String hostname = m.group(1);
           HostAddress addresses[] = null;
           if (hostname.indexOf(',') != -1)	{
               // We have a failover list
               addresses = HostAddress.parse(hostname);
           }
           String database = null;
           if ( m.group(2)!= null) {
               database=m.group(2).substring(1);
           }
           return new JDBCUrl(DBType.MYSQL, "", "",  database, addresses);
        }
        return null;
    }

    public static JDBCUrl parse(final String url) {
        final DBType dbType;
        final String username;
        final String password;
        final String hostname;
        final int port;
        final String database;        

        if(url.startsWith("jdbc:mysql://")) {
            return parseConnectorJUrl(url);
        }
        final Pattern p = Pattern.compile("^jdbc:(drizzle|mysql:thin)://((\\w+)(:(\\w*))?@)?([^/:]+)(:(\\d+))?(/(\\w+))?");
        final Matcher m = p.matcher(url);
        if (m.find()) {
            if (m.group(1).equals("mysql:thin")) {
                dbType = DBType.MYSQL;
            } else {
                dbType = DBType.DRIZZLE;
            }

            username = (m.group(3) == null ? "" : m.group(3));
            password = (m.group(5) == null ? "" : m.group(5));
            hostname = (m.group(6) == null ? "" : m.group(6));
            if (m.group(8) != null) {
                port = Integer.parseInt(m.group(8));
            } else {
                if (dbType == DBType.DRIZZLE) {
                    port = 3306;
                } else {
                    port = 3306;
                }
            }
            database = m.group(10);
            HostAddress addresses[] = new HostAddress[1];
            addresses[0].host = hostname;
            addresses[0].port = port;
            return new JDBCUrl(dbType, username, password, database, addresses);
        } else {
            return null;
        }
    }
    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getHostname() {
        return addresses[0].host;
    }

    public int getPort() {
        return addresses[0].port;
    }

    public String getDatabase() {
        return database;
    }

    public DBType getDBType() {
        return this.dbType;
    }

    public HostAddress[] getHostAddresses() {
	return this.addresses;
    }

}

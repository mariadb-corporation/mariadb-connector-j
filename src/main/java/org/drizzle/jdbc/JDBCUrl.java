/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a jdbc url.
 * <p/>
 * User: marcuse Date: Apr 21, 2009 Time: 9:32:34 AM
 */
public class JDBCUrl {
    private final DBType dbType;
    private final String username;
    private final String password;
    private final String hostname;
    private final int port;
    private final String database;

    public enum DBType {
        DRIZZLE, MYSQL
    }

    private JDBCUrl(DBType dbType, String username, String password, String hostname, int port, String database) {
        this.dbType = dbType;
        this.username = username;
        this.password = password;
        this.hostname = hostname;
        this.port = port;
        this.database = database;
    }

    public static JDBCUrl parse(final String url) {
        final DBType dbType;
        final String username;
        final String password;
        final String hostname;
        final int port;
        final String database;        

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
            return new JDBCUrl(dbType, username, password, hostname, port, database);
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
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public DBType getDBType() {
        return this.dbType;
    }

}
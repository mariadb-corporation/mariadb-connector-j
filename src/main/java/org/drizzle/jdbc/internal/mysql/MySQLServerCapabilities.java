/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.mysql;

import java.util.EnumSet;
import java.util.Set;

/**
 * Represents what the server is capable of
 * <p/>
 * User: marcuse Date: Feb 27, 2009 Time: 9:01:29 PM
 */
public enum MySQLServerCapabilities {
    LONG_PASSWORD((int) 1),       /* new more secure passwords */
    FOUND_ROWS((int) 2),       /* Found instead of affected rows */
    LONG_FLAG((int) 4),       /* Get all column flags */
    CONNECT_WITH_DB((int) 8),       /* One can specify db on connect */
    NO_SCHEMA((int) 16),      /* Don't allow database.table.column */
    COMPRESS((int) 32),      /* Can use compression protocol */
    ODBC((int) 64),      /* Odbc client */
    LOCAL_FILES((int) 128),     /* Can use LOAD DATA LOCAL */
    IGNORE_SPACE((int) 256),     /* Ignore spaces before '(' */
    CLIENT_PROTOCOL_41((int) 512),     /* New 4.1 protocol */
    CLIENT_INTERACTIVE((int) 1024),
    SSL((int) 2048),    /* Switch to SSL after handshake */
    IGNORE_SIGPIPE((int) 4096),    /* IGNORE sigpipes */
    TRANSACTIONS((int) 8192),
    RESERVED((int) 16384),   /* Old flag for 4.1 protocol  */
    SECURE_CONNECTION((int) 32768),  /* New 4.1 authentication */
    MULTI_STATEMENTS((int) (1L << 16)), /* Enable/disable multi-stmt support */
    MULTI_RESULTS((int) (1L << 17)), /* Enable/disable multi-results */
    DRIZZLE_CAPABILITIES_ADMIN((int) (1L<< 25));

    private final int bitmapFlag;

    MySQLServerCapabilities(final int i) {
        this.bitmapFlag = i;
    }

    /**
     * creates an enum set of the server capabilities represented by i
     *
     * @param i the value from the server
     * @return an enum set containing the flags in i
     */
    public static Set<MySQLServerCapabilities> getServerCapabilitiesSet(final short i) {
        final Set<MySQLServerCapabilities> statusSet = EnumSet.noneOf(MySQLServerCapabilities.class);
        for (final MySQLServerCapabilities value : MySQLServerCapabilities.values()) {
            if ((i & value.getBitmapFlag()) == value.getBitmapFlag()) {
                statusSet.add(value);
            }
        }
        return statusSet;
    }

    /**
     * the raw bit map flag from the server
     *
     * @return the raw map flag
     */
    public int getBitmapFlag() {
        return bitmapFlag;
    }

    /**
     * creates a bitmasked short from an enum set
     *
     * @param capabilities
     * @return
     */
    public static int fromSet(final Set<MySQLServerCapabilities> capabilities) {
        int retVal = 0;
        for (final MySQLServerCapabilities cap : capabilities) {
            retVal = (retVal | cap.getBitmapFlag());
        }
        return retVal;
    }
}
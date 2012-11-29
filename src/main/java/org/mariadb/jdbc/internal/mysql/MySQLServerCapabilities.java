/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab. All Rights Reserved.

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

All rights reserved.

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

package org.mariadb.jdbc.internal.mysql;

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
/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

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

package org.mariadb.jdbc.internal;

public class MariaDbServerCapabilities {
    public static final int CLIENT_MYSQL = 1;
    public static final int FOUND_ROWS = 2;       /* Found instead of affected rows */
    public static final int LONG_FLAG = 4;       /* Get all column flags */
    public static final int CONNECT_WITH_DB = 8;     /* One can specify db on connect */
    public static final int NO_SCHEMA = 16;          /* Don't allow database.table.column */
    public static final int COMPRESS = 32;          /* Can use compression protocol */
    public static final int ODBC = 64;               /* Odbc client */
    public static final int LOCAL_FILES = 128;       /* Can use LOAD DATA LOCAL */
    public static final int IGNORE_SPACE = 256;       /* Ignore spaces before '(' */
    public static final int CLIENT_PROTOCOL_41 = 512; /* New 4.1 protocol */
    public static final int CLIENT_INTERACTIVE = 1024;
    public static final int SSL = 2048;                /* Switch to SSL after handshake */
    public static final int IGNORE_SIGPIPE = 4096;     /* IGNORE sigpipes */
    public static final int TRANSACTIONS = 8192;
    public static final int RESERVED = 16384;           /* Old flag for 4.1 protocol  */
    public static final int SECURE_CONNECTION = 32768;  /* New 4.1 authentication */
    public static final int MULTI_STATEMENTS = 1 << 16; /* Enable/disable multi-stmt support */
    public static final int MULTI_RESULTS = 1 << 17;    /* Enable/disable multi-results */
    public static final int PS_MULTI_RESULTS = 1 << 18; /* Enable/disable multi-results for PrepareStatement */
    public static final int PLUGIN_AUTH = 1 << 19;      /* Client supports plugin authentication */
    public static final int CONNECT_ATTRS = 1 << 20;    /* Client send connection attributes */
    public static final int PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;    /* authentication data length is a length auth integer */
    public static final int CLIENT_SESSION_TRACK = 1 << 23; /* server send session tracking info */
    public static final int CLIENT_DEPRECATE_EOF = 1 << 24; /* EOF packet deprecated */
    public static final int PROGRESS_OLD = 1 << 29;         /* Client support progress indicator (before 10.2)*/

    /* MariaDB specific capabilities */
    public static final long MARIADB_CLIENT_PROGRESS = 1L << 32; /* Client support progress indicator (since 10.2) */
    public static final long MARIADB_CLIENT_COM_MULTI = 1L << 33; /* bundle command during connection */

}
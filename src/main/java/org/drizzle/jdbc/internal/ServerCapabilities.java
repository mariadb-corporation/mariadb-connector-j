package org.drizzle.jdbc.internal;

import java.util.Set;
import java.util.EnumSet;

/**
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 9:01:29 PM
 */
public enum ServerCapabilities {
    LONG_PASSWORD((short)1),       /* new more secure passwords */
    FOUND_ROWS((short)2),       /* Found instead of affected rows */
    LONG_FLAG((short)4),       /* Get all column flags */
    CONNECT_WITH_DB((short)8),       /* One can specify db on connect */
    NO_SCHEMA((short)16),      /* Don't allow database.table.column */
    COMPRESS((short)32),      /* Can use compression protocol */
    ODBC((short)64),      /* Odbc client */
    LOCAL_FILES((short)128),     /* Can use LOAD DATA LOCAL */
    IGNORE_SPACE((short)256),     /* Ignore spaces before '(' */
    CLIENT_PROTOCOL_41((short)512),     /* New 4.1 protocol */
    INTERACTIVE((short)1024),    /* This is an interactive client */
    SSL((short)2048),    /* Switch to SSL after handshake */
    IGNORE_SIGPIPE((short)4096),    /* IGNORE sigpipes */
    TRANSACTIONS((short)8192),    /* Client knows about transactions */
    RESERVED((short)16384),   /* Old flag for 4.1 protocol  */
    SECURE_CONNECTION((short)32768),  /* New 4.1 authentication */
    MULTI_STATEMENTS((short)(1L << 16)), /* Enable/disable multi-stmt support */
    MULTI_RESULTS((short)(1L << 17)); /* Enable/disable multi-results */
//    SSL_VERIFY_SERVER_CERT ((short)(1L << 30)), //TODO: uh, not used i hope
//    REMEMBER_OPTIONS ((short)(1L << 31));

    private short bitmapFlag;

    ServerCapabilities(short i) {
        this.bitmapFlag = i;
    }

    public static Set<ServerCapabilities> getServerCapabilitiesSet(short i) {
        Set<ServerCapabilities> statusSet = EnumSet.noneOf(ServerCapabilities.class);
        for(ServerCapabilities value : ServerCapabilities.values())
            if((i & value.getBitmapFlag()) == value.getBitmapFlag())
                statusSet.add(value);
        return statusSet;
    }


    public short getBitmapFlag() {
        return bitmapFlag;
    }

    public static short fromSet(Set<ServerCapabilities> capabilities) {
        short retVal = 0;
        for(ServerCapabilities cap : capabilities) {
            retVal = (short) (retVal | cap.getBitmapFlag());
        }
        return retVal;
    }
}

/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

import java.util.EnumSet;
import java.util.Set;

/**
 * Represents the server status
 * <p/>
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 8:36:46 PM
 */
public enum ServerStatus {
    IN_TRANSACTION((short) 1),
    AUTOCOMMIT((short) 2),
    MORE_RESULTS_EXISTS((short) 8),
    QUERY_NO_GOOD_INDEX_USED((short) 16),
    QUERY_NO_INDEX_USED((short) 32),
    CURSOR_EXISTS((short) 64),
    LAST_ROW_SENT((short) 128),
    DB_DROPPED((short) 256),
    NO_BACKSLASH_ESCAPES((short) 512),
    QUERY_WAS_SLOW((short) 1024);


    private short bitmapFlag;

    ServerStatus(short i) {
        this.bitmapFlag = i;
    }

    /**
     * returns the bit map flag
     *
     * @return the bitmap flag
     */
    public short getBitmapFlag() {
        return bitmapFlag;
    }

    /**
     * creates an enum set of the bitmasked field i
     *
     * @param i the bitmasked field
     * @return an enum set with the flags defined by i
     */
    public static Set<ServerStatus> getServerStatusSet(short i) {
        Set<ServerStatus> statusSet = EnumSet.noneOf(ServerStatus.class);
        for (ServerStatus value : ServerStatus.values())
            if ((i & value.getBitmapFlag()) == value.getBitmapFlag())
                statusSet.add(value);
        return statusSet;
    }
}

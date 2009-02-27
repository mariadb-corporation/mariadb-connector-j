package org.drizzle.jdbc.internal;

import java.util.Set;
import java.util.EnumSet;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 8:36:46 PM
 * To change this template use File | Settings | File Templates.
 */
public enum ServerStatus {
    IN_TRANSACTION((short)1),
    AUTOCOMMIT((short)2),
    MORE_RESULTS_EXISTS((short)8),
    QUERY_NO_GOOD_INDEX_USED((short)16),
    QUERY_NO_INDEX_USED((short)32),
    CURSOR_EXISTS((short)64),
    LAST_ROW_SENT((short)128),
    DB_DROPPED((short)256),
    NO_BACKSLASH_ESCAPES((short)512),
    QUERY_WAS_SLOW((short)1024);


    private short bitmapFlag;

    ServerStatus(short i) {
        this.bitmapFlag = i;
    }

    public short getBitmapFlag() {
        return bitmapFlag;
    }

    public static Set<ServerStatus> getServerStatusSet(short i) {
        Set<ServerStatus> statusSet = EnumSet.noneOf(ServerStatus.class);
        for(ServerStatus value : ServerStatus.values())
            if((i & value.getBitmapFlag()) == value.getBitmapFlag())
                statusSet.add(value);
        return statusSet;
    }
}

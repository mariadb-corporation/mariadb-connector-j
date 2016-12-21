package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.protocol.Protocol;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by kolzeq on 16/12/2016.
 */
public interface CmdInformation {
    public static final int NO_UPDATE_COUNT = -1;

    int[] getUpdateCounts();

    int getUpdateCount();

    void addStats(int updateCount, long insertId);

    void addStats(int updateCount);

    ResultSet getGeneratedKeys(Protocol protocol);

    int getCurrentStatNumber();

    boolean moreResults();

    boolean isCurrentUpdateCount();
}

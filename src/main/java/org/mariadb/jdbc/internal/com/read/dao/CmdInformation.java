package org.mariadb.jdbc.internal.com.read.dao;

import org.mariadb.jdbc.internal.protocol.Protocol;

import java.sql.ResultSet;

/**
 * Created by kolzeq on 16/12/2016.
 */
public interface CmdInformation {
    public static final int RESULT_SET_VALUE = -1;

    int[] getUpdateCounts();

    int[] getRewriteUpdateCounts();

    int getUpdateCount();

    void addSuccessStat(int updateCount, long insertId);

    void addErrorStat();

    void addResultSetStat();

    ResultSet getGeneratedKeys(Protocol protocol);

    ResultSet getBatchGeneratedKeys(Protocol protocol);

    int getCurrentStatNumber();

    boolean moreResults();

    boolean isCurrentUpdateCount();

}

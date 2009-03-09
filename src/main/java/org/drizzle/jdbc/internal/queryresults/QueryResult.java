package org.drizzle.jdbc.internal.queryresults;

import org.drizzle.jdbc.internal.packet.FieldPacket;
import org.drizzle.jdbc.internal.ValueObject;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 5, 2009
 * Time: 10:20:03 PM
 * To change this template use File | Settings | File Templates.
 */
public interface QueryResult {
    public ResultSetType getResultSetType();
     public void close();
    short getWarnings();
    String getMessage();
/*
    boolean next();
    void close();

    List<FieldPacket> getFieldPackets();

    ValueObject getValueObject(int i);

    ValueObject getValueObject(String column);

    void setUpdateCount(int updateCount);

    long getUpdateCount();

    void setInsertId(long insertId);

    long getInsertId();

    int getRows();

    void setWarnings(int warnings);

    void setMessage(String message);

    short getWarnings();

    String getMessage();

    QueryResult getGeneratedKeysResult();

    int getColumnId(String columnLabel);

    int getRowPointer();

    void moveRowPointerTo(int i);*/

    List<ColumnInformation> getColumnInformation();

    int getRows();
}

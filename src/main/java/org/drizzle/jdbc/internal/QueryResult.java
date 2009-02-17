package org.drizzle.jdbc.internal;

import org.drizzle.jdbc.internal.packet.FieldPacket;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 5, 2009
 * Time: 10:20:03 PM
 * To change this template use File | Settings | File Templates.
 */
public interface QueryResult {
    boolean next();
    void close();
    void addRow(List<ValueObject> row);

    List<FieldPacket> getFieldPackets();

    ValueObject getValueObject(int i);

    ValueObject getValueObject(String column);

    void setUpdateCount(int updateCount);

    int getUpdateCount();

    void setInsertId(long insertId);

    long getInsertId();

    int getRows();

    void setWarnings(int warnings);

    void setMessage(String message);

    int getWarnings();

    String getMessage();

    QueryResult getGeneratedKeysResult();

    int getColumnId(String columnLabel);

    int getRowPointer();

    void moveRowPointerTo(int i);
}

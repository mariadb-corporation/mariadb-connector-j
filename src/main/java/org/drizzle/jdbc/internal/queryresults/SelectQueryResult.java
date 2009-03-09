package org.drizzle.jdbc.internal.queryresults;

import org.drizzle.jdbc.internal.ValueObject;
import org.drizzle.jdbc.internal.packet.FieldPacket;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 8:42:45 PM
 * To change this template use File | Settings | File Templates.
 */
public interface SelectQueryResult extends QueryResult {
    ValueObject getValueObject(int index);
    ValueObject getValueObject(String columnName);
    int getRows();
    int getColumnId(String columnLabel);
    void moveRowPointerTo(int i);
    int getRowPointer();
    boolean next();
    // TODO: make field packet a column descriptor factory instead, don't use FieldPackets as a general
    // TODO: column information holder
    public List<ColumnInformation> getColumnInformation();


}

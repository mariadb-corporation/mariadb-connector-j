package org.drizzle.jdbc.internal.common.queryresults;

import org.drizzle.jdbc.internal.common.ValueObject;
import org.drizzle.jdbc.internal.common.PacketFetcher;
import org.drizzle.jdbc.internal.drizzle.packet.RawPacket;
import org.drizzle.jdbc.internal.drizzle.packet.RowPacket;
import org.drizzle.jdbc.internal.drizzle.packet.buffer.ReadUtil;

import java.util.*;

/**
 * TODO: refactor, badly need to split this into two/three different classes, one for insert/update/ddl, one for selects and one for generated keys?
 *
 * User: marcuse
 * Date: Jan 23, 2009
 * Time: 8:15:55 PM
 */
public class DrizzleAsyncQueryResult implements SelectQueryResult {
    private final List<ColumnInformation> columnInformation;
    private List<ValueObject> currentRow;
    private final Map<String, Integer> columnNameMap;
    private int rowPointer;
    private final PacketFetcher packetFetcher;
    private boolean hasReadEOF=false;

    public DrizzleAsyncQueryResult(List<ColumnInformation> columnInformation, PacketFetcher packetFetcher) {
        this.columnInformation = Collections.unmodifiableList(columnInformation);
        this.packetFetcher=packetFetcher;
        columnNameMap=new HashMap<String,Integer>();
        rowPointer =-1;
        int i=0;
        for(ColumnInformation ci : columnInformation) {
            columnNameMap.put(ci.getName().toLowerCase(),i++);
        }
    }

    public boolean next() {
        RawPacket rawPacket = packetFetcher.getRawPacket();
        if(ReadUtil.eofIsNext(rawPacket)) {
            hasReadEOF=true;
            return false;
        }
        RowPacket rowPacket = new RowPacket(rawPacket,columnInformation);
        currentRow=rowPacket.getRow();
        rowPointer++;
        return true;
    }

    public void close() {
        while(!hasReadEOF) {
            RawPacket rp  = packetFetcher.getRawPacket();
            if(ReadUtil.eofIsNext(rp)) {
                this.hasReadEOF=true;
            }
        }
    }

    public short getWarnings() {
        return 0;
    }

    public String getMessage() {
        return null;
    }

    public List<ColumnInformation> getColumnInformation() {
        return columnInformation;
    }

    /**
     * gets the value at position i in the result set. i starts at zero!
     * @param i index, starts at 0
     * @return
     */
    public ValueObject getValueObject(int i) throws NoSuchColumnException {
        if(i<0 || i > currentRow.size())
            throw new NoSuchColumnException("No such column: "+i);
        return currentRow.get(i);
    }

    public ValueObject getValueObject(String column) throws NoSuchColumnException {
        if(columnNameMap.get(column.toLowerCase())==null)
            throw new NoSuchColumnException("No such column: "+column);
        return getValueObject(columnNameMap.get(column.toLowerCase()));
    }

    public int getRows() {
        return -1;
    }

    public int getColumnId(String columnLabel) throws NoSuchColumnException {
        if(columnNameMap.get(columnLabel.toLowerCase())==null)
            throw new NoSuchColumnException("No such column: "+columnLabel);
        return columnNameMap.get(columnLabel.toLowerCase());
    }

    public void moveRowPointerTo(int i) {
        this.rowPointer =i;
    }

    public int getRowPointer() {
        return rowPointer;
    }


    public ResultSetType getResultSetType() {
        return ResultSetType.SELECT;
    }
}
package org.drizzle.jdbc.internal.queryresults;

import org.drizzle.jdbc.internal.ValueObject;
import org.drizzle.jdbc.internal.DrizzleType;
import org.drizzle.jdbc.internal.DrizzleValueObject;
import org.drizzle.jdbc.internal.packet.FieldPacket;

import java.util.List;
import java.util.Arrays;
import java.util.EnumSet;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 8:34:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrizzleInsertIdQueryResult implements SelectQueryResult {
    private final List<ColumnInformation> columnInformation;
    private final long insertId;

    public DrizzleInsertIdQueryResult(long insertId){
        this.insertId=insertId;
        ColumnInformation ci = new DrizzleColumnInformation.Builder().catalog("").charsetNumber((short)0).db("").decimals((byte)0).flags(EnumSet.of(ColumnFlags.AUTO_INCREMENT)).length(10).name("insert_id").originalName("insert_id").originalTable("").type(DrizzleType.LONG).build();
        columnInformation=Arrays.asList(ci);
    }

    public ValueObject getValueObject(int index) {
        return DrizzleValueObject.fromLong(insertId);
    }

    public ValueObject getValueObject(String columnName) {
        return DrizzleValueObject.fromLong(insertId);
    }

    public int getRows() {
        return 1;
    }

    public int getColumnId(String columnLabel) {
        if(columnLabel.equals("insert_id")) return 1;
        return 0;
    }

    public void moveRowPointerTo(int i) {
        
    }

    public int getRowPointer() {
        return 0;
    }

    public boolean next() {
        return true;
    }

    public List<ColumnInformation> getColumnInformation() {
        return columnInformation;
    }

    public ResultSetType getResultSetType() {
        return ResultSetType.SELECT;
    }

    public void close() {

    }

    public short getWarnings() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getMessage() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

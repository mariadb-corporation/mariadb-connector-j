/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.queryresults;

import org.drizzle.jdbc.internal.common.ValueObject;
import org.drizzle.jdbc.internal.drizzle.DrizzleType;
import org.drizzle.jdbc.internal.common.DrizzleValueObject;

import java.util.List;
import java.util.Arrays;
import java.util.EnumSet;

/**
 .
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 8:34:44 PM

 */
public class DrizzleInsertIdQueryResult implements SelectQueryResult {
    private final List<ColumnInformation> columnInformation;
    private final long insertId;

    public DrizzleInsertIdQueryResult(long insertId){
        this.insertId=insertId;
        ColumnInformation ci = new DrizzleColumnInformation.Builder().catalog("").charsetNumber((short)0).db("").decimals((byte)0).flags(EnumSet.of(ColumnFlags.AUTO_INCREMENT)).length(10).name("insert_id").originalName("insert_id").originalTable("").type(DrizzleType.LONG).build();
        columnInformation=Arrays.asList(ci);
    }

    public ValueObject getValueObject(int index) throws NoSuchColumnException {
        if(index!=0) throw new NoSuchColumnException("No such column: "+index);
        return DrizzleValueObject.fromLong(insertId);
    }

    public ValueObject getValueObject(String columnName) throws NoSuchColumnException {
        if(!columnName.toLowerCase().equals("insert_id")) throw new NoSuchColumnException("No such column: "+columnName);
        return DrizzleValueObject.fromLong(insertId);
    }

    public int getRows() {
        return 1;
    }

    public int getColumnId(String columnLabel) throws NoSuchColumnException {
        if(columnLabel.equals("insert_id")) return 0;
        throw new NoSuchColumnException("No such column");
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
        return 0;
    }

    public String getMessage() {
        return null;
    }
}

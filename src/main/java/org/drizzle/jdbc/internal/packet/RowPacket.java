package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.ValueObject;
import org.drizzle.jdbc.internal.DrizzleValueObject;
import org.drizzle.jdbc.internal.queryresults.ColumnInformation;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/**
 * User: marcuse
 * Date: Jan 23, 2009
 * Time: 9:28:43 PM
 */
public class RowPacket {
    private final List<ValueObject> columns = new ArrayList<ValueObject>();
    /*public RowPacket(InputStream istream, List<ColumnInformation> columnInformation) throws IOException {
        int fieldCount = columnInformation.size();
        Reader reader = new Reader(istream);
        for(int i = 0;i<fieldCount;i++){
            ColumnInformation currentColumn = columnInformation.get(i);
            byte [] col  = reader.getLengthEncodedBytes();
            DrizzleValueObject dvo = new DrizzleValueObject(col, currentColumn.getType());
            columns.add(dvo);
            currentColumn.updateDisplaySize(dvo.getDisplayLength());
        }
    }*/
    public RowPacket(RawPacket rawPacket, List<ColumnInformation> columnInformation) throws IOException {
        int fieldCount = columnInformation.size();
        System.out.println("field count: "+fieldCount);
        byte [] rawBytes = rawPacket.getRawBytes();
        int readBytes=0;
        for(int i = 0;i<fieldCount;i++){
            ColumnInformation currentColumn = columnInformation.get(i);
            int lenEncLen = ReadUtil.getLengthEncodedByteLength(rawBytes,readBytes);
            byte [] col  = ReadUtil.getLengthEncodedBytes(rawBytes,readBytes);
            readBytes+=lenEncLen;
            DrizzleValueObject dvo = new DrizzleValueObject(col, currentColumn.getType());
            System.out.println("got vo: "+dvo.getString());
            columns.add(dvo);
            currentColumn.updateDisplaySize(dvo.getDisplayLength());
        }
    }

    public List<ValueObject> getRow() {
        return columns;
    }

}

package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.LengthEncodedBytes;
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
      private final List<ValueObject> columns;

    public RowPacket(RawPacket rawPacket, List<ColumnInformation> columnInformation) {
        //int fieldCount = columnInformation.size();
        columns = new ArrayList<ValueObject>(columnInformation.size());
        byte [] rawBytes = rawPacket.getRawBytes();
        int readBytes=0;
        for (ColumnInformation currentColumn : columnInformation) {
            LengthEncodedBytes leb = ReadUtil.getLengthEncodedBytes(rawBytes, readBytes);
            readBytes += leb.getLength();

            DrizzleValueObject dvo = new DrizzleValueObject(leb.getBytes(), currentColumn.getType());
            columns.add(dvo);
            currentColumn.updateDisplaySize(dvo.getDisplayLength());
        }
    }

    public List<ValueObject> getRow() {
        return columns;
    }

}

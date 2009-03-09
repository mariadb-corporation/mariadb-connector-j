package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.Reader;
import org.drizzle.jdbc.internal.ValueObject;
import org.drizzle.jdbc.internal.DrizzleValueObject;
import org.drizzle.jdbc.internal.queryresults.ColumnInformation;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.InputStream;

/**
 * User: marcuse
 * Date: Jan 23, 2009
 * Time: 9:28:43 PM
 */
public class RowPacket {
    private List<ValueObject> columns = new ArrayList<ValueObject>();
    public RowPacket(InputStream istream, List<ColumnInformation> columnInformation) throws IOException {
        int fieldCount = columnInformation.size();
        Reader reader = new Reader(istream);
        for(int i = 0;i<fieldCount;i++){
            ColumnInformation currentColumn = columnInformation.get(i);
            byte [] col  = reader.getLengthEncodedBytes();
            DrizzleValueObject dvo = new DrizzleValueObject(col, currentColumn.getType());
            columns.add(dvo);
            currentColumn.updateDisplaySize(dvo.getDisplayLength());
        }
    }

    public List<ValueObject> getRow() {
        return columns;
    }

}

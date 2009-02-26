package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.Reader;
import org.drizzle.jdbc.internal.ValueObject;
import org.drizzle.jdbc.internal.DrizzleValueObject;
import static org.drizzle.jdbc.internal.DrizzleValueObject.DataType.STRING;

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
    public RowPacket(InputStream istream, List<FieldPacket> fieldPackets) throws IOException {
        int fieldCount = fieldPackets.size();
        Reader reader = new Reader(istream);
        for(int i = 0;i<fieldCount;i++){
            FieldPacket currentField = fieldPackets.get(i);
            byte [] col  = reader.getLengthEncodedBytes();
            DrizzleValueObject dvo = new DrizzleValueObject(col, STRING);
            columns.add(dvo);
            currentField.updateDisplaySize(dvo.getDisplayLength());
        }
    }

    public List<ValueObject> getRow() {
        return columns;
    }

}

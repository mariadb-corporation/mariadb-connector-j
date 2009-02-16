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
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 23, 2009
 * Time: 9:28:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class RowPacket {
    private List<ValueObject> columns = new ArrayList<ValueObject>();
    public RowPacket(InputStream istream, long fieldCount) throws IOException {
        Reader reader = new Reader(istream);
        for(int i = 0;i<fieldCount;i++){
            byte [] col  = reader.getLengthEncodedBytes();
            columns.add(new DrizzleValueObject(col, STRING));
        }
    }

    public List<ValueObject> getRow() {
        return columns;
    }

}

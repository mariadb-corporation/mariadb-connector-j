package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.Reader;

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
    private List<String> columns = new ArrayList<String>();
    public RowPacket(InputStream istream, long fieldCount) throws IOException {
        Reader reader = new Reader(istream);
        for(int i = 0;i<fieldCount;i++){
            String col = reader.getLengthEncodedString();
            columns.add(col);
        }
    }

    public List<String> getRow() {
        return columns;
    }
}

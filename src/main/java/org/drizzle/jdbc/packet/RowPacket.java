package org.drizzle.jdbc.packet;

import org.drizzle.jdbc.packet.buffer.ReadBuffer;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 23, 2009
 * Time: 9:28:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class RowPacket {
    private List<String> columns = new ArrayList<String>();
    private ReadBuffer readBuffer;
    public RowPacket(ReadBuffer readBuffer, long fieldCount) throws IOException {
        this.readBuffer=readBuffer;
        for(int i = 0;i<fieldCount;i++){
            String col = readBuffer.getLengthEncodedString();
            //System.out.println(col);
            columns.add(col);
        }


        //String col = readBuffer.getLengthEncodedString();
    }

    public List<String> getRow() {
        return columns;
    }
}

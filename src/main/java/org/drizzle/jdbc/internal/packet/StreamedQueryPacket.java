package org.drizzle.jdbc.internal.packet;

import static org.drizzle.jdbc.internal.packet.buffer.WriteBuffer.intToByteArray;
import org.drizzle.jdbc.internal.packet.DrizzlePacket;
import org.drizzle.jdbc.internal.query.DrizzleParameterizedQuery;
import org.drizzle.jdbc.internal.query.Query;

import java.io.OutputStream;
import java.io.IOException;
import java.util.Arrays;


/**
 * User: marcuse
 * Date: Jan 19, 2009
 * Time: 10:14:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class StreamedQueryPacket implements DrizzlePacket {
    private final Query query;
    private byte [] byteHeader;
    private byte byteHeaderPointer=0;
    private boolean headerWritten=false;
    public StreamedQueryPacket(Query query) {
        this.query=query;
        byteHeader = Arrays.copyOf(intToByteArray(query.length()+1),5); //
        byteHeader[4]=(byte)0x03;
    }
    public byte [] toBytes(byte queryNumber) {
        return null;
    }
    public void sendQuery(OutputStream ostream) throws IOException {
        System.out.println("a");
        for(byte b:byteHeader)
            ostream.write(b);
        System.out.println("b");

        query.writeTo(ostream);
        System.out.println("c");

        ostream.flush();
        System.out.println("d");
        
    }
}
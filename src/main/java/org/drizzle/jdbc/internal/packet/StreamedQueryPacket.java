package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.WriteBuffer;
import static org.drizzle.jdbc.internal.packet.buffer.WriteBuffer.intToByteArray;
import org.drizzle.jdbc.internal.packet.DrizzlePacket;
import org.drizzle.jdbc.internal.query.DrizzleQuery;

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
    private final DrizzleQuery query;
    private byte [] byteHeader;
    private byte byteHeaderPointer=0;
    private boolean headerWritten=false;
    public StreamedQueryPacket(DrizzleQuery query) {
        this.query=query;
        byteHeader = Arrays.copyOf(intToByteArray(query.length()+1),5); //
        byteHeader[4]=(byte)0x03;
    }
    public byte [] toBytes(byte queryNumber) {
        return null;
    }
    public void sendQuery(OutputStream ostream) throws IOException {

        for(byte b:byteHeader)
            ostream.write(b);
        DrizzleQuery.QueryReader qr = query.getQueryReader();
        int b;
        while((b=qr.read()) != -1) {
            ostream.write(b);
        }
        ostream.flush();
    }
}
package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.Reader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:23:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class EOFPacket extends ResultPacket {
    private final byte packetSeq;

    public EOFPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        packetSeq = reader.getPacketSeq();
        byte packetType=reader.readByte();
        if(packetType!=(byte)0xfe)
            throw new IOException("Could not create EOF packet");
        reader.readShort();
        reader.readShort();
    }

    public ResultType getResultType() {
        return ResultType.EOF;
    }

    public byte getPacketSeq() {
        return packetSeq;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

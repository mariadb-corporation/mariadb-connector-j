package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.Reader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:20:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class ErrorPacket extends ResultPacket {
    private byte fieldCount;
    private short errorNumber;
    private byte sqlStateMarker;
    private byte[] sqlState;
    private String message;
    private byte packetSeq;

    public ErrorPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        this.fieldCount = reader.readByte();
        this.errorNumber = reader.readShort();
        this.sqlStateMarker = reader.readByte();
        this.sqlState = reader.readRawBytes(5);
        this.message= reader.readString("ASCII");
    }

    public String getMessage() {
        return message;
    }
    public ResultType getResultType() {
        return ResultType.ERROR;
    }

    public byte getPacketSeq() {
        return packetSeq;
    }
}

package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.Reader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:23:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class OKPacket extends ResultPacket {
    private final byte fieldCount;
    private final long affectedRows;
    private final long insertId;
    private final short serverStatus;
    private final short warnings;
    private final String message;
    private final byte packetSeqNum;

    public OKPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        packetSeqNum = reader.getPacketSeq();
        fieldCount = reader.readByte();
        affectedRows = reader.getLengthEncodedBinary();
        insertId = reader.getLengthEncodedBinary();
        serverStatus = reader.readShort();
        warnings = reader.readShort();
        message = reader.readString("ASCII");
    }
    public ResultType getResultType() {
        return ResultType.OK;
    }

    public byte getPacketSeq() {
        return packetSeqNum;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String toString() {
        return "affectedRows = "+affectedRows+"&insertId = "+insertId+"&serverStatus="+serverStatus+"&warnings="+warnings+"&message="+message;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public long getInsertId() {
        return insertId;
    }

    public short getServerStatus() {
        return serverStatus;
    }

    public short getWarnings() {
        return warnings;
    }

    public String getMessage() {
        return message;
    }
}

package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.Reader;
import org.drizzle.jdbc.internal.ServerStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 .
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:23:40 PM

 */
public class OKPacket extends ResultPacket {
    private final byte fieldCount;
    private final long affectedRows;
    private final long insertId;
    private final Set<ServerStatus> serverStatus;
    private final short warnings;
    private final String message;
    private final byte packetSeqNum;

    public OKPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        packetSeqNum = reader.getPacketSeq();
        fieldCount = reader.readByte();
        affectedRows = reader.getLengthEncodedBinary();
        insertId = reader.getLengthEncodedBinary();
        serverStatus = ServerStatus.getServerStatusSet(reader.readShort());
        warnings = reader.readShort();
        message = reader.readString("ASCII");
    }
    public ResultType getResultType() {
        return ResultType.OK;
    }

    public byte getPacketSeq() {
        return packetSeqNum;
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

    public Set<ServerStatus> getServerStatus() {
        return serverStatus;
    }

    public short getWarnings() {
        return warnings;
    }

    public String getMessage() {
        return message;
    }
}

package org.mariadb.jdbc.internal.common.packet;


import org.mariadb.jdbc.internal.common.packet.buffer.Reader;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class LocalInfilePacket extends ResultPacket {
    private String fileName;

    /**
     * Read Local Infile Packet result.
     * @param byteBuffer current packet's byteBuffer
     */
    public LocalInfilePacket(ByteBuffer byteBuffer) {
        super(byteBuffer);
        final Reader reader = new Reader(byteBuffer);
        long fieldCount = reader.getLengthEncodedBinary();
        if (fieldCount != -1) {
            throw new AssertionError("field count must be -1");
        }
        fileName = reader.readString(StandardCharsets.UTF_8);
    }

    public String getFileName() {
        return fileName;
    }

    public String toString() {
        return fileName;
    }

    public ResultType getResultType() {
        return ResultType.LOCALINFILE;
    }

    public byte getPacketSeq() {
        return 0;
    }
}

package org.mariadb.jdbc.internal.packet.result;


import org.mariadb.jdbc.internal.util.buffer.Buffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class LocalInfilePacket extends AbstractResultPacket {
    private String fileName;

    /**
     * Read Local Infile Packet result.
     * @param buffer current stream's rawBytes
     */
    public LocalInfilePacket(Buffer buffer) {
        long fieldCount = buffer.getLengthEncodedBinary();
        if (fieldCount != -1) {
            throw new AssertionError("field count must be -1");
        }
        fileName = buffer.readString(StandardCharsets.UTF_8);
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

package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadBuffer;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:23:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class EOFPacket extends ResultPacket {
    public EOFPacket(ReadBuffer readBuffer) throws IOException {

    }

    public ResultType getResultType() {
        return ResultType.EOF;
    }

    public byte getPacketSeq() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

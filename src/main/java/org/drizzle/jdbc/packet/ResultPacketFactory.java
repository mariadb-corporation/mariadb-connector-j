package org.drizzle.jdbc.packet;

import org.drizzle.jdbc.packet.buffer.ReadBuffer;

import java.io.InputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 1:12:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class ResultPacketFactory {


    private final static byte ERROR=(byte)0xff;
    private final static byte OK=(byte)0x00;
    private final static byte EOF = (byte)0xfe;


    public static ResultPacket createResultPacket(InputStream reader) throws IOException {
        ReadBuffer readBuffer = new ReadBuffer(reader);
        switch(readBuffer.getByteAt(0)) {
            case ERROR:
                return new ErrorPacket(readBuffer);
            case OK:
                return new OKPacket(readBuffer);
            case EOF:
                return new EOFPacket(readBuffer);
        }
        return null;
    }
}

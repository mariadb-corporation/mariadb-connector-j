package org.drizzle.jdbc.internal.packet.buffer;

import java.io.InputStream;
import java.io.IOException;
import java.io.BufferedInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 8:27:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReadUtil {
    private static int readLength(InputStream reader) throws IOException {
        byte [] lengthBuffer = new byte[3];
        //int readBytes = reader.read(lengthBuffer,0,3);
        for(int i =0 ;i<3;i++)
            lengthBuffer[i]= (byte) reader.read();
//        if(readBytes!=3) {
//            throw new IOException("Could not read packet");
//        }
        return  (lengthBuffer[0] & 0xff)+(lengthBuffer[1]<<8) + (lengthBuffer[2]<<16);
    }

    private static byte readPacketSeq(InputStream reader) throws IOException {
        return (byte)reader.read();
    }
    public static byte getByteAt(InputStream reader, int i) throws IOException {
        reader.mark(i+1);
        reader.skip(i-1);
        byte b = (byte) reader.read();
        reader.reset();
        return b;
    }

    public static boolean eofIsNext(BufferedInputStream reader) throws IOException {
        reader.mark(10);
        int length = readLength(reader);
        byte packetSeq = readPacketSeq(reader);
        byte packetType = (byte)reader.read();
        reader.reset();
        return (packetType == (byte)0xfe) && length<9; 
    }
}
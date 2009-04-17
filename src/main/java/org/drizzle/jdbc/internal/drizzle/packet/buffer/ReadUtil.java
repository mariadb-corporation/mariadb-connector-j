package org.drizzle.jdbc.internal.drizzle.packet.buffer;

import org.drizzle.jdbc.internal.drizzle.packet.RawPacket;

import java.io.InputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 8:27:38 PM

 */
public class ReadUtil {
    private static int readLength(InputStream reader) throws IOException {
        byte [] lengthBuffer = new byte[3];
        for(int i =0 ;i<3;i++)
            lengthBuffer[i]= (byte) reader.read();
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

    public static boolean eofIsNext(InputStream reader) throws IOException {
        reader.mark(10);
        int length = readLength(reader);
        byte packetSeq = readPacketSeq(reader);
        byte packetType = (byte)reader.read();
        reader.reset();
        return (packetType == (byte)0xfe) && length<9; 
    }
    public static boolean eofIsNext(RawPacket rawPacket) {
        byte [] rawBytes = rawPacket.getRawBytes();
        return (rawBytes[0] == (byte)0xfe && rawBytes.length<9);

    }

    public static short readShort(byte[] bytes, int start) {
        if(bytes.length-start >= 2)
            return (short) ((bytes[start]&0xff) + ((bytes[start+1]&0xff)<<8));
        return 0;
    }
    public static int read24bitword(byte[] bytes, int start) {
        return (bytes[start]&0xff) + ((bytes[start+1]&0xff)<<8) +((bytes[start+2]&0xff)<<16);
    }
    public static  long readLong(byte [] bytes, int start) {
        return
                (bytes[start]&0xff) +
               ((bytes[start+1]&0xff)<<8) +
               ((bytes[start+2]&0xff)<<16) +
               ((bytes[start+3]&0xff)<<24) +
               ((bytes[start+4]&0xff)<<32)+
               ((bytes[start+5]&0xff)<<40)+
               ((bytes[start+6]&0xff)<<48) +
               ((bytes[start+7]&0xff)<<56);
    }
 
    public static LengthEncodedBytes getLengthEncodedBytes(byte [] rawBytes, int start) {
        return new LengthEncodedBytes(rawBytes,start);
    }
    public static LengthEncodedBinary getLengthEncodedBinary(byte [] rawBytes, int start) {
        return new LengthEncodedBinary(rawBytes,start);
    }
}
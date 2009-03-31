package org.drizzle.jdbc.internal.packet.buffer;

import org.drizzle.jdbc.internal.packet.RawPacket;

import java.io.InputStream;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.util.Arrays;

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

    public static boolean eofIsNext(BufferedInputStream reader) throws IOException {
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
            return (short) (bytes[start] + (bytes[start+1]<<8));
        return 0;
    }
    public static int read24bitword(byte[] bytes, int start) {
        return bytes[start] + (bytes[start+1]<<8) +(bytes[start+2]<<16);
    }
    public static  long readLong(byte [] bytes, int start) {
        return
                bytes[start] +
               (bytes[start+1]<<8) +
               (bytes[start+2]<<16) +
               (bytes[start+3]<<24) +
               (bytes[start+4]<<32)+
               (bytes[start+5]<<40)+
               (bytes[start+6]<<48) +
               (bytes[start+7]<<56);
    }
    public static byte[] getLengthEncodedBytes(byte[] rawBytes, int start) {
        byte [] actualBytes;
        switch(getLengthEncodedByteLength(rawBytes,start)) {
            case 0:
                return Arrays.copyOfRange(rawBytes,start+1,rawBytes[start]);
            case -1:
                return null;
            case 2:
                return Arrays.copyOfRange(rawBytes,start+1,readShort(rawBytes,start+1));
            case 3:
                return Arrays.copyOfRange(rawBytes,start+1,read24bitword(rawBytes,start+1));
            case 8:
                return Arrays.copyOfRange(rawBytes,start+1,(int)readLong(rawBytes,start+1));
        }
        return null;
    }
    public static byte getLengthEncodedByteLength(byte[]rawBytes, int start) {
        System.out.println(rawBytes.length+":"+start);
        if(start<rawBytes.length) {
            byte type = rawBytes[start];
            if(type == (byte)251)
                return -1;
            if(type == (byte)252)
                return 2;
            if(type == (byte)253)
                return 3;
            if(type == (byte)254) {
                return 8;
            }
        }
        System.out.println("mpppppppp");
        return 0;
    }

    public static long getLengthEncodedBinary(byte[] rawBytes, int start) {
        
        return 0;
    }
}
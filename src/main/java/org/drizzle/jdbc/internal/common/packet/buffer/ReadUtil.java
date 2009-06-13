/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet.buffer;

import org.drizzle.jdbc.internal.common.packet.RawPacket;

import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.InputStream;
import java.io.IOException;

/**
 * .
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 8:27:38 PM
 */
public class ReadUtil {
    private static int readLength(InputStream reader) throws IOException {
        byte[] lengthBuffer = new byte[3];
        if (safeRead(reader, lengthBuffer) != 3) {
            throw new IOException("Incomplete read!");
        }
        /* The & 0xff is to correctly convert unsigned byte to int ;-) */
        return (lengthBuffer[0] & 0xff) + ((lengthBuffer[1] & 0xff) << 8) + ((lengthBuffer[2] & 0xff) << 16);
    }

     /**
     * Read a number of bytes from the stream and store it in the buffer, and
     * fix the problem with "incomplete" reads by doing another read if we
     * don't have all of the data yet.
     *
     * @param is     the input stream to read from
     * @param buffer where to store the data
     * @return the number of bytes read (should be == length if we didn't hit EOF)
     * @throws java.io.IOException if an error occurs while reading the stream
     */
    public static int safeRead(InputStream is, byte[] buffer) throws IOException {
        int offset = 0;
        int left = buffer.length;
        do {
            try {
                int nr = is.read(buffer, offset, left);
                if (nr == -1) {
                    return nr;
                }
                offset += nr;
                left -= nr;
            } catch (InterruptedIOException exp) {
               /* Ignore, just retry */
            }
        } while (left > 0);

        return buffer.length;
    }

    private static byte readPacketSeq(InputStream reader) throws IOException {
        return (byte) reader.read();
    }

    public static byte getByteAt(InputStream reader, int i) throws IOException {
        reader.mark(i + 1);
        long skipped = reader.skip(i - 1);
        if(skipped != i-1) {
            throw new IOException("Could not skip the requested number of bytes.");
        }

        byte b = (byte) reader.read();
        reader.reset();
        return b;
    }

    public static boolean eofIsNext(InputStream reader) throws IOException {
        reader.mark(10);
        int length = readLength(reader);
        byte packetType = (byte) reader.read();
        reader.reset();
        return (packetType == (byte) 0xfe) && length < 9;
    }

    public static boolean eofIsNext(RawPacket rawPacket) {
        byte[] rawBytes = rawPacket.getRawBytes();
        return (rawBytes[0] == (byte) 0xfe && rawBytes.length < 9);

    }

    public static short readShort(byte[] bytes, int start) {
        short length = 0;

        if(bytes.length-start >= 2) {
            length = (short)((bytes[start] & (short)0xff) + (short)((bytes[start + 1] & (short)0xff) << 8));
        }
        
        return length;
    }

    public static int read16bitword(byte[] bytes, int start) {
        int length = 0;

        if(bytes.length-start >= 2) {
            length = ((bytes[start] & 0xff) + ((bytes[start + 1] & 0xff) << 8));
        }

        return length;
    }

    public static int read24bitword(byte[] bytes, int start) {
        return (bytes[start] & 0xff) + ((bytes[start + 1] & 0xff) << 8) + ((bytes[start + 2] & 0xff) << 16);
    }

    public static  long readLong(byte [] bytes, int start) {
        long length = 
                (bytes[start]& (long)0xff) +
               ((bytes[start+1]&(long)0xff)<<8) +
               ((bytes[start+2]&(long)0xff)<<16) +
               ((bytes[start+3]&(long)0xff)<<24) +
               ((bytes[start+4]&(long)0xff)<<32)+
               ((bytes[start+5]&(long)0xff)<<40)+
               ((bytes[start+6]&(long)0xff)<<48) +
               ((bytes[start+7]&(long)0xff)<<56);
        return length;
    }
 
    public static LengthEncodedBytes getLengthEncodedBytes(byte[] rawBytes, int start) {
        return new LengthEncodedBytes(rawBytes, start);
    }

    public static LengthEncodedBinary getLengthEncodedBinary(byte[] rawBytes, int start) {
        return new LengthEncodedBinary(rawBytes, start);
    }
}
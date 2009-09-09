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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 8:27:38 PM
 */
public class Reader {
    private final InputStream inputReader;
    private final int length;
    private int readBytes = 0;
    private final byte packetSeq;

    public Reader(final InputStream inputReader) throws IOException {
        this.inputReader = inputReader;
        this.length = readLength();
        this.packetSeq = readPacketSeq();
    }

    public Reader(final RawPacket rawPacket) {
        this.inputReader = new ByteArrayInputStream(rawPacket.getRawBytes());
        this.length = rawPacket.getRawBytes().length;
        this.packetSeq = 0;
    }

    private int readLength() throws IOException {
        final byte[] lengthBuffer = new byte[3];
        for (int i = 0; i < 3; i++) {
            lengthBuffer[i] = (byte) inputReader.read();
        }
        return (lengthBuffer[0] & 0xff) + (lengthBuffer[1] << 8) + (lengthBuffer[2] << 16);
    }

    private byte readPacketSeq() throws IOException {
        return (byte) inputReader.read();
    }


    /**
     * Reads a string from the buffer, looks for a 0 to end the string
     *
     * @param charset the charset to use, for example ASCII
     * @return the read string
     * @throws java.io.IOException if it is not possible to create the string from the buffer
     */
    public String readString(final String charset) throws IOException {
        int ch;
        final byte[] tempArr = new byte[length - readBytes]; //todo: fix!
        int i = 0;
        while (readBytes < length && ((ch = inputReader.read()) != 0)) {
            readBytes++;
            tempArr[i++] = (byte) ch;
        }
        return new String(tempArr, charset);
    }

    /**
     * read a short (2 bytes) from the buffer;
     *
     * @return an short
     * @throws java.io.IOException if there are not 2 bytes left in the buffer
     */
    public short readShort() throws IOException {
        if (readBytes + 2 > length) {
            throw new IOException("Could not read short");
        }
        final byte[] tempBuf = new byte[2];
        tempBuf[0] = (byte) inputReader.read();
        tempBuf[1] = (byte) inputReader.read();
        readBytes += 2;
        return (short) ((tempBuf[0] & 0xff) + ((tempBuf[1] & 0xff) << 8));
    }

    /**
     * read a int (4 bytes) from the buffer;
     *
     * @return a int
     * @throws java.io.IOException if there are not 4 bytes left in the buffer
     */
    public int readInt() throws IOException {
        if (readBytes + 4 > length) {
            throw new IOException("Could not read int");
        }

        final byte[] tempBuf = new byte[4];
        for (int i = 0; i < 4; i++) {
            tempBuf[i] = (byte) inputReader.read();
        }
        readBytes += 4;
        return (tempBuf[0] & 0xff) +
                ((tempBuf[1] & 0xff) << 8) +
                ((tempBuf[2] & 0xff) << 16) +
                ((tempBuf[3] & 0xff) << 24);
    }

    /**
     * read a long (8 bytes) from the buffer;
     *
     * @return a long
     * @throws java.io.IOException if there are not 8 bytes left in the buffer
     */
    public long readLong() throws IOException {
        if (readBytes + 8 > length) {
            throw new IOException("Could not read short");
        }

        final byte[] tempBuf = new byte[8];
        for (int i = 0; i < 8; i++) {
            tempBuf[i] = (byte) inputReader.read();
        }
        readBytes += 8;
        return
                ((long) (tempBuf[0] & 0xff)) +
                        (((long) (tempBuf[1] & 0xff)) << 8) +
                        (((long) (tempBuf[2] & 0xff)) << 16) +
                        (((long) (tempBuf[3] & 0xff)) << 24) +
                        (((long) (tempBuf[4] & 0xff)) << 32) +
                        (((long) (tempBuf[5] & 0xff)) << 40) +
                        (((long) (tempBuf[6] & 0xff)) << 48) +
                        (((long) (tempBuf[7] & 0xff)) << 56);
    }


    /**
     * reads a byte from the buffer
     *
     * @return the byte
     * @throws java.io.IOException if bufferPointer exceeds the length of the buffer
     */
    public byte readByte() throws IOException {
        if (readBytes >= length) {
            throw new IOException("Could not read byte");
        }
        readBytes++;
        return (byte) inputReader.read();
    }

    public byte[] readRawBytes(final int numberOfBytes) throws IOException {
        if (readBytes + numberOfBytes >= length) {
            throw new IOException("Could not read bytes");
        }
        final byte[] tmpArr = new byte[numberOfBytes];
        for (int i = 0; i < numberOfBytes; i++) {
            tmpArr[i] = (byte) inputReader.read();
        }
        readBytes += numberOfBytes;
        return tmpArr;
    }

    public void skipByte() throws IOException {
        skipBytes(1);
    }

    public long skipBytes(final int bytesToSkip) throws IOException {
        if (readBytes + bytesToSkip > length) {
            throw new IOException("Could not skip bytes");
        }
        readBytes += bytesToSkip;
        return inputReader.skip(bytesToSkip);

    }

    public int read24bitword() throws IOException {
        if (readBytes + 3 >= length) {
            throw new IOException("Could not read 3 bytes");
        }
        final byte[] tmpArr = new byte[3];
        for (int i = 0; i < 3; i++) {
            tmpArr[i] = (byte) inputReader.read();
        }
        readBytes += 3;
        return (tmpArr[0] & 0xff) + ((tmpArr[1] & 0xff) << 8) + ((tmpArr[2] & 0xff) << 16);
    }

    public long getLengthEncodedBinary() throws IOException {
        if (readBytes >= length) {
            throw new IOException("Could not read length encoded binary (" + readBytes + ")(" + length + ")");
        }
        final byte type = (byte) inputReader.read();
        readBytes += 1;

        if ((type & 0xff) == 251) {
            return -1;
        }
        if ((type & 0xff) == 252) {
            return (long) readShort();
        }
        if ((type & 0xff) == 253) {
            return read24bitword();
        }
        if ((type & 0xff) == 254) {
            return readLong();
        }
        if ((type & 0xff) <= 250) {
            return (long) type;
        }

        return 0;
    }

    public byte peek() throws IOException {
        inputReader.mark(2);
        final byte b = (byte) inputReader.read();
        inputReader.reset();
        return b;
    }

    public String getLengthEncodedString() throws IOException {
        final long encLength = getLengthEncodedBinary();
        if (encLength == -1) {
            return null;
        }
        if (readBytes + encLength > length) {
            throw new IOException("Could not read length encoded binary (" +
                    readBytes +
                    ")(" +
                    encLength +
                    ")(" +
                    length +
                    ")");
        }
        final byte[] tmpBuf = new byte[(int) encLength];
        for (int i = 0; i < encLength; i++) {
            tmpBuf[i] = (byte) inputReader.read();
        }
        readBytes += encLength;
        return new String(tmpBuf, "ASCII");
    }

    public byte[] getLengthEncodedBytes() throws IOException {
        final long encLength = getLengthEncodedBinary();
        if (encLength == -1) {
            return null;
        }
        if (readBytes + encLength > length) {
            throw new IOException("Could not read length encoded binary (" +
                    readBytes +
                    ")(" +
                    encLength +
                    ")(" +
                    length +
                    ")");
        }
        final byte[] tmpBuf = new byte[(int) encLength];
        for (int i = 0; i < encLength; i++) {
            tmpBuf[i] = (byte) inputReader.read();
        }
        readBytes += encLength;
        return tmpBuf;
    }

    public byte getByteAt(final int i) throws IOException {
        inputReader.mark(i + 1);
        final long skipped = inputReader.skip(i - 1);
        if (skipped != i - 1) {
            throw new IOException("Could not skip the requested amount of bytes");
        }
        final byte b = (byte) inputReader.read();
        inputReader.reset();
        return b;
    }

    public byte getPacketSeq() {
        return packetSeq;
    }
}
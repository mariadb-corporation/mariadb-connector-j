/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015-2016 MariaDB Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.stream;

import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.packet.Packet;
import org.mariadb.jdbc.internal.util.Utils;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.io.*;
import java.nio.*;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.zip.DeflaterOutputStream;

import static org.mariadb.jdbc.internal.util.SqlStates.INTERRUPTED_EXCEPTION;

public class PacketOutputStream extends OutputStream {

    private static Logger logger = LoggerFactory.getLogger(PacketOutputStream.class);

    private static final int MIN_COMPRESSION_SIZE = 16 * 1024;
    private static final float MIN_COMPRESSION_RATIO = 0.9f;
    private static final int MAX_PACKET_LENGTH = 0x00ffffff;
    private static final int HEADER_LENGTH = 4;
    private static final int BUFFER_DEFAULT_SIZE = 4096;
    private static final float NORMAL_INCREASE = 4f;
    private static final float BIG_SIZE_INCREASE = 1.5f;

    public ByteBuffer buffer;
    public ByteBuffer firstBuffer;

    int seqNo;
    int compressSeqNo;
    int lastSeq;
    int maxAllowedPacket = MAX_PACKET_LENGTH;
    int maxPacketSize = MAX_PACKET_LENGTH;
    boolean checkPacketLength;
    boolean useCompression;
    boolean logQuery;
    int maxQuerySizeToLog;
    public OutputStream outputStream;
    private volatile boolean closed = false;

    /**
     * Initialization with server outputStream.
     * @param outputStream server outputStream
     * @param logQuery is query logging enable ?
     * @param maxQuerySizeToLog max query size to log
     */
    public PacketOutputStream(OutputStream outputStream, boolean logQuery, int maxQuerySizeToLog) {
        this.outputStream = outputStream;
        buffer = firstBuffer = ByteBuffer.allocate(BUFFER_DEFAULT_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        useCompression = false;
        buffer.position(4);
        this.logQuery = logQuery;
        this.maxQuerySizeToLog = maxQuerySizeToLog;
    }

    protected void increase(int newCapacity) {
        ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity).order(ByteOrder.LITTLE_ENDIAN);
        System.arraycopy(buffer.array(), 0, newBuffer.array(), 0, buffer.position());
        newBuffer.position(buffer.position());
        buffer = newBuffer;
    }

    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    /**
     * Initialize stream sequence. Max stream allowed size will be checked.
     * @param seqNo stream sequence number
     * @param checkPacketLength indication that max stream allowed size will be checked.
     * @throws IOException if any error occur during data send to server
     */
    public void startPacket(int seqNo, boolean checkPacketLength) throws IOException {
        if (closed) {
            throw new IOException("Stream has already closed");
        }
        this.seqNo = seqNo;
        this.compressSeqNo = seqNo;
        this.checkPacketLength = checkPacketLength;
        buffer.clear();
        buffer.position(4);
    }

    /**
     * Initialize stream sequence.
     * @param seqNo stream sequence number
     * @throws IOException if any error occur during data send to server
     */
    public void startPacket(int seqNo) throws IOException {
        startPacket(seqNo, true);
    }

    /**
     * Send an empty stream to server.
     * @param seqNo stream sequence number
     * @throws IOException if any error occur during data send to server
     */
    public void writeEmptyPacket(int seqNo) throws IOException {
        byte[] header;
        logger.trace("send empty packet");
        if (!useCompression) {
            header = new byte[4];
            header[0] = ((byte) 0);
            header[1] = ((byte) 0);
            header[2] = ((byte) 0);
            header[3] = ((byte) seqNo);
            outputStream.write(header, 0, 4);
        } else {
            header = new byte[7];
            header[0] = (byte) 4;
            header[1] = (byte) 0;
            header[2] = (byte) 0;
            header[3] = (byte) compressSeqNo;
            header[4] = (byte) 0;
            header[5] = (byte) 0;
            header[6] = (byte) 0;
            outputStream.write(header, 0, 7);
            header = new byte[4];
            header[0] = ((byte) 0);
            header[1] = ((byte) 0);
            header[2] = ((byte) 0);
            header[3] = ((byte) seqNo);
            outputStream.write(header, 0, 4);
        }
    }

    public void setCompressSeqNo(int compressSeqNo) {
        this.compressSeqNo = compressSeqNo;
    }

    /**
     * Used to send LOAD DATA INFILE. End of data is indicated by stream of length 0.
     * @param is inputStream to send
     * @param seq stream sequence number
     * @throws IOException if any error occur during data send to server
     */
    public void sendFile(InputStream is, int seq) throws IOException {
        this.seqNo = seq;

        if (!useCompression) {
            //No compression
            //According to protocol, buffer can be up to max_allowed_packet, but if max_allowed_packet size > a packet :
            // - it may take a lot of memory client side
            // - it will be faster to send packet directly
            //so, reserve the 4th first bytes for packet header to permit writing buffer immediately buffer to socket

            int bufLength =  Math.min(maxAllowedPacket, MAX_PACKET_LENGTH) - 4;
            byte[] buf = new byte[bufLength + 4];

            int len;
            while ((len = is.read(buf, 4, bufLength)) > 0) {
                buf[0] = (byte) ((len) & 0xff);
                buf[1] = (byte) ((len) >>> 8);
                buf[2] = (byte) ((len) >>> 16);
                buf[3] = (byte) seqNo++;
                outputStream.write(buf, 0, len + 4);

                if (logger.isTraceEnabled() && logQuery) {
                    logger.trace("send packet local file packet seq:" + (seqNo - 1) + " length:" + (len));
                }
            }

            //send empty packet when finish
            buf[0] = ((byte) 0);
            buf[1] = ((byte) 0);
            buf[2] = ((byte) 0);
            buf[3] = ((byte) seqNo);
            outputStream.write(buf, 0, 4);

        } else {
            //compression
            //reserve 11 byte for header (7 bytes for compression header + 4 byte packet header)
            int bufLength =  Math.min(maxAllowedPacket - 11, MAX_PACKET_LENGTH - 11);
            byte[] buf = new byte[bufLength + 11];
            int len;

            while ((len = is.read(buf, 11, bufLength)) > 0) {
                boolean compressedPacketSend = false;

                if (len > MIN_COMPRESSION_SIZE) {
                    buf[7] = (byte) ((len) & 0xff);
                    buf[8] = (byte) (((len) >> 8) & 0xff);
                    buf[9] = (byte) (((len) >> 16) & 0xff);
                    buf[10] = (byte) this.seqNo++;
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DeflaterOutputStream deflater = new DeflaterOutputStream(baos);
                    deflater.write(buf, 7, len + 4);
                    deflater.finish();
                    deflater.close();

                    byte[] compressedBytes = baos.toByteArray();
                    baos.close();

                    if (compressedBytes.length < (int) (MIN_COMPRESSION_RATIO * len)) {

                        int compressedLength = compressedBytes.length;

                        buf[0] = (byte) ((compressedLength) & 0xff);
                        buf[1] = (byte) (((compressedLength) >> 8) & 0xff);
                        buf[2] = (byte) (((compressedLength) >> 16) & 0xff);
                        buf[3] = (byte) this.compressSeqNo++;
                        buf[4] = (byte) ((len + 4) & 0xff);
                        buf[5] = (byte) (((len + 4) >> 8) & 0xff);
                        buf[6] = (byte) (((len + 4) >> 16) & 0xff);
                        outputStream.write(buf, 0, 7);
                        outputStream.write(compressedBytes, 0, compressedLength);
                        compressedPacketSend = true;
                        if (logger.isTraceEnabled()) {
                            logger.trace("send compress packet seq:" + compressSeqNo + " length:" + compressedLength
                                    + " gzip data");
                        }
                    }
                }

                if (!compressedPacketSend) {
                    //uncompress packet : 7 bytes compress packet header + standard packet header

                    buf[0] = (byte) ((len + 4) & 0xff);
                    buf[1] = (byte) (((len + 4) >> 8) & 0xff);
                    buf[2] = (byte) (((len + 4) >> 16) & 0xff);
                    buf[3] = (byte) this.compressSeqNo++;
                    buf[4] = 0;
                    buf[5] = 0;
                    buf[6] = 0;
                    buf[7] = (byte) ((len) & 0xff);
                    buf[8] = (byte) (((len) >> 8) & 0xff);;
                    buf[9] = (byte) (((len) >> 16) & 0xff);
                    buf[10] = (byte) this.seqNo++;

                    outputStream.write(buf, 0, len + 11);

                    if (logger.isTraceEnabled()) {
                        logger.trace("send compress packet seq:" + compressSeqNo + " length:" + len
                                + " data:" + Utils.hexdump(buf, maxQuerySizeToLog, 7, len));
                    }
                }

            }

            //write empty packet
            buf[0] = (byte) 4;
            buf[1] = (byte) 0;
            buf[2] = (byte) 0;
            buf[3] = (byte) compressSeqNo++;
            buf[4] = (byte) 0;
            buf[5] = (byte) 0;
            buf[6] = (byte) 0;
            buf[7] = (byte) 0;
            buf[8] = (byte) 0;
            buf[9] = (byte) 0;
            buf[10] = (byte) seqNo++;
            outputStream.write(buf, 0, 11);

            if (logger.isTraceEnabled()) {
                logger.trace("send compress empty packet seq:" + compressSeqNo);
            }

        }
    }

    /**
     * Send stream to server.
     * @param is inputStream to send
     * @throws IOException if any error occur during data send to server
     */
    public void sendStream(InputStream is) throws IOException {
        byte[] buffer = new byte[BUFFER_DEFAULT_SIZE];
        int len;
        while ((len = is.read(buffer)) > 0) {
            write(buffer, 0, len);
        }
    }

    /**
     * Send stream to server.
     * @param is inputStream to send
     * @param readLength max size to send
     * @throws IOException if any error occur during data send to server
     */
    public void sendStream(InputStream is, long readLength) throws IOException {
        byte[] buffer = new byte[BUFFER_DEFAULT_SIZE];
        long remainingReadLength = readLength;
        int read;
        while (remainingReadLength > 0) {
            read = is.read(buffer, 0, Math.min((int)remainingReadLength, BUFFER_DEFAULT_SIZE));
            if (read == -1) {
                return;
            }
            write(buffer, 0, read);
            remainingReadLength -= read;
        }
    }

    /**
     * Send reader stream to server.
     * @param reader reader to send
     * @throws IOException if any error occur during data send to server
     */
    public void sendStream(Reader reader) throws IOException {
        char[] buffer = new char[BUFFER_DEFAULT_SIZE];
        int len;
        while ((len = reader.read(buffer)) > 0) {
            byte[] bytes = new String(buffer, 0, len).getBytes("UTF-8");
            write(bytes, 0, bytes.length);
        }
    }

    /**
     * Send reader stream to server.
     * @param reader reader to send
     * @param readLength max size to send
     * @throws IOException if any error occur during data send to server
     */
    public void sendStream(Reader reader, long readLength) throws IOException {
        char[] buffer = new char[BUFFER_DEFAULT_SIZE];
        long remainingReadLength = readLength;
        int read;
        while (remainingReadLength > 0) {
            read = reader.read(buffer, 0, Math.min((int)remainingReadLength, BUFFER_DEFAULT_SIZE));
            if (read == -1) {
                return;
            }
            byte[] bytes = new String(buffer, 0, read).getBytes("UTF-8");
            write(bytes, 0, bytes.length);
            remainingReadLength -= read;
        }

    }

    /**
     * Reinitialized buffer to smaller size if needed to avoid memory consumption.
     */
    public void releaseBuffer() {
        //save big buffer next query to avoid new allocation if next query size is similar
        if ((buffer.capacity() > 4194304 && buffer.limit() * BIG_SIZE_INCREASE < buffer.capacity())
                || (buffer.capacity() <= 4194304 && buffer.limit() * NORMAL_INCREASE < buffer.capacity())) {
            buffer = firstBuffer;
        }
    }

    /**
     * If logging is not active, release buffer.
     * (if logging is active, it will use the buffer to know send query and release buffer after a while)
     */
    public void releaseBufferIfNotLogging() {
        //save big buffer next query to avoid new allocation if next query size is similar
        if (!logQuery && buffer != null && ((buffer.capacity() > 4194304 && buffer.limit() * BIG_SIZE_INCREASE < buffer.capacity())
                || (buffer.capacity() <= 4194304 && buffer.limit() * NORMAL_INCREASE < buffer.capacity()))) {
            buffer = firstBuffer;
        }
    }

    /**
     * Ending command that tell to send buffer to server.
     * @param logQuery log query (password mustn't be logged)
     * @throws IOException if any connection error occur
     */
    public void finishPacketWithoutRelease(boolean logQuery) throws IOException {
        if (buffer.position() > 4) {
            checkPacketMaxSize(buffer.position() - 4);

            if (useCompression) {
                generatePacketWithCompression(logQuery);
            } else {
                generatePacket(logQuery);
            }
        }
        this.lastSeq =  (useCompression) ? this.compressSeqNo : this.seqNo;
    }

    /**
     * Force buffer cleanup.
     */
    public void forceCleanupBuffer() {
        Arrays.fill(buffer.array(), (byte) 0x00);
    }

    @Override
    public void write(byte[] bytes) {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(int byteInt) {
        assureBufferCapacity(1);
        buffer.put((byte) byteInt);
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        assureBufferCapacity(len);
        buffer.put(bytes, off, len);
    }

    /**
     * Write byte value directly to buffer.
     * (buffer size must have been checked beforeheand !)
     *
     * @param value byte value
     */
    public void writeUnsafe(int value) {
        buffer.put((byte) value);
    }

    public void writeUnsafe(byte[] bytes, int off, int len) {
        buffer.put(bytes, off, len);
    }

    public void writeUnsafe(byte[] bytes) {
        writeUnsafe(bytes, 0, bytes.length);
    }

    /**
     * Check that current buffer + length will not be superior to max_allowed_packet + header size.
     * That permit to separate rewritable queries to be separate in multiple stream.
     * @param length additionnal length
     * @return true if with this additional length stream can be send in the same stream
     */
    public boolean checkRewritableLength(int length) {
        return !(checkPacketLength
                && ((!useCompression && buffer.position() + length >= maxAllowedPacket)
                || (useCompression && buffer.position() + length + 4 >= maxAllowedPacket)));
    }

    private void checkPacketMaxSize(int limit) throws MaxAllowedPacketException {
        if (checkPacketLength
                && maxAllowedPacket > 0
                && ((!useCompression && limit >= maxAllowedPacket) || (useCompression && limit + 4 >= maxAllowedPacket))) {
            this.seqNo = -1;
            throw new MaxAllowedPacketException("stream size " + (limit + (useCompression ? 4 : 0))
                    + " is >= to max_allowed_packet (" + maxAllowedPacket + ")", this.seqNo != 1);
        }
    }

    private void generatePacket(boolean logQuery) throws IOException {
        buffer.flip();
        // the 4th first byte are reserved for first header.
        int dataLength = buffer.remaining() - 4;

        if (dataLength < maxPacketSize) {
            //if only one packet, put array to socket
            buffer.put((byte) (dataLength & 0xff))
                    .put((byte) (dataLength >>> 8))
                    .put((byte) (dataLength >>> 16))
                    .put((byte) seqNo++);
            if (logger.isTraceEnabled() && logQuery) {
                logger.trace("send packet seq:" + (seqNo - 1) + " length:" + dataLength
                        + " data:" + Utils.hexdump(buffer.array(), maxQuerySizeToLog, 4, dataLength));
            }
            outputStream.write(buffer.array(), 0, buffer.limit());
        } else {

            //multiple packet. Send first one
            buffer.put((byte) (maxPacketSize & 0xff))
                    .put((byte) (maxPacketSize >>> 8))
                    .put((byte) (maxPacketSize >>> 16))
                    .put((byte) seqNo++);
            if (logger.isTraceEnabled() && logQuery) {
                logger.trace("send packet seq:" + (seqNo - 1) + " length:" + maxPacketSize
                        + " data:" + Utils.hexdump(buffer.array(), maxQuerySizeToLog, 4, maxPacketSize));
            }
            outputStream.write(buffer.array(), 0, maxPacketSize + 4);
            buffer.position(maxPacketSize + 4);

            while (buffer.remaining() > 0 ) {
                int length = buffer.remaining();
                buffer.position(buffer.position() - 4);

                if (length > maxPacketSize) {
                    buffer.put((byte) (maxPacketSize & 0xff))
                            .put((byte) (maxPacketSize >>> 8))
                            .put((byte) (maxPacketSize >>> 16))
                            .put((byte) seqNo++);
                    if (logger.isTraceEnabled() && logQuery) {
                        logger.trace("send packet seq:" + (seqNo - 1) + " length:" + maxPacketSize
                                + " data:" + Utils.hexdump(buffer.array(), maxQuerySizeToLog, buffer.position(), maxPacketSize));
                    }
                    outputStream.write(buffer.array(), buffer.position() - 4, maxPacketSize + 4);
                    buffer.position(buffer.position() + maxPacketSize);
                } else {
                    buffer.put((byte) (length & 0xff))
                            .put((byte) (length >>> 8))
                            .put((byte) (length >>> 16))
                            .put((byte) seqNo++);
                    if (logger.isTraceEnabled() && logQuery) {
                        logger.trace("send packet seq:" + (seqNo - 1) + " length:" + maxPacketSize
                                + " data:" + Utils.hexdump(buffer.array(), maxQuerySizeToLog, buffer.position(), length));
                    }
                    outputStream.write(buffer.array(), buffer.position() - 4, length + 4);
                    break;
                }
            }
        }
    }


    private void generatePacketWithCompression(boolean logQuery) throws IOException {
        buffer.flip();
        int limit = buffer.limit();
        buffer.position(4);
        int position = 0;
        int expectedPacketSize = limit - 4 + HEADER_LENGTH * ((limit / maxPacketSize) + 1);
        byte[] bufferBytes = new byte[expectedPacketSize];

        while (position < expectedPacketSize) {
            int length = buffer.remaining();
            if (length > maxPacketSize) {
                length = maxPacketSize;
            }
            bufferBytes[position++] = (byte) (length & 0xff);
            bufferBytes[position++] = (byte) (length >>> 8);
            bufferBytes[position++] = (byte) (length >>> 16);
            bufferBytes[position++] = (byte) seqNo++;

            if (length > 0) {
                buffer.get(bufferBytes, position, length);
                position += length;
            }
        }
        //now bufferBytes in filled with uncompressed data
        compressedAndSend(position, bufferBytes, logQuery);
    }

    /**
     * Compress datas and send them to database.
     * @param notCompressPosition notCompressPosition
     * @param bufferBytes not compressed data buffer
     * @param logQuery log query
     * @throws IOException if any compression or connection error occur
     */
    public void compressedAndSend(int notCompressPosition, byte[] bufferBytes, boolean logQuery) throws IOException {
        int position = 0;
        int packetLength;

        while (position - notCompressPosition < 0) {
            packetLength = Math.min(notCompressPosition - position, maxPacketSize);
            boolean compressedPacketSend = false;

            if (packetLength > MIN_COMPRESSION_SIZE) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DeflaterOutputStream deflater = new DeflaterOutputStream(baos);

                deflater.write(bufferBytes, position, packetLength);
                deflater.finish();
                deflater.close();

                byte[] compressedBytes = baos.toByteArray();
                baos.close();

                if (compressedBytes.length < (int) (MIN_COMPRESSION_RATIO * packetLength)) {

                    int compressedLength = compressedBytes.length;
                    writeCompressedHeader(compressedLength, packetLength);
                    if (logger.isTraceEnabled() && logQuery) {
                        logger.trace("send packet seq:" + compressSeqNo + " length:" + packetLength
                                + " data:" + Utils.hexdump(compressedBytes, maxQuerySizeToLog));
                    }
                    outputStream.write(compressedBytes, 0, compressedLength);
                    compressedPacketSend = true;
                }
            }

            if (!compressedPacketSend) {
                writeCompressedHeader(packetLength, 0);
                if (logger.isTraceEnabled() && logQuery) {
                    logger.trace("send packet seq:" + compressSeqNo + " length:" + packetLength
                            + " data:" + Utils.hexdump(bufferBytes, maxQuerySizeToLog, position, packetLength));
                }
                outputStream.write(bufferBytes, position, packetLength);
            }

            position += packetLength;
        }
    }

    private void writeCompressedHeader(int packetLength, int initialLength) throws IOException {
        byte[] header = new byte[7];
        header[0] = (byte) (packetLength & 0xff);
        header[1] = (byte) ((packetLength >> 8) & 0xff);
        header[2] = (byte) ((packetLength >> 16) & 0xff);
        header[3] = (byte) this.compressSeqNo++;
        header[4] = (byte) (initialLength & 0xff);
        header[5] = (byte) ((initialLength >> 8) & 0xff);
        header[6] = (byte) ((initialLength >> 16) & 0xff);
        outputStream.write(header);
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
        buffer = null;
        firstBuffer = null;
        closed = true;
    }

    /**
     * Initialize maximal send size (can be send in multiple stream).
     * @param maxAllowedPacket value of server maxAllowedPacket
     */
    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
        if (maxAllowedPacket > 0) {
            maxPacketSize = Math.min(maxAllowedPacket, MAX_PACKET_LENGTH);
        } else {
            maxPacketSize = MAX_PACKET_LENGTH;
        }
    }

    /**
     * Ensure that the buffer remaining size permit to write a data with a size len.
     * @param len size of the data
     */
    public void assureBufferCapacity(final int len) {
        while (len > buffer.remaining()) {
            int newCapacity = Math.max(
                    (int)(len + buffer.position() * BIG_SIZE_INCREASE),
                    (int) ((buffer.capacity() > 4194304) ? buffer.capacity() * BIG_SIZE_INCREASE : buffer.capacity() * NORMAL_INCREASE));
            increase(newCapacity);
        }
    }

    /**
     * Write a byte data to buffer.
     * @param theByte byte to write
     * @return this
     */
    public PacketOutputStream writeByte(final byte theByte) {
        assureBufferCapacity(1);
        buffer.put(theByte);
        return this;
    }

    /**
     * Write count time the byte value.
     * @param theByte byte to write to buffer
     * @param count number of time the value will be put to buffer
     * @return this
     */
    public PacketOutputStream writeBytes(final byte theByte, final int count) {
        for (int i = 0; i < count; i++) {
            this.writeByte(theByte);
        }
        return this;
    }


    /**
     * Write byte array to buffer.
     * @param bytes  byte array
     * @return this.
     */
    public PacketOutputStream writeByteArray(final byte[] bytes) {
        assureBufferCapacity(bytes.length);
        buffer.put(bytes);
        return this;
    }

    /**
     * Write byte array data to binary data.
     * @param bytes  byte array to encode
     * @return this.
     */
    public PacketOutputStream writeByteArrayLength(final byte[] bytes) {
        assureBufferCapacity(bytes.length + 9);
        writeFieldLength(bytes.length);
        buffer.put(bytes);
        return this;
    }

    /**
     * Write string data in binary format.
     * @param str string value to encode
     * @return this.
     */
    public PacketOutputStream writeString(final String str) {
        final byte[] strBytes;
        try {
            strBytes = str.getBytes("UTF-8");
            return writeByteArray(strBytes);
        } catch (UnsupportedEncodingException u) {
            return this;
        }
    }

    /**
     * Write short data in binary format.
     * @param theShort short data to encode
     * @return this
     */
    public PacketOutputStream writeShort(final short theShort) {
        assureBufferCapacity(2);
        buffer.putShort(theShort);
        return this;
    }

    /**
     * Write int data in binary format.
     * @param theInt int data
     * @return this.
     */
    public PacketOutputStream writeInt(final int theInt) {
        assureBufferCapacity(4);
        buffer.putInt(theInt);
        return this;
    }

    /**
     * Write long data in binary format.
     * @param theLong long data
     * @return this
     */
    public PacketOutputStream writeLong(final long theLong) {
        assureBufferCapacity(8);
        buffer.putLong(theLong);
        return this;
    }

    /**
     * Write field length to encode in binary format.
     * @param length data length to encode
     * @return this.
     */
    public PacketOutputStream writeFieldLength(long length) {
        if (length < 251) {
            buffer.put((byte) length);
        } else if (length < 65536) {
            assureBufferCapacity(3);
            buffer.put((byte) 0xfc);
            buffer.putShort((short) length);
        } else if (length < 16777216) {
            assureBufferCapacity(4);
            buffer.put((byte) 0xfd);
            buffer.put((byte) (length & 0xff));
            buffer.put((byte) (length >>> 8));
            buffer.put((byte) (length >>> 16));
        } else {
            assureBufferCapacity(9);
            buffer.put((byte) 0xfe);
            buffer.putLong(length);
        }
        return this;
    }

    /**
     * Write string in binary format.
     * @param str string to encode
     * @return this.
     */
    public PacketOutputStream writeStringLength(final String str) {
        try {
            final byte[] strBytes = str.getBytes("UTF-8");
            assureBufferCapacity(strBytes.length + 9);
            writeFieldLength(strBytes.length);
            buffer.put(strBytes);
        } catch (UnsupportedEncodingException u) {
        }
        return this;
    }

    /**
     * Write timestamp in binary format.
     * @param calendar session calendar
     * @param ts timestamp to send
     * @param fractionalSeconds must fractionnal second be send to server
     * @return this
     */
    public PacketOutputStream writeTimestampLength(final Calendar calendar, Timestamp ts, boolean fractionalSeconds) {
        assureBufferCapacity(fractionalSeconds ? 12 : 8);
        buffer.put((byte) (fractionalSeconds ? 11 : 7));//length

        buffer.putShort((short) calendar.get(Calendar.YEAR));
        buffer.put((byte) ((calendar.get(Calendar.MONTH) + 1) & 0xff));
        buffer.put((byte) (calendar.get(Calendar.DAY_OF_MONTH) & 0xff));
        buffer.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
        buffer.put((byte) calendar.get(Calendar.MINUTE));
        buffer.put((byte) calendar.get(Calendar.SECOND));
        if (fractionalSeconds) {
            buffer.putInt(ts.getNanos() / 1000);
        }
        return this;
    }

    /**
     * Write date in binary format.
     * @param calendar date
     * @return this
     */
    public PacketOutputStream writeDateLength(final Calendar calendar) {
        assureBufferCapacity(8);
        buffer.put((byte) 7);//length
        buffer.putShort((short) calendar.get(Calendar.YEAR));
        buffer.put((byte) ((calendar.get(Calendar.MONTH) + 1) & 0xff));
        buffer.put((byte) (calendar.get(Calendar.DAY_OF_MONTH) & 0xff));
        buffer.put((byte) 0);
        buffer.put((byte) 0);
        buffer.put((byte) 0);
        return this;
    }

    /**
     * Write time in binary format.
     * @param calendar session calendar.
     * @param fractionalSeconds fractional seconds must be send
     * @return this
     */
    public PacketOutputStream writeTimeLength(final Calendar calendar, final boolean fractionalSeconds) {
        if (fractionalSeconds) {
            assureBufferCapacity(13);
            buffer.put((byte) 12);
            buffer.put((byte) 0);
            buffer.putInt(0);
            buffer.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
            buffer.put((byte) calendar.get(Calendar.MINUTE));
            buffer.put((byte) calendar.get(Calendar.SECOND));
            buffer.putInt(calendar.get(Calendar.MILLISECOND) * 1000);
        } else {
            assureBufferCapacity(9);
            buffer.put((byte) 8);//length
            buffer.put((byte) 0);
            buffer.putInt(0);
            buffer.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
            buffer.put((byte) calendar.get(Calendar.MINUTE));
            buffer.put((byte) calendar.get(Calendar.SECOND));
        }
        return this;
    }

    public boolean isClosed() {
        return closed;
    }

    public int getMaxAllowedPacket() {
        return maxAllowedPacket;
    }

    /**
     * Send bytes according to compression.
     * @param packetBuffer data to write
     * @param packetSize packet size
     * @throws IOException if connection to server fail
     */
    public void send(byte[] packetBuffer, int packetSize) throws IOException {
        if (!useCompression) {
            if (logger.isTraceEnabled()) {
                logger.trace("send packet seq:" + seqNo + " length:" + (packetSize - 4)
                        + " data:" + Utils.hexdump(packetBuffer, maxQuerySizeToLog, 4, packetSize - 4));
            }
            outputStream.write(packetBuffer, 0, packetSize);
        } else {
            this.setCompressSeqNo(0);
            compressedAndSend(packetSize, packetBuffer, true);
        }
    }

    /**
     * Send SQL string to outputStream.
     * SQL will be transform to UTF-8 byte buffer, and if possible this buffer will be send to stream directly.
     *
     * @param sql sql
     * @param commandType command type
     * @throws IOException if connection error occur.
     * @throws QueryException if query size is to big according to server max_allowed_size
     */
    public void send(String sql, byte commandType) throws IOException, QueryException {

        startPacket(0,true);
        int charsLength = sql.length();
        int charsOffset = 0;
        int position = 4;

        //create UTF-8 byte array
        //since java char are internally using UTF-16 using surrogate's pattern, 4 bytes unicode characters will
        //represent 2 characters : example "\uD83C\uDFA4" = 🎤 unicode 8 "no microphones"
        //so max size is 3 * charLength + 1 for query type
        assureBufferCapacity((charsLength * 3) + 1);

        byte[] arr = buffer.array();
        arr[position++] = commandType;

        while (charsOffset < charsLength) {
            char currChar = sql.charAt(charsOffset++);
            if (currChar < 0x80) {
                arr[position++] = (byte) currChar;
            } else if (currChar < 0x800) {
                arr[position++] = (byte) (0xc0 | (currChar >> 6));
                arr[position++] = (byte) (0x80 | (currChar & 0x3f));
            } else if (currChar >= 0xD800 && currChar < 0xE000) {
                //reserved for surrogate - see https://en.wikipedia.org/wiki/UTF-16
                if (currChar >= 0xD800 && currChar < 0xDC00) {
                    //is high surrogate
                    if (charsOffset + 1 > charsLength) {
                        arr[position++] = (byte)0x63;
                    } else {
                        char nextChar = sql.charAt(charsOffset);
                        if (nextChar >= 0xDC00 && nextChar < 0xE000) {
                            //is low surrogate
                            int surrogatePairs =  ((currChar << 10) + nextChar) + (0x010000 - (0xD800 << 10) - 0xDC00);
                            arr[position++] = (byte) (0xf0 | ((surrogatePairs >> 18)));
                            arr[position++] = (byte) (0x80 | ((surrogatePairs >> 12) & 0x3f));
                            arr[position++] = (byte) (0x80 | ((surrogatePairs >> 6) & 0x3f));
                            arr[position++] = (byte) (0x80 | (surrogatePairs & 0x3f));
                            charsOffset++;
                        } else {
                            //must have low surrogate
                            arr[position++] = (byte)0x63;
                        }
                    }
                } else {
                    //low surrogate without high surrogate before
                    arr[position++] = (byte)0x63;
                }
            } else {
                arr[position++] = (byte) (0xe0 | ((currChar >> 12)));
                arr[position++] = (byte) (0x80 | ((currChar >> 6) & 0x3f));
                arr[position++] = (byte) (0x80 | (currChar & 0x3f));
            }
        }

        if (position - 4 < maxPacketSize && !useCompression) {
            arr[0] = (byte) ((position - 4) & 0xff);
            arr[1] = (byte) ((position - 4) >>> 8);
            arr[2] = (byte) ((position - 4) >>> 16);
            arr[3] = (byte) 0;
            if (logger.isTraceEnabled()) {
                logger.trace("send packet seq:" + seqNo + " length:" + (position - 4)
                        + " data:" + Utils.hexdump(arr, maxQuerySizeToLog, 4, position - 4));
            }
            outputStream.write(arr, 0, position);
        } else {
            sendDirect(arr, 5, position - 5, commandType);
        }
    }

    /**
     * Send buffer to outputStream.
     *
     * @param sqlBytes buffer
     * @param offset offset
     * @param sqlLength length to send to stream
     * @param commandType command type
     * @throws IOException if connection error occur
     * @throws QueryException if query size is to big according to server max_allowed_size
     */
    public void sendDirect(byte[] sqlBytes, int offset, int sqlLength, byte commandType) throws IOException, QueryException {
        if (isClosed()) throw new IOException("Stream has already closed");
        int seqNo = 0;
        setCompressSeqNo(0);

        if (sqlLength + (useCompression ? 5 : 1) > getMaxAllowedPacket()) {
            throw new QueryException("Could not send query: query size " + (sqlLength + (useCompression ? 5 : 1))
                    + " is >= to max_allowed_packet (" + maxAllowedPacket + ")", -1, INTERRUPTED_EXCEPTION);
        }
        if (!isUseCompression()) {

            if (sqlLength + 1 <= maxPacketSize) {
                byte[] packetBuffer = new byte[sqlLength + 5];
                packetBuffer[0] = (byte) ((sqlLength + 1) & 0xff);
                packetBuffer[1] = (byte) ((sqlLength + 1) >>> 8);
                packetBuffer[2] = (byte) ((sqlLength + 1) >>> 16);
                packetBuffer[3] = (byte) seqNo++;
                packetBuffer[4] = commandType;

                System.arraycopy(sqlBytes, offset, packetBuffer, 5, sqlLength);
                if (logger.isTraceEnabled()) {
                    logger.trace("send packet seq:" + seqNo + " length:" + (sqlLength + 1)
                            + " data:" + Utils.hexdump(packetBuffer, maxQuerySizeToLog, 4, sqlLength + 1));
                }
                outputStream.write(packetBuffer);
            } else {
                //send first packet
                byte[] packetBuffer = new byte[maxPacketSize + 4];
                packetBuffer[0] = (byte) (maxPacketSize & 0xff);
                packetBuffer[1] = (byte) (maxPacketSize >>> 8);
                packetBuffer[2] = (byte) (maxPacketSize >>> 16);
                packetBuffer[3] = (byte) seqNo++;
                packetBuffer[4] = commandType;

                System.arraycopy(sqlBytes, offset, packetBuffer, 5, maxPacketSize - 1);
                int lengthAlreadySend = maxPacketSize - 1;
                if (logger.isTraceEnabled()) {
                    logger.trace("send packet seq:" + seqNo + " length:" + maxPacketSize
                            + " data:" + Utils.hexdump(packetBuffer, maxQuerySizeToLog, 4, maxPacketSize));
                }
                outputStream.write(packetBuffer);
                int length;

                while ((length = sqlLength - lengthAlreadySend) > 0) {
                    if (length > maxPacketSize) {
                        packetBuffer[0] = (byte) (maxPacketSize & 0xff);
                        packetBuffer[1] = (byte) (maxPacketSize >>> 8);
                        packetBuffer[2] = (byte) (maxPacketSize >>> 16);
                        packetBuffer[3] = (byte) seqNo++;
                        System.arraycopy(sqlBytes, offset + lengthAlreadySend, packetBuffer, 4, maxPacketSize);
                        if (logger.isTraceEnabled()) {
                            logger.trace("send packet seq:" + seqNo + " length:" + maxPacketSize
                                    + " data:" + Utils.hexdump(packetBuffer, maxQuerySizeToLog, 4, maxPacketSize));
                        }
                        outputStream.write(packetBuffer);
                        lengthAlreadySend += maxPacketSize;
                    } else {
                        packetBuffer[0] = (byte) (length & 0xff);
                        packetBuffer[1] = (byte) (length >>> 8);
                        packetBuffer[2] = (byte) (length >>> 16);
                        packetBuffer[3] = (byte) seqNo++;
                        System.arraycopy(sqlBytes, offset + lengthAlreadySend, packetBuffer, 4, length);
                        if (logger.isTraceEnabled()) {
                            logger.trace("send packet seq:" + seqNo + " length:" + length
                                    + " data:" + Utils.hexdump(packetBuffer, maxQuerySizeToLog, 4, length));

                        }
                        outputStream.write(packetBuffer, 0, length + 4);
                        break;
                    }
                }
            }
        } else {

            if (sqlLength < maxPacketSize) {
                byte[] packetBuffer = new byte[sqlLength + 5];
                packetBuffer[0] = (byte) ((sqlLength + 1) & 0xff);
                packetBuffer[1] = (byte) ((sqlLength + 1) >>> 8);
                packetBuffer[2] = (byte) ((sqlLength + 1) >>> 16);
                packetBuffer[3] = (byte) 0;
                packetBuffer[4] = commandType;

                System.arraycopy(sqlBytes, offset, packetBuffer, 5, sqlLength);
                compressedAndSend(sqlLength + 5, packetBuffer, true);

            } else {
                final int expectedPacketSize = sqlLength + 1 + 4 * (((sqlLength + 1) / maxPacketSize) + 1);

                //create packet
                byte[] packetBuffer = new byte[expectedPacketSize];
                packetBuffer[0] = (byte) (maxPacketSize & 0xff);
                packetBuffer[1] = (byte) (maxPacketSize >>> 8);
                packetBuffer[2] = (byte) (maxPacketSize >>> 16);
                packetBuffer[3] = (byte) seqNo++;
                packetBuffer[4] = commandType;
                System.arraycopy(sqlBytes, offset, packetBuffer, 5, maxPacketSize - 1);

                int sqlBytesPosition = maxPacketSize - 1;
                int positionDest = maxPacketSize + 4;

                int length;
                while ((length = sqlLength - sqlBytesPosition) > 0) {
                    if (length > maxPacketSize) {
                        packetBuffer[positionDest++] = (byte) (maxPacketSize & 0xff);
                        packetBuffer[positionDest++] = (byte) (maxPacketSize >>> 8);
                        packetBuffer[positionDest++] = (byte) (maxPacketSize >>> 16);
                        packetBuffer[positionDest++] = (byte) seqNo++;
                        System.arraycopy(sqlBytes, offset + sqlBytesPosition, packetBuffer, positionDest, maxPacketSize);
                        sqlBytesPosition += maxPacketSize;
                        positionDest += maxPacketSize;
                    } else {
                        packetBuffer[positionDest++] = (byte) (length & 0xff);
                        packetBuffer[positionDest++] = (byte) (length >>> 8);
                        packetBuffer[positionDest++] = (byte) (length >>> 16);
                        packetBuffer[positionDest++] = (byte) seqNo++;
                        System.arraycopy(sqlBytes, offset + sqlBytesPosition, packetBuffer, positionDest, length);
                        break;
                    }
                }
                compressedAndSend(expectedPacketSize, packetBuffer, true);
            }
        }
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    public boolean isUseCompression() {
        return useCompression;
    }

    /**
     * Send COM_STMT_CLOSE packet.
     * @param statementId statement id to close.
     * @throws IOException if connection error occur.
     */
    public void closePrepare(int statementId) throws IOException {
        byte[] packetBuffer;
        if (useCompression) {
            packetBuffer = new byte[12];
            packetBuffer[0] = (byte) 5;
            //packetBuffer[1,2,3,4,5,6] = (byte) 0;
            packetBuffer[7] = Packet.COM_STMT_CLOSE;
            packetBuffer[8] = (byte) (statementId & 0xff);
            packetBuffer[9] = (byte) ((statementId >> 8) & 0xff);
            packetBuffer[10] = (byte) ((statementId >> 16) & 0xff);
            packetBuffer[11] = (byte) ((statementId >> 24) & 0xff);
        } else {
            packetBuffer = new byte[9];
            packetBuffer[0] = (byte) 5; //packet length 1st byte
            //packetBuffer[1,2,3] = (byte) 0;
            packetBuffer[4] = Packet.COM_STMT_CLOSE;
            packetBuffer[5] = (byte) (statementId & 0xff);
            packetBuffer[6] = (byte) ((statementId >> 8) & 0xff);
            packetBuffer[7] = (byte) ((statementId >> 16) & 0xff);
            packetBuffer[8] = (byte) ((statementId >> 24) & 0xff);
        }
        if (logger.isTraceEnabled()) {
            logger.trace("send packet seq:" + seqNo + " length:" + 5
                    + " data:" + Utils.hexdump(packetBuffer, maxQuerySizeToLog));
        }
        outputStream.write(packetBuffer);
    }

}

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

package org.mariadb.jdbc.internal.io.output;

import org.mariadb.jdbc.internal.util.Utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.DeflaterOutputStream;

public class CompressPacketOutputStream extends AbstractPacketOutputStream {

    private static final int MAX_PACKET_LENGTH = 0x00ffffff;
    private static final byte[] EMPTY_ARRAY = new byte[0];

    private int maxPacketLength = MAX_PACKET_LENGTH;
    private static final int MIN_COMPRESSION_SIZE = 100;
    private static final float MIN_COMPRESSION_RATIO = 0.9f;
    private int compressSeqNo;
    private byte[] header = new byte[7];
    private byte[] subHeader = new byte[4];
    private byte[] remainingData = new byte[0];
    private boolean lastPacketExactMaxPacketLength = false;

    public CompressPacketOutputStream(OutputStream out, int maxQuerySizeToLog) {
        super(out, maxQuerySizeToLog);
    }

    public int getMaxPacketLength() {
        return maxPacketLength;
    }

    @Override
    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
        maxPacketLength = Math.min(MAX_PACKET_LENGTH, maxAllowedPacket + 7);
    }

    @Override
    public void startPacket(int compressSeqNo) {
        this.compressSeqNo = compressSeqNo;
        this.seqNo = 0;
        pos = 0;
        cmdLength = 0;
        remainingData = EMPTY_ARRAY;
        lastPacketExactMaxPacketLength = false;
    }

    /**
     * Flush the internal buffer.
     *
     * Compression add a 7 header :
     * <ol>
     *  <li>3 byte compression length</li>
     *  <li>1 byte compress sequence number</li>
     *  <li>3 bytes uncompress length</li>
     * </ol>
     *
     * in case packet isn't compressed (last 3 bytes == 0):
     * <ol>
     *  <li>3 byte uncompress length</li>
     *  <li>1 byte compress sequence number</li>
     *  <li>3 bytes with 0 value</li>
     * </ol>
     *
     * Content correspond to standard content.
     * <ol>
     *  <li>3 byte length</li>
     *  <li>1 byte sequence number (!= than compress sequence number)</li>
     *  <li>sub-content</li>
     * </ol>
     *
     * Problem is when standard content is bigger than 16mb :
     * content will not send 4byte standard header + 16mb content, since packet are limited to 16mb
     * then 4 bytes standard header + 16mb - 4 bytes content. the ending 4 bytes are waiting to be send.
     * next packet will then send the waiting data before next packet, putting more waiting data is needed.
     * if ending data is exactly MAX_PACKET_LENGTH length, then an empty packet must be send.
     *
     * @param commandEnd  command end
     * @throws IOException id connection error occur.
     */
    protected void flushBuffer(boolean commandEnd) throws IOException {
        if (pos > 0) {
            if (pos + remainingData.length > MIN_COMPRESSION_SIZE) {

                byte[] compressedBytes;
                int uncompressSize = Math.min(MAX_PACKET_LENGTH, remainingData.length + 4 + pos);
                checkMaxAllowedLength(uncompressSize);
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    try (DeflaterOutputStream deflater = new DeflaterOutputStream(baos)) {
                        if (remainingData.length != 0) deflater.write(remainingData);
                        subHeader[0] = (byte) (pos >>> 0);
                        subHeader[1] = (byte) (pos >>> 8);
                        subHeader[2] = (byte) (pos >>> 16);
                        subHeader[3] = (byte) this.seqNo++;
                        deflater.write(subHeader, 0, 4);
                        deflater.write(buf, 0, uncompressSize - (remainingData.length + 4));
                        deflater.finish();
                    }
                    compressedBytes = baos.toByteArray();
                    if (pos + remainingData.length + 4 - uncompressSize > 0) {
                        remainingData = Arrays.copyOfRange(buf, uncompressSize - (remainingData.length + 4), pos);
                    } else {
                        remainingData = EMPTY_ARRAY;
                    }
                }

                if (compressedBytes.length < (int) (MIN_COMPRESSION_RATIO * pos)) {
                    int compressedLength = compressedBytes.length;

                    header[0] = (byte) (compressedLength >>> 0);
                    header[1] = (byte) (compressedLength >>> 8);
                    header[2] = (byte) (compressedLength >>> 16);
                    header[3] = (byte) this.compressSeqNo++;
                    header[4] = (byte) (uncompressSize >>> 0);
                    header[5] = (byte) (uncompressSize >>> 8);
                    header[6] = (byte) (uncompressSize >>> 16);
                    out.write(header, 0, 7);
                    out.write(compressedBytes, 0, compressedLength);

                    if (logger.isTraceEnabled()) {
                        if (permitTrace) {
                            logger.trace("send compress: length:(zlib:" + compressedLength + ",std:" + uncompressSize + ")"
                                    + serverThreadLog
                                    + " packet:0x"
                                    + Utils.hexdump(header, maxQuerySizeToLog, 0, 7)
                                    + Utils.hexdump(compressedBytes, maxQuerySizeToLog - 7, 0, compressedLength));
                        } else {
                            logger.trace("send compress: length:(zlib:" + compressedLength + ",std:" + uncompressSize + ")"
                                    + serverThreadLog
                                    + " packet:<hidden>");
                        }
                    }

                    //if last packet fill the max size, must send an empty packet to indicate command end.
                    lastPacketExactMaxPacketLength = pos == MAX_PACKET_LENGTH;
                    if (commandEnd && lastPacketExactMaxPacketLength) writeEmptyPacket();
                    pos = 0;
                    return;
                }
            }

            int uncompressSize = Math.min(MAX_PACKET_LENGTH, remainingData.length + 4 + pos);
            checkMaxAllowedLength(uncompressSize);

            //send packet without compression
            header[0] = (byte) (uncompressSize >>> 0);
            header[1] = (byte) (uncompressSize >>> 8);
            header[2] = (byte) (uncompressSize >>> 16);
            header[3] = (byte) this.compressSeqNo++;
            header[4] = (byte) 0x00;
            header[5] = (byte) 0x00;
            header[6] = (byte) 0x00;
            out.write(header, 0, 7);

            if (remainingData.length != 0) out.write(remainingData);
            subHeader[0] = (byte) (pos >>> 0);
            subHeader[1] = (byte) (pos >>> 8);
            subHeader[2] = (byte) (pos >>> 16);
            subHeader[3] = (byte) this.seqNo++;
            out.write(subHeader, 0, 4);
            out.write(buf, 0, uncompressSize - (remainingData.length + 4));
            if (pos + remainingData.length + 4 - uncompressSize > 0) {
                remainingData = Arrays.copyOfRange(buf, uncompressSize - (remainingData.length + 4), pos);
            } else {
                remainingData = EMPTY_ARRAY;
            }

            if (logger.isTraceEnabled()) {
                if (permitTrace) {
                    logger.trace("send compress: length:(zlib:0,std:" + uncompressSize + ")"
                            + serverThreadLog
                            + " packet:0x"
                            + Utils.hexdump(header, maxQuerySizeToLog, 0, pos)
                            + Utils.hexdump(remainingData, maxQuerySizeToLog - pos, 0, remainingData.length)
                            + Utils.hexdump(subHeader, maxQuerySizeToLog - (pos + remainingData.length), 0, 4)
                            + Utils.hexdump(buf, maxQuerySizeToLog - (pos + remainingData.length + 4), 0, pos));
                } else {
                    logger.trace("send compress: length:(zlib:0,std:" + uncompressSize + ")"
                            + serverThreadLog
                            + " packet:<hidden>");
                }
            }

            //if last packet fill the max size, must send an empty packet to indicate command end.
            lastPacketExactMaxPacketLength = pos == MAX_PACKET_LENGTH;
            pos = 0;
        }

        if (remainingData.length > 0) {
            if (remainingData.length > MIN_COMPRESSION_SIZE) {

                byte[] compressedBytes;
                int uncompressSize = Math.min(MAX_PACKET_LENGTH, remainingData.length);
                checkMaxAllowedLength(uncompressSize);
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    try (DeflaterOutputStream deflater = new DeflaterOutputStream(baos)) {
                        deflater.write(remainingData);
                        deflater.finish();
                    }
                    compressedBytes = baos.toByteArray();
                    remainingData = EMPTY_ARRAY;
                }

                if (compressedBytes.length < (int) (MIN_COMPRESSION_RATIO * pos)) {
                    int compressedLength = compressedBytes.length;

                    header[0] = (byte) (compressedLength >>> 0);
                    header[1] = (byte) (compressedLength >>> 8);
                    header[2] = (byte) (compressedLength >>> 16);
                    header[3] = (byte) this.compressSeqNo++;
                    header[4] = (byte) (uncompressSize >>> 0);
                    header[5] = (byte) (uncompressSize >>> 8);
                    header[6] = (byte) (uncompressSize >>> 16);
                    out.write(header, 0, 7);
                    out.write(compressedBytes, 0, compressedLength);

                    if (logger.isTraceEnabled()) {
                        if (permitTrace) {
                            logger.trace("send compress: length:(zlib:" + compressedLength + ",std:" + uncompressSize + ")"
                                    + serverThreadLog
                                    + " packet:0x"
                                    + Utils.hexdump(header, maxQuerySizeToLog, 0, 7)
                                    + Utils.hexdump(compressedBytes, maxQuerySizeToLog - 7, 0, compressedLength));
                        } else {
                            logger.trace("send compress: length:(zlib:" + compressedLength + ",std:" + uncompressSize + ")"
                                    + serverThreadLog
                                    + " packet:<hidden>");
                        }
                    }

                    //if last packet fill the max size, must send an empty packet to indicate command end.
                    if (commandEnd && lastPacketExactMaxPacketLength) writeEmptyPacket();
                    return;
                }
            }

            int uncompressSize = Math.min(MAX_PACKET_LENGTH, remainingData.length);
            checkMaxAllowedLength(uncompressSize);

            //send packet without compression
            header[0] = (byte) (uncompressSize >>> 0);
            header[1] = (byte) (uncompressSize >>> 8);
            header[2] = (byte) (uncompressSize >>> 16);
            header[3] = (byte) this.compressSeqNo++;
            header[4] = (byte) 0x00;
            header[5] = (byte) 0x00;
            header[6] = (byte) 0x00;
            out.write(header, 0, 7);

            out.write(remainingData);
            remainingData = EMPTY_ARRAY;

            if (logger.isTraceEnabled()) {
                if (permitTrace) {
                    logger.trace("send compress: length:(zlib:0,std:" + uncompressSize + ")"
                            + serverThreadLog
                            + " packet:0x"
                            + Utils.hexdump(header, maxQuerySizeToLog, 0, pos)
                            + Utils.hexdump(remainingData, maxQuerySizeToLog - pos, 0, remainingData.length));
                } else {
                    logger.trace("send compress: length:(zlib:0,std:" + uncompressSize + ")"
                            + serverThreadLog
                            + " packet:<hidden>");
                }
            }
            if (commandEnd && lastPacketExactMaxPacketLength) writeEmptyPacket();
        }
    }

    /**
     * Write an empty packet.
     *
     * @throws IOException if socket error occur.
     */
    public void writeEmptyPacket() throws IOException {
        buf[0] = (byte) (4 >>> 0);
        buf[1] = (byte) 0x00;
        buf[2] = (byte) 0x00;
        buf[3] = (byte) this.compressSeqNo++;
        buf[4] = (byte) 0x00;
        buf[5] = (byte) 0x00;
        buf[6] = (byte) 0x00;
        buf[7] = (byte) 0x00;
        buf[8] = (byte) 0x00;
        buf[9] = (byte) 0x00;
        buf[10] = (byte) this.seqNo++;
        out.write(buf, 0, 11);
        if (logger.isTraceEnabled()) {
            if (permitTrace) {
                logger.trace("send compress: length:(zlib:0,std:0)"
                        + serverThreadLog
                        + " packet:0x" + Utils.hexdump(buf, maxQuerySizeToLog, 0, 11));
            } else {
                logger.trace("send compress: length:(zlib:0,std:0)"
                        + serverThreadLog
                        + " packet:<hidden>");
            }
        }
    }

}

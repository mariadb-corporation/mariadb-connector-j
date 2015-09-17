package org.mariadb.jdbc.internal.common.packet;

import org.mariadb.jdbc.internal.common.MySQLCharset;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.zip.DeflaterOutputStream;


public class PacketOutputStream extends OutputStream {
    //private final static Logger log = LoggerFactory.getLogger(PacketOutputStream.class);
    private static final int MIN_COMPRESSION_SIZE = 16*1024;
    private static final float MIN_COMPRESSION_RATIO =0.9f;
    private static final int MAX_PACKET_LENGTH = 0x00ffffff;
    private static final int HEADER_LENGTH = 4;
    private float increasing = 1.5f;
    public ByteBuffer buffer;
    private OutputStream outputStream;

    int seqNo;
    int maxAllowedPacket;
    boolean checkPacketLength;
    boolean useCompression;


    protected void increase(int newCapacity) {
        buffer.limit(buffer.position());
        buffer.rewind();

        ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);

        newBuffer.put(buffer);
        buffer.clear();
        buffer = newBuffer;
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public PacketOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
        buffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
        this.seqNo = -1;
        useCompression = false;
    }

    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    public void startPacket(int seqNo, boolean checkPacketLength) throws IOException {
        if (this.seqNo != -1) {
            throw new IOException("Last packet not finished");
        }
        this.seqNo = seqNo;
        buffer.clear();
        this.checkPacketLength = checkPacketLength;
    }

    public void startPacket(int seqNo) throws IOException {
        startPacket(seqNo, true);
    }

    private void writeEmptyPacket(int seqNo) throws IOException {
        byte[] buf = new byte[4];
        buf[0] = ((byte) 0);
        buf[1] = ((byte) 0);
        buf[2] = ((byte) 0);
        buf[3] = ((byte) seqNo);
        outputStream.write(buf, 0, 4);
    }

    /* Used by LOAD DATA INFILE. End of data is indicated by packet of length 0. */
    public void sendFile(InputStream is, int seq) throws IOException {

        int bufferSize = this.maxAllowedPacket > 0 ? Math.min(this.maxAllowedPacket - HEADER_LENGTH, MAX_PACKET_LENGTH) : 1024;
        bufferSize -= HEADER_LENGTH;
        byte[] buffer = new byte[bufferSize];
        int len;
        while ((len = is.read(buffer)) > 0) {
            startPacket(seq++, false);
            write(buffer, 0, len);
            finishPacket();
        }
        writeEmptyPacket(seq);
    }

    public void sendStream(InputStream is, MySQLCharset charset) throws IOException {
        byte[] buffer = new byte[8192];
        int len;
        while ((len = is.read(buffer)) > 0) {
            write(buffer, 0, len);
        }
    }

    public void sendStream(InputStream is, long readLength, MySQLCharset charset) throws IOException {
        byte[] buffer = new byte[(int) readLength];
        int len;
        while ((len = is.read(buffer, 0, (int) readLength)) > 0) {
            write(buffer, 0, len);
            if (len >= readLength) return;
        }
    }

    public void sendStream(java.io.Reader reader, MySQLCharset charset) throws IOException {
        char[] buffer = new char[8192];
        int len;
        while ((len = reader.read(buffer)) > 0) {
            byte[] s = new String(buffer, 0, len).getBytes(charset.javaIoCharsetName);
            write(s, 0, s.length);
        }
    }

    public void sendStream(java.io.Reader reader, long readLength, MySQLCharset charset) throws IOException {
        char[] buffer = new char[8192];
        int len;
        while ((len = reader.read(buffer, 0, (int) readLength)) > 0) {
            byte[] s = new String(buffer, 0, len).getBytes(charset.javaIoCharsetName);
            write(s, 0, s.length);
            if (len >= readLength) return;
        }
    }


    public void finishPacket() throws IOException {
        if (this.seqNo == -1) {
            throw new AssertionError("Packet not started");
        }
        internalFlush();
        buffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
        this.seqNo = -1;
    }

    private int maxPacketSize() {
        if (maxAllowedPacket > 0)
            return Math.min(maxAllowedPacket - HEADER_LENGTH, MAX_PACKET_LENGTH);
        else return MAX_PACKET_LENGTH;
    }


    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (this.seqNo == -1) throw new AssertionError("Use PacketOutputStream.startPacket() before write()");

        while (len > buffer.remaining()){
            int newCapacity = Math.max(len + buffer.position(), (int)(buffer.capacity() * increasing));
            increase(newCapacity);
        }
        buffer.put(b, 0, len);
    }


    @Override
    public void flush() throws IOException {
        throw new AssertionError("Do not call flush() on PacketOutputStream. use finishPacket() instead.");
    }

    private void internalFlush() throws IOException {
        buffer.flip();
        if (buffer.limit() > 0) {
            splitPacket();
        }
        buffer.clear();
    }
    private void splitPacket() throws IOException {
        int maxPacketSize = maxPacketSize();
        if (checkPacketLength
                && maxAllowedPacket > 0
                && (buffer.limit() + HEADER_LENGTH * ((buffer.limit() / maxPacketSize) + 1)) > maxAllowedPacket) {
            this.seqNo = -1;
            throw new MaxAllowedPacketException("max_allowed_packet exceeded. packet size " + (buffer.limit() + HEADER_LENGTH * ((buffer.limit() / maxPacketSize) + 1)) + " is > to max_allowed_packet = " + maxAllowedPacket, this.seqNo != 0);
        }

        byte[] bufferBytes = new byte[0];
        int notCompressPosition = 0;
        int expectedPacketSize = buffer.limit() + HEADER_LENGTH * ((buffer.limit() / maxPacketSize) + 1);
        if (useCompression) bufferBytes = new byte[expectedPacketSize];

        while (notCompressPosition < expectedPacketSize) {
            int length = buffer.remaining();
            if (buffer.remaining() > maxPacketSize) length = maxPacketSize;
            if (useCompression) {
                bufferBytes[notCompressPosition++] = (byte) (length & 0xff);
                bufferBytes[notCompressPosition++] = (byte) (length >>> 8);
                bufferBytes[notCompressPosition++] = (byte) (length >>> 16);
                bufferBytes[notCompressPosition++] = (byte) seqNo++;
            } else {
                byte[] header = new byte[HEADER_LENGTH];
                header[0] = (byte) (length & 0xff);
                header[1] = (byte) (length >>> 8);
                header[2] = (byte) (length >>> 16);
                header[3] = (byte) seqNo++;
                outputStream.write(header);
                notCompressPosition+=4;
            }

            if (length > 0) {
                if (useCompression) {
                    buffer.get(bufferBytes, notCompressPosition, length);
                    notCompressPosition += length;
                } else {
                    if (buffer.hasArray()) {
                        outputStream.write(buffer.array(), buffer.position(), length);
                        buffer.position(buffer.position() + length);
                    } else {
                        bufferBytes = new byte[length];
                        buffer.get(bufferBytes, 0, length);
                        outputStream.write(bufferBytes, 0, length);
                    }
                    notCompressPosition+=length;
                    outputStream.flush();
                }
            }
        }


        if (useCompression) {
            //now bufferBytes in filled with uncompressed data
            this.seqNo=0;
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

                    if (compressedBytes.length < (int)(MIN_COMPRESSION_RATIO * packetLength)) {

                        int compressedLength = compressedBytes.length;
                        writeCompressedHeader(compressedLength, packetLength, outputStream);
                        outputStream.write(compressedBytes, 0, compressedLength);
                        compressedPacketSend = true;
                    }
                }

                if (!compressedPacketSend) {
                    writeCompressedHeader(packetLength, 0, outputStream);
                    outputStream.write(bufferBytes, position, packetLength);
                }

                position+=packetLength;
                outputStream.flush();
            }
        }
    }


    private void writeCompressedHeader(int packetLength, int initialLength, OutputStream outputStream) throws IOException {
        byte header[] = new byte[7];
        header[0] = (byte)(packetLength & 0xff);
        header[1] = (byte)((packetLength >> 8) & 0xff);
        header[2] = (byte)((packetLength >> 16) & 0xff);
        header[3] = (byte) seqNo++;
        header[4] = (byte)(initialLength & 0xff);
        header[5] = (byte)((initialLength >> 8) & 0xff);
        header[6] = (byte)((initialLength >> 16) & 0xff);
        outputStream.write(header);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(int b) throws IOException {
        byte[] a = {(byte) b};
        write(a);
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
        buffer = null;
    }

    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
    }

    public PacketOutputStream assureBufferCapacity(final int len) {
        while (len > buffer.remaining()){
            int newCapacity = Math.max(len +  buffer.position() , (int)(buffer.capacity() * increasing));
            increase(newCapacity);
        }
        return this;
    }


    public PacketOutputStream writeByte(final byte theByte) {
        assureBufferCapacity(1);
        buffer.put(theByte);
        return this;
    }

    public PacketOutputStream writeBytes(final byte theByte, final int count) {
        for (int i = 0; i < count; i++) {
            this.writeByte(theByte);
        }
        return this;
    }


    public PacketOutputStream writeByteArray(final byte[] bytes) {
        assureBufferCapacity(bytes.length);
        buffer.put(bytes);
        return this;
    }

    public PacketOutputStream writeByteArrayLength(final byte[] bytes) {
        assureBufferCapacity(bytes.length + 9);
        writeFieldLength(bytes.length);
        buffer.put(bytes);
        return this;
    }

    public PacketOutputStream writeString(final String str) {
        final byte[] strBytes;
        strBytes = str.getBytes(StandardCharsets.UTF_8);
        return writeByteArray(strBytes);

    }

    public PacketOutputStream writeShort(final short theShort) {
        assureBufferCapacity(2);
        buffer.putShort(theShort);
        return this;
    }

    public PacketOutputStream writeInt(final int theInt) {
        assureBufferCapacity(4);
        buffer.putInt(theInt);
        return this;
    }


    public PacketOutputStream writeLong(final long theLong) {
        assureBufferCapacity(8);
        buffer.putLong(theLong);
        return this;
    }

    public PacketOutputStream writeFieldLength(long length) {
        if (length < 251) {
            buffer.put((byte) length);
        } else if (length < 65536) {
            assureBufferCapacity(3);
            buffer.put((byte) 0xfc);
            buffer.putShort((short) length);
        } else if (length < 16777216) {
            assureBufferCapacity(4);
            buffer.put((byte)0xfd);
            buffer.put((byte) (length & 0xff) );
            buffer.put((byte) (length >>> 8) );
            buffer.put((byte) (length >>> 16));
        } else {
            assureBufferCapacity(9);
            buffer.put((byte) 0xfe);
            buffer.putLong(length);
        }
        return this;
    }


    public PacketOutputStream writeStringLength(final String str) {
        final byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
        assureBufferCapacity(strBytes.length + 9);
        writeFieldLength(strBytes.length);
        buffer.put(strBytes);
        return this;
    }


    public PacketOutputStream writeTimestampLength(final Calendar calendar, Timestamp ts) {
        assureBufferCapacity(12);
        buffer.put((byte) 11);//length

        buffer.putShort((short) calendar.get(Calendar.YEAR));
        buffer.put((byte) ((calendar.get(Calendar.MONTH) + 1) & 0xff));
        buffer.put((byte) (calendar.get(Calendar.DAY_OF_MONTH) & 0xff));
        buffer.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
        buffer.put((byte) calendar.get(Calendar.MINUTE));
        buffer.put((byte) calendar.get(Calendar.SECOND));
        buffer.putInt(ts.getNanos() / 1000);
        return this;
    }

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

}

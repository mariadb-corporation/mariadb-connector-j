package org.mariadb.jdbc.internal.common.packet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.sql.Timestamp;
import java.util.Calendar;


public class PacketOutputStream extends OutputStream {
    private final static Logger log = LoggerFactory.getLogger(PacketOutputStream.class);

    private static final int MAX_PACKET_LENGTH = 0x00ffffff;
    private static final int HEADER_LENGTH = 4;
    private float increasing = 1.5f;

    private ByteBuffer buffer;
    private WritableByteChannel socketChannel;

    int seqNo;
    int maxAllowedPacket;
    boolean checkPacketLength;


    protected void increase(int newCapacity) {
        buffer.limit(buffer.position());
        buffer.rewind();

        ByteBuffer newBuffer = ByteBuffer.allocateDirect(newCapacity);

        newBuffer.put(buffer);
        buffer.clear();
        buffer = newBuffer;
    }

    public PacketOutputStream(WritableByteChannel socketChannel) {
        this.socketChannel = socketChannel;
        buffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
        this.seqNo = -1;
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

    public int getSeqNo() {
        return seqNo;
    }

    private void writeEmptyPacket(int seqNo) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.clear();
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) seqNo);
        buf.flip();

        while(buf.hasRemaining()) {
            socketChannel.write(buf);
        }
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

    public void sendStream(InputStream is) throws IOException {
        int bufferSize = this.maxAllowedPacket > 0 ? Math.min(this.maxAllowedPacket - HEADER_LENGTH, MAX_PACKET_LENGTH) : 1024;
        byte[] buffer = new byte[bufferSize];
        int len;
        while ((len = is.read(buffer)) > 0) {
            write(buffer, 0, len);
        }
    }

    public void sendStream(InputStream is, long readLength) throws IOException {
        int bufferSize = this.maxAllowedPacket > 0 ? Math.min(this.maxAllowedPacket - HEADER_LENGTH, MAX_PACKET_LENGTH) : 1024;
        byte[] buffer = new byte[bufferSize];
        int len;
        while ((len = is.read(buffer, 0, (int) readLength)) > 0) {
            write(buffer, 0, len);
            if (len >= readLength) return;
        }
    }

    public void sendStream(java.io.Reader reader) throws IOException {
        int bufferSize = this.maxAllowedPacket > 0 ? Math.min(this.maxAllowedPacket - HEADER_LENGTH, MAX_PACKET_LENGTH) : 1024;
        char[] buffer = new char[bufferSize];
        int len;
        while ((len = reader.read(buffer)) > 0) {
            byte[] s = new String(buffer, 0, len).getBytes("UTF-8");
            write(s, 0, s.length);
        }
    }

    public void sendStream(java.io.Reader reader, long readLength) throws IOException {
        int bufferSize = this.maxAllowedPacket > 0 ? Math.min(this.maxAllowedPacket - HEADER_LENGTH, MAX_PACKET_LENGTH) : 1024;
        char[] buffer = new char[bufferSize];
        int len;
        while ((len = reader.read(buffer, 0, (int) readLength)) > 0) {
            byte[] s = new String(buffer, 0, len).getBytes("UTF-8");
            write(s, 0, s.length);
            if (len >= readLength) return;
        }
    }


    public void finishPacket() throws IOException {
        if (this.seqNo == -1) {
            throw new AssertionError("Packet not started");
        }
        internalFlush();
        this.seqNo = -1;
    }

    private boolean hasSizeReachedPacketLimit() {
        if (maxAllowedPacket > 0 && buffer.position() >= maxAllowedPacket - HEADER_LENGTH) return true;
        if (buffer.position() > MAX_PACKET_LENGTH) return true;
        return false;
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
            int newCapacity = Math.max(len, (int)(buffer.capacity() * increasing));
            increase(newCapacity);
        }

        if (len <= buffer.remaining()) {
            buffer.put(b, 0, len);
        } else {
            int bytesToWrite = buffer.remaining();
            while (len > 0) {
                buffer.put(b, 0, bytesToWrite);
                if (buffer.remaining() == 0) internalFlush();
                len -= bytesToWrite;
                bytesToWrite = Math.min(len, buffer.remaining());
            }
        }
        if (hasSizeReachedPacketLimit()) internalFlush();
    }


    @Override
    public void flush() throws IOException {
        throw new AssertionError("Do not call flush() on PacketOutputStream. use finishPacket() instead.");
    }

    private void internalFlush() throws IOException {
        internalFlush(0);
    }
    private void internalFlush(int bytesWritten) throws IOException {
        buffer.flip();

        int position = buffer.limit();

        if (maxAllowedPacket > 0 && bytesWritten + position > maxAllowedPacket && checkPacketLength) {
            this.seqNo = -1;
            throw new MaxAllowedPacketException("max_allowed_packet exceeded. wrote " + bytesWritten + ", max_allowed_packet = " + maxAllowedPacket, this.seqNo != 0);
        }

        if (position > maxPacketSize()) {
            int length = maxPacketSize();
            bytesWritten+=length;
            ByteBuffer bufHeader = ByteBuffer.allocate(4);
            bufHeader.clear();
            bufHeader.put((byte) (length & 0xff));
            bufHeader.put((byte) ((length >> 8) & 0xff));
            bufHeader.put((byte) ((length >> 16) & 0xff));
            bufHeader.put((byte) seqNo);
            bufHeader.flip();

            buffer.position(length);
            buffer.flip();

            while (bufHeader.remaining() > 0)socketChannel.write(bufHeader);
            while (buffer.remaining() > 0)socketChannel.write(buffer);

            buffer.limit(position);
            buffer.compact();
            this.seqNo++;
            internalFlush(bytesWritten);
        } else {

            ByteBuffer bufHeader = ByteBuffer.allocate(4);
            bufHeader.clear();
            bufHeader.put((byte) (position & 0xff));
            bufHeader.put((byte) ((position >> 8) & 0xff));
            bufHeader.put((byte) ((position >> 16) & 0xff));
            bufHeader.put((byte) seqNo);
            bufHeader.flip();

            while (bufHeader.remaining() > 0)socketChannel.write(bufHeader);
            while (buffer.remaining() > 0)socketChannel.write(buffer);

            buffer.clear();
            this.seqNo++;
        }
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
        socketChannel.close();
        buffer = null;
    }

    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
    }

    public PacketOutputStream assureBufferCapacity(final int len) {
        while (len > buffer.remaining()){
            int newCapacity = Math.max(len, (int)(buffer.capacity() * increasing));
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
        try {
            strBytes = str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 not supported", e);
        }
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
        final byte[] strBytes;
        try {
            strBytes = str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 not supported", e);
        }
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

package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public final class ByteArrayBuffer {
    
    /**
     * Default page size.
     */
    public static final int DEFAULT_PAGE_SIZE = 4096;
    
    /**
     * Bigger page size for stream.
     */
    public static final int DEFAULT_PAGE_SIZE_STREAM = 32768;
    
    /**
     * Current byte buffer.
     */
    private ByteArray buffer;
    
    /**
     * All byte buffer arrays.
     */
    private final List<ByteArray> buffers;
    
    /**
     * Position overall buffers.
     */
    private int pos;
    
    private int limit;
    
    private int compressRead;
    
    /**
     * Create a byte array buffer with first byte array element with one page size.
     */
    public ByteArrayBuffer() {
        this.buffer = new ByteArray(DEFAULT_PAGE_SIZE);
        this.buffers = new ArrayList<ByteArray>(16);
        this.buffers.add(this.buffer);
        this.limit = 0;
        this.compressRead = 0;
    }
    
    /**
     * Returns the number of elements between the current position and the limit.
     *
     * @return The number of elements remaining in this buffer
     */
    public int remaining() {
        return this.limit - this.pos;
    }
    
    /**
     * Returns this buffer's position.
     *
     * @return The position of this buffer
     */
    public int position() {
        return pos;
    }
    
    /**
     * recycle this buffer (reset).
     */
    public void recycle() {
        this.limit = 0;
        this.pos = 0;
        this.compressRead = 0;
        this.buffer = this.buffers.get(0);
        this.buffer.pos = 0;
        this.buffers.clear();
        this.buffers.add(this.buffer);
    }
    
    /**
     * Relative bulk <i>get</i> method.
     * 
     * @param bufferBytes
     *            The array into which bytes are to be written.
     * @param off
     *            The offset within the array of the first byte to be written.
     * @param len
     *            The maximum number of bytes to be written to the given array
     * @return the number of bytes put into the given buffer.
     */
    public int get(byte[] bufferBytes, int off, int len) {
        ByteArray array;
        int index = off;
        for (int n = this.buffers.size(); compressRead < n; compressRead++) {
            array = this.buffers.get(compressRead);
            if (array == null) {
                break;
            }
            if (pos + len > array.pos) {
                System.arraycopy(array.buf, 0, bufferBytes, index, array.pos);
                index += array.pos;
            } else {
                break;
            }
        }
        return index - off;
    }
    
    /**
     * Write all byte buffers to the given {@link OutputStream}.
     * 
     * @param outputStream
     *            the output stream to write buffers.
     * 
     * @throws IOException
     *             if any error occur during data send to server
     */
    public void writeTo(OutputStream outputStream) throws IOException {
        for (int i = 0, n = this.buffers.size(); i < n; i++) {
            ByteArray array = this.buffers.get(i);
            if (array == null) {
                break;
            }
            outputStream.write(array.buf, 0, array.pos);
        }
    }
    
    /**
     * prepare this byte array buffer to be written.
     */
    public void prepare() {
        limit = 0;
        for (int i = 0, n = this.buffers.size(); i < n; i++) {
            if (this.buffers.get(i) == null) {
                break;
            }
            limit += this.buffers.get(i).pos;
        }
        pos = 0;
    }
    
    /**
     * Relative bulk <i>put</i> method.
     * 
     * @param src
     *            the bytes array to put into this byte array buffer.
     * @param off
     *            The offset within the array of the first byte to be read.
     *
     * @param len
     *            The number of bytes to be read from the given array.
     */
    public void put(byte[] src, int off, int len) {
        ByteArray array = this.buffer;
        if (array.remaining() > len) {
            System.arraycopy(src, off, array.buf, array.pos, len);
            array.pos += len;
            this.pos += len;
        } else {
            if (off == 0 && len == src.length) {
                this.buffer = new ByteArray(src);
                this.buffers.add(this.buffer);
                allocate(DEFAULT_PAGE_SIZE);
                this.pos += len;
            } else {
                if (len > DEFAULT_PAGE_SIZE) {
                    array = new ByteArray(len);
                } else {
                    array = new ByteArray(DEFAULT_PAGE_SIZE);
                }
                System.arraycopy(src, 0, array.buf, 0, len);
                array.pos = len;
                this.pos += len;
            }
        }
    }
       
    /**
     * Relative <i>put</i> method for writing a byte value (1 byte).
     *
     * @param value
     *            The byte value to be written
     */
    public void put(byte value) {
        if (this.buffer.assureBufferCapacity(1)) {
            allocate(DEFAULT_PAGE_SIZE);
        }
        buffer.put(value);
        this.pos += 1;
    }
    
    /**
     * Relative <i>put</i> method for writing a short value (2 bytes).
     *
     * @param value
     *            The short value to be written
     */
    public void putShort(short value) {
        if (this.buffer.assureBufferCapacity(2)) {
            allocate(DEFAULT_PAGE_SIZE);
        }
        buffer.put((byte) value);
        buffer.put((byte) (value >> 8));
        this.pos += 2;
    }
    
    /**
     * Relative <i>put</i> method for writing an int value (4 bytes).
     * 
     * @param value
     *            the int value to be written.
     */
    public void putInt(int value) {
        if (this.buffer.assureBufferCapacity(4)) {
            allocate(DEFAULT_PAGE_SIZE);
        }
        ByteArray buffer = this.buffer;
        buffer.put((byte) value);
        buffer.put((byte) (value >> 8));
        buffer.put((byte) (value >> 16));
        buffer.put((byte) (value >> 24));
        this.pos += 4;
    }
    
    /**
     * Relative <i>put</i> method for writing an long value (8 bytes).
     * 
     * @param value
     *            the long value to be written.
     */
    public void putLong(long value) {
        if (this.buffer.assureBufferCapacity(8)) {
            allocate(DEFAULT_PAGE_SIZE);
        }
        ByteArray buffer = this.buffer;
        buffer.put((byte) value);
        buffer.put((byte) (value >> 8));
        buffer.put((byte) (value >> 16));
        buffer.put((byte) (value >> 24));
        buffer.put((byte) (value >> 32));
        buffer.put((byte) (value >> 40));
        buffer.put((byte) (value >> 48));
        buffer.put((byte) (value >> 56));
        this.pos += 8;
    }
    
    /**
     * Relative <i>put</i> method for writing a string value (UTF8).
     * 
     * @param value
     *            the string value to be written.
     */
    public void putString(String value) {
        Utf8.write(this, UnsafeString.getChars(value), 0, value.length());
    }
    
    /**
     * Relative <i>put</i> method for writing a stream value (UTF8).
     * 
     * @param is
     *            the InputStream value to be written.
     * @param readLength
     *            the number of bytes to read from the given {@link InputStream}.
     */
    public void putStream(InputStream is, long readLength) throws IOException {
        ByteArray array = this.buffer;
        if (array.pos + readLength < array.buf.length) {
            is.read(array.buf, array.pos, (int) readLength);
            this.pos += (int) readLength;
        } else {
            long remainingReadLength = readLength;
            int read;
            while (remainingReadLength > 0) {
                allocate(DEFAULT_PAGE_SIZE_STREAM);
                read = is.read(this.buffer.buf, 0, Math.min((int) remainingReadLength,
                                                            DEFAULT_PAGE_SIZE_STREAM));
                remainingReadLength -= read;
                this.buffer.pos = read;
                this.pos += read;
            }
        }
    }
    
    private void allocate(int size) {
        this.buffer = new ByteArray(size);
        this.buffers.add(this.buffer);
    }
    
    private static final class ByteArray {
        private byte[] buf;
        private int pos;
        
        private ByteArray(int size) {
            this.buf = new byte[size];
            this.pos = 0;
        }
        
        public ByteArray(byte[] bytes) {
            this.buf = bytes;
            this.pos = bytes.length;
        }
        
        public int remaining() {
            return buf.length - pos;
        }
        
        public void put(byte value) {
            buf[pos++] = value;
        }
        
        public boolean assureBufferCapacity(int size) {
            return (pos + size > buf.length);
        }
        
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("ByteBufferArray {");
            builder.append("buf=[").append(buf).append("], ");
            builder.append("pos=[").append(pos).append("], ");
            builder.append("size=[").append(buf.length).append("]");
            builder.append("}");
            return builder.toString();
        }
    }
    
    /**
     * Ensure that the buffer remaining size permit to write a data with a size len.
     * 
     * @param len
     *            size of the data
     */
    public void assureBufferCapacity(int len) {
        if (buffer.pos + len > buffer.buf.length) {
            allocate(DEFAULT_PAGE_SIZE);
        }
    }
    
}
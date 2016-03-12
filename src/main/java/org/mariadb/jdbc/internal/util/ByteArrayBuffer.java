package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public final class ByteArrayBuffer {
    
    private static final Unsafe UNSAFE = UnsafeUtil.unsafe();
    
    /**
     * Current byte buffer.
     */
    private ByteBuf current;
    
    /**
     * List of all buffers used.
     */
    private final ByteBufList buffers;
    
    /**
     * Memory address for the current.
     */
    private long adr;

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
        this.buffers = new ByteBufList();
        current = this.buffers.first;
        this.limit = 0;
        this.compressRead = 0;
        this.adr = current.address();
    }
    
    public ByteBuf current() {
        return current;
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
        return pos + this.current.pos();
    }
    
    /**
     * recycle this buffer (reset).
     */
    public void recycle() {
        this.limit = 0;
        this.pos = 0;
        this.compressRead = 0;
        this.buffers.recylce();
        this.current = buffers.first;
        this.adr = current.address();
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
        ByteBuf buf;
        int index = off;
        for (int n = this.buffers.size(); compressRead < n; compressRead++) {
            buf = this.buffers.get(compressRead);
            if (buf == null) {
                break;
            }
            if (pos + len > buf.pos()) {
                System.arraycopy(buf.array(), 0, bufferBytes, index, buf.pos());
                index += buf.pos();
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
            ByteBuf byteBuf = this.buffers.get(i);
            if (byteBuf == null) {
                break;
            }
            byteBuf.writeTo(outputStream);
        }
    }
    
    /**
     * prepare this byte array buffer to be written.
     */
    public void prepare() {
        limit = pos + current.pos();
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
        if (this.current.remaining() > len) {
            UNSAFE.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, adr + current.pos(), len);
            current.incPos(len);
        } else {
            if (len > ByteBufArray.DEFAULT_PAGE) {
                this.pos += current.pos();
                this.current = new ByteBufArray(src, off, len);
                this.buffers.add(this.current);
            } else {
                allocate();
                UNSAFE.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, adr + current.pos(), len);
                current.incPos(len);
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
        if (current.pos() >= ByteBufUnsafe.DEFAULT_PAGE) {
            allocate();
        }
        
        UNSAFE.putByte(adr + current.pos(), value);
        current.incPos(1);
    }
    
    /**
     * Relative <i>put</i> method for writing a short value (2 bytes).
     *
     * @param value
     *            The short value to be written
     */
    public void putShort(short value) {
        if (current.remaining() < 2) {
            allocate();
        }
        UNSAFE.putShort(adr + current.pos(), value);
        current.incPos(2);
    }
    
    /**
     * Relative <i>put</i> method for writing an int value (4 bytes).
     * 
     * @param value
     *            the int value to be written.
     */
    public void putInt(int value) {
        if (current.remaining() < 4) {
            allocate();
        }
        UNSAFE.putInt(adr + current.pos(), value);
        current.incPos(4);
        // put(new byte[] { (byte) value, (byte) (value >> 8), (byte) (value >> 16),
        // (byte) (value >> 24) }, 0, 4);
    }
    
    /**
     * Relative <i>put</i> method for writing an long value (8 bytes).
     * 
     * @param value
     *            the long value to be written.
     */
    public void putLong(long value) {
        if (current.remaining() < 8) {
            allocate();
        }
        UNSAFE.putLong(adr + current.pos(), value);
        current.incPos(8);
        // put(new byte[] { (byte) value, (byte) (value >> 8), (byte) (value >> 16),
        // (byte) (value >> 24), (byte) (value >> 32), (byte) (value >> 40),
        // (byte) (value >> 48), (byte) (value >> 56) }, 0, 8);
    }
    
    /**
     * Relative <i>put</i> method for writing an byte value n times (n bytes).
     * 
     * @param value
     *            the long value to be written.
     * @param count
     *            the number of repeat for the value.
     * 
     */
    public void writeBytes(byte value, int count) {
        if (current.remaining() < count) {
            allocate();
        }
        UNSAFE.setMemory(adr + current.pos(), count, value);
        current.incPos(count);
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
        this.pos += this.current.pos();
        this.current = new ByteBufStream(is, (int) readLength);
        this.buffers.add(this.current);
    }
    
    /**
     * allocate a new {@link ByteBufArray}.
     */
    public void allocate() {
        this.pos += this.current.pos();
        this.current = new ByteBufUnsafe();
        this.buffers.add(this.current);
        this.adr = this.current.address();
    }

}
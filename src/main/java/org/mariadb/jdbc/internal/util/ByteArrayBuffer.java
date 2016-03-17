package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public final class ByteArrayBuffer {
    
    private static final Unsafe UNSAFE = UnsafeUtil.unsafe();
    
    private static final ByteBuf[] DEFAULT = new ByteBuf[0];
    
    /**
     * Current byte buffer.
     */
    private ByteBuf current;
    
    private final ByteBufUnsafe first = new ByteBufUnsafe();
    
    private ByteBuf[] elem = DEFAULT;
    
    private int elemIdx = 0;
    
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
        this.current = first;
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
        recylce();
        this.current = this.first;
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
        int index = off;
        if (compressRead == 0) {
            System.arraycopy(first.array(), 0, bufferBytes, index, first.pos());
            index += first.pos();
        }
        for (int n = elemIdx; compressRead < n; compressRead++) {
            if (pos + len > elem[compressRead].pos()) {
                System.arraycopy(elem[compressRead].array(), 0, bufferBytes, index,
                                 elem[compressRead].pos());
                index += elem[compressRead].pos();
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
        first.writeTo(outputStream);
        if (elem != null) {
            for (int i = 0, n = elemIdx; i < n; i++) {
                elem[i].writeTo(outputStream);
            }
        }
    }
    
    int writePartPos = -1;
    
    public void writePartTo(OutputStream outputStream, int length) throws IOException {
        if (writePartPos == -1) {
            first.writeTo(outputStream);
            length -= first.pos();
            writePartPos++;
        }
        while (writePartPos < elemIdx && length > 0) {
           // length = elem[writePartPos++].writePartTo(outputStream, length);
        }
    }
    
    /**
     * prepare this byte array buffer to be written.
     */
    public void prepare() {
        limit = pos + current.pos();
        pos = 0;
    }
    
    
    public void put(ByteBufUnsafe buf) {
        int len = buf.pos();
        
        if (this.current.remaining() > len) {
            UNSAFE.copyMemory(buf.address(), adr + current.pos(len), len);
        } else {
            allocate();
            UNSAFE.copyMemory(buf.address(), adr + current.pos(len), len);
        }
        
        
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
            UNSAFE.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, adr + current.pos(
                                                                                                len),
                              len);
        } else {
            if (len > ByteBufArray.DEFAULT_PAGE) {
                this.pos += current.pos();
                this.current = new ByteBufArray(src, off, len);
                add();
            } else {
                allocate();
                UNSAFE.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, adr + current.pos(
                                                                                                    len),
                                  len);
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
        if (current.remaining() == 0) {
            allocate();
        }
        UNSAFE.putByte(adr + current.pos(1), value);
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
        UNSAFE.putShort(adr + current.pos(2), value);
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
        UNSAFE.putInt(adr + current.pos(4), value);
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
        UNSAFE.putLong(adr + current.pos(8), value);
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
        UNSAFE.setMemory(adr + current.pos(count), count, value);
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
        add();
    }
    
    /**
     * allocate a new {@link ByteBufArray}.
     */
    public void allocate() {
        this.pos += this.current.pos();
        this.current = new ByteBufUnsafe(elemIdx);
        add();
        this.adr = this.current.address();
    }
    
    private void recylce() {
        first.recycle();
        
        if (elem != DEFAULT) {
            for (int i = 0; i < elemIdx; i++) {
                elem[i].free();
            }
            elemIdx = 0;
            elem = DEFAULT;
        }
        
    }
    
    private void add() {
        if (elem == DEFAULT) {
            elem = new ByteBuf[8];
        } else if (elemIdx == elem.length) {
            ByteBuf[] tmp = new ByteBuf[elem.length * 2];
            System.arraycopy(elem, 0, tmp, 0, elem.length);
            elem = tmp;
        }
        elem[elemIdx++] = this.current;
    }
    
    /**
     * Close this byte array buffer (free first ByteByfUnsafe, ...).
     */
    public void close() {
        try {
            first.free();
        } finally {
            if (elem != DEFAULT) {
                for (int i = 0; i < elemIdx; i++) {
                    elem[i].free();
                }
                elemIdx = 0;
                elem = DEFAULT;
            } 
        }
    }
    
}
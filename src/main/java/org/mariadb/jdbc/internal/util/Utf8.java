package org.mariadb.jdbc.internal.util;

import sun.misc.Unsafe;

@SuppressWarnings("all")
public final class Utf8 {
    
    private static final Unsafe UNSAFE = UnsafeUtil.unsafe();
    
    private Utf8() {
    }
    
    /**
     * convert the char array to byte representation.
     * 
     * @param buffer
     *            the byte array buffer to write utf-8 chars.
     * @param buf
     *            the source char array.
     * @param off
     *            the offset in the char array.
     * @param len
     *            the number of chars to write.
     */
    public static void write(ByteArrayBuffer buffer, char[] buf, int off, int len) {
        int cp;
        for (int i = off, max = off + len; i < max; i++) {
            cp = buf[i];
            // ascii
            if (cp < 0x80) {
                buffer.put((byte) cp);
            } else {
                // 2-byte
                if (cp < 0x800) {
                    buffer.put((byte) (0xc0 | (cp >> 6)));
                    buffer.put((byte) (0x80 | (cp & 0x3f)));
                    // 3 bytes
                } else if (cp <= 0xFFFF) {
                    buffer.put((byte) (0xe0 | (cp >> 12)));
                    buffer.put((byte) (0x80 | ((cp >> 6) & 0x3f)));
                    buffer.put((byte) (0x80 | (cp & 0x3f)));
                } else {
                    // 4 bytes
                    if (cp > 0x10FFFF) {
                        // illegal, as per RFC 3629
                        throw new IllegalStateException();
                    }
                    buffer.put((byte) (0xf0 | (cp >> 18)));
                    buffer.put((byte) (0x80 | ((cp >> 12) & 0x3f)));
                    buffer.put((byte) (0x80 | ((cp >> 6) & 0x3f)));
                    buffer.put((byte) (0x80 | (cp & 0x3f)));
                }
            }
        }
    }
    
    /**
     * convert the char array to byte representation (Use UNSAFE with address to put directly byte).
     * 
     * @param buffer
     *            the byte array buffer to write utf-8 chars.
     * @param buf
     *            the source char array.
     * @param off
     *            the offset in the char array.
     * @param len
     *            the number of chars to write.
     */
    public static void write2(ByteArrayBuffer buffer, char[] buf, int off, int len) {
        ByteBuf byteBuf = buffer.current();
        if (ByteBufUnsafe.DEFAULT_PAGE - byteBuf.pos() > 3 * len) {
            long adr = byteBuf.address();
            int pos = byteBuf.pos();
            int cp;
            for (int i = off, max = off + len; i < max; i++) {
                cp = buf[i];
                // ascii
                if (cp < 0x80) {
                    UNSAFE.putByte(adr + pos++, (byte) cp);
                } else {
                    // 2-byte
                    if (cp < 0x800) {
                        UNSAFE.putByte(adr + pos++, (byte) (0xc0 | (cp >> 6)));
                        UNSAFE.putByte(adr + pos++, (byte) (0x80 | (cp & 0x3f)));
                        // 3 bytes
                    } else if (cp <= 0xFFFF) {
                        UNSAFE.putByte(adr + pos++, (byte) (0xe0 | (cp >> 12)));
                        UNSAFE.putByte(adr + pos++, (byte) (0x80 | ((cp >> 6) & 0x3f)));
                        UNSAFE.putByte(adr + pos++, (byte) (0x80 | (cp & 0x3f)));
                    } else {
                        // 4 bytes
                        if (cp > 0x10FFFF) {
                            // illegal, as per RFC 3629
                            throw new IllegalStateException();
                        }
                        UNSAFE.putByte(adr + pos++, (byte) (0xf0 | (cp >> 18)));
                        UNSAFE.putByte(adr + pos++, (byte) (0x80 | ((cp >> 12) & 0x3f)));
                        UNSAFE.putByte(adr + pos++, (byte) (0x80 | ((cp >> 6) & 0x3f)));
                        UNSAFE.putByte(adr + pos++, (byte) (0x80 | (cp & 0x3f)));
                    }
                }
            }
            byteBuf.incPos(pos - byteBuf.pos());
        } else {
            write(buffer, buf, off, len);
        }
        
    }
    
}
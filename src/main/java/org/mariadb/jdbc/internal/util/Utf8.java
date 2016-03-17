package org.mariadb.jdbc.internal.util;

import sun.misc.Unsafe;
import sun.nio.cs.Surrogate;
import sun.nio.cs.Surrogate.Parser;

@SuppressWarnings("all")
public final class Utf8 {
    
    private static final Surrogate.Parser SGP = new Surrogate.Parser();
    
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
    
    public static void write(ByteBuf byteBuf, char[] buf, int off, int len) {
        int sl = off + len;
        int dp = 0;
        int dlASCII = len;
        long pos = byteBuf.pos() + byteBuf.address();
        
        // ASCII only optimized loop
        while (dp < dlASCII && buf[off] < '\u0080') {
            UNSAFE.putByte(pos++, (byte) buf[off++]);
            dp++;
        }
        
        while (off < sl) {
            char c = buf[off++];
            if (c < 0x80) {
                // 1 byte, 7 bits: 0xxxxxxx
                UNSAFE.putByte(pos++, (byte) c);
            } else if (c < 0x800) {
                // 2 bytes, 11 bits: 110xxxxx 10xxxxxx
                UNSAFE.putByte(pos++, (byte) (0xc0 | (c >> 6)));
                UNSAFE.putByte(pos++, (byte) (0x80 | (c & 0x3f)));
            } else if (Character.isSurrogate(c)) {
                int uc = SGP.parse(c, buf, off - 1, sl);
                if (uc < 0) {
                    UNSAFE.putByte(pos++, (byte) "\ufffd".charAt(0));
                    // da[dp++] = "\ufffd"; //$NON-NLS-1$
                } else {
                    UNSAFE.putByte(pos++, (byte) (0xf0 | (c >> 18)));
                    UNSAFE.putByte(pos++, (byte) (0x80 | ((c >> 12) & 0x3f)));
                    UNSAFE.putByte(pos++, (byte) (0x80 | ((c >> 6) & 0x3f)));
                    UNSAFE.putByte(pos++, (byte) (0x80 | (c & 0x3f)));
                    off++; // 2 chars
                }
            } else {
                // 3 bytes, 16 bits
                UNSAFE.putByte(pos++, (byte) (0xe0 | (c >> 12)));
                UNSAFE.putByte(pos++, (byte) (0x80 | ((c >> 6) & 0x3f)));
                UNSAFE.putByte(pos++, (byte) (0x80 | (c & 0x3f)));
            }
        }
        byteBuf.pos(pos);
        
    }
    
    public static void write2(ByteArrayBuffer buffer, char[] buf, int off, int len) {
        ByteBuf byteBuf = buffer.current();
        if (byteBuf.remaining() < 3 * len) {
            write(buffer, buf, off, len);
            return;
        }
        int sl = off + len;
        int dp = 0;
        int dlASCII = len;
        long pos = byteBuf.pos() + byteBuf.address();
        
        // ASCII only optimized loop
        while (dp < dlASCII && buf[off] < '\u0080') {
            UNSAFE.putByte(pos++, (byte) buf[off++]);
            dp++;
        }
        
        while (off < sl) {
            char c = buf[off++];
            if (c < 0x80) {
                // 1 byte, 7 bits: 0xxxxxxx
                UNSAFE.putByte(pos++, (byte) c);
            } else if (c < 0x800) {
                // 2 bytes, 11 bits: 110xxxxx 10xxxxxx
                UNSAFE.putByte(pos++, (byte) (0xc0 | (c >> 6)));
                UNSAFE.putByte(pos++, (byte) (0x80 | (c & 0x3f)));
            } else if (Character.isSurrogate(c)) {
                int uc = SGP.parse(c, buf, off - 1, sl);
                if (uc < 0) {
                    UNSAFE.putByte(pos++, (byte) "\ufffd".charAt(0));
                    // da[dp++] = "\ufffd"; //$NON-NLS-1$
                } else {
                    UNSAFE.putByte(pos++, (byte) (0xf0 | (c >> 18)));
                    UNSAFE.putByte(pos++, (byte) (0x80 | ((c >> 12) & 0x3f)));
                    UNSAFE.putByte(pos++, (byte) (0x80 | ((c >> 6) & 0x3f)));
                    UNSAFE.putByte(pos++, (byte) (0x80 | (c & 0x3f)));
                    off++; // 2 chars
                }
            } else {
                // 3 bytes, 16 bits
                UNSAFE.putByte(pos++, (byte) (0xe0 | (c >> 12)));
                UNSAFE.putByte(pos++, (byte) (0x80 | ((c >> 6) & 0x3f)));
                UNSAFE.putByte(pos++, (byte) (0x80 | (c & 0x3f)));
            }
        }
        byteBuf.pos(pos);
    }
    
}

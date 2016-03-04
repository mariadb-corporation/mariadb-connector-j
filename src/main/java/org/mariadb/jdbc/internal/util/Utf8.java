package org.mariadb.jdbc.internal.util;

public final class Utf8 {
    
    private Utf8() {
    }
    
    /**
     * convert the char array to byte representation.
     * 
     * @param buffer the byte array buffer to write utf-8 chars.
     * @param buf the source char array.
     * @param off the offset in the char array.
     * @param len the number of chars to write.
     */
    public static void write(ByteArrayBuffer buffer, char[] buf, int off, int len) {
        int cp;
        for (int i = off; i < len; i++) {
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
}

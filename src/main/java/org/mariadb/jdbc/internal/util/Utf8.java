package org.mariadb.jdbc.internal.util;

public final class Utf8 {

	private Utf8(){
	}

	public static void write(ByteBuffer buffer, char[] buf, int off, int len) {
		int c;
		for (int i = off; i < len; i++) {
			c = buf[i];
			// ascii
			if (c < 0x80) {
				buffer.put((byte) c);
			} else {
				// 2-byte
				if (c < 0x800) {
					buffer.put((byte) (0xc0 | (c >> 6)));
					buffer.put((byte) (0x80 | (c & 0x3f)));
					// 3 bytes
				} else if (c <= 0xFFFF) {
					buffer.put((byte) (0xe0 | (c >> 12)));
					buffer.put((byte) (0x80 | ((c >> 6) & 0x3f)));
					buffer.put((byte) (0x80 | (c & 0x3f)));
				} else {
					// 4 bytes
					if (c > 0x10FFFF) {
						// illegal, as per RFC 3629
						throw new IllegalStateException();
					}
					buffer.put((byte) (0xf0 | (c >> 18)));
					buffer.put((byte) (0x80 | ((c >> 12) & 0x3f)));
					buffer.put((byte) (0x80 | ((c >> 6) & 0x3f)));
					buffer.put((byte) (0x80 | (c & 0x3f)));
				}
			}
		}
	}
}

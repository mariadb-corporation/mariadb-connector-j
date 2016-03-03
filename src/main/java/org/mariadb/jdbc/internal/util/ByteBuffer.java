package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public final class ByteBuffer {

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
	private ByteBufferArray buffer;

	/**
	 * All byte buffer arrays.
	 */
	private final List<ByteBufferArray> buffers;

	/**
	 * Position overall buffers.
	 */
	private int pos;
	
	private int limit;
	
	private int compressRead;
	
	public ByteBuffer() {
		this.buffer = new ByteBufferArray(DEFAULT_PAGE_SIZE);
		this.buffers = new ArrayList<ByteBufferArray>(16);
		this.buffers.add(this.buffer);
		this.limit = 0;
		this.compressRead = 0;
	}

	public int remaining() {
		return this.limit - this.pos;
	}

	public int position() {
		return pos;
	}

	public void recycle() {
		this.limit = 0;
		this.pos = 0;
		this.compressRead = 0;
		this.buffer = this.buffers.get(0);
		this.buffer.pos = 0;
		this.buffers.clear();
		this.buffers.add(this.buffer);
	}

	public int get(byte[] bufferBytes, int indexHeader, int off, int len) {
		ByteBufferArray array;
		int j = off;
		for (int n = this.buffers.size(); compressRead < n ; compressRead++) {
			array = this.buffers.get(compressRead);
			if (array == null) {
				break;
			} 
			if (pos+len > array.pos) {
				System.arraycopy(array.buf, 0, bufferBytes, j, array.pos);
				j+=array.pos;
			} else {
				break;
			}
		}
		return j-off;
	}

	
	public void writeTo(OutputStream outputStream) throws IOException {
		for (int i = 0 , n = this.buffers.size(); i < n ; i++) {
			ByteBufferArray array = this.buffers.get(i);
			if (array == null) {
				break;
			}
			outputStream.write(array.buf, 0 , array.pos);
		}
	}

	public void prepare() {
		limit = 0;
		for (int i = 0 , n = this.buffers.size(); i < n ; i++) {
			if (this.buffers.get(i) == null) {
				break;
			}
			limit += this.buffers.get(i).pos;
		}
		pos = 0;
	}

	
	// -------------------------------------------------------------------------------------------
	public void put(byte[] bytes, int i, int len) {
		ByteBufferArray array = this.buffer;
		if (array.remaining() > len) {
			System.arraycopy(bytes, i, array.buf, array.pos, len);
			array.pos += len;
			this.pos += len;
		} else {
			if (i == 0 && len == bytes.length) {
				this.buffer = new ByteBufferArray(bytes);
				this.buffers.add(this.buffer);
				allocate(DEFAULT_PAGE_SIZE);
				this.pos += len;
			} else {
				if (len > DEFAULT_PAGE_SIZE) {
					array  = new ByteBufferArray(len);	
				} else {
					array = new ByteBufferArray(DEFAULT_PAGE_SIZE);
				}
				System.arraycopy(bytes, 0, array.buf, 0, len);
				array.pos = len;
				this.pos += len;
			}
		}
	}
	
	public void put(byte[] bytes) {
		if (bytes == null || bytes.length ==0) {
			return;
		}
		ByteBufferArray array = this.buffer;
		if (array.remaining() > bytes.length) {
			System.arraycopy(bytes, 0, array.buf, array.pos, bytes.length);
			array.pos += bytes.length;
			this.pos += bytes.length;
		} else {
			this.buffer = new ByteBufferArray(bytes);
			this.buffers.add(this.buffer);
			//allocate(DEFAULT_PAGE_SIZE);
			this.pos += bytes.length;
		}
	}

	public void putShort(short x) {
		buffer.buf[buffer.pos++] = (byte) x;
		buffer.buf[buffer.pos++] = (byte) (x >> 8);
		this.pos += 2;
	}

	public void putInt(int x) {
		if (this.buffer.pos + 4 > this.buffer.buf.length) {
			allocate(DEFAULT_PAGE_SIZE);
		}
		ByteBufferArray buffer = this.buffer;
		buffer.buf[buffer.pos++] = (byte) x;
		buffer.buf[buffer.pos++] = (byte) (x >> 8);
		buffer.buf[buffer.pos++] = (byte) (x >> 16);
		buffer.buf[buffer.pos++] = (byte) (x >> 24);
		this.pos += 4;
	}


	public void putLong(long x) {
		if (this.buffer.pos + 4 > this.buffer.buf.length) {
			allocate(DEFAULT_PAGE_SIZE);
		}
		ByteBufferArray buffer = this.buffer;
		buffer.buf[buffer.pos++] = (byte) x;
		buffer.buf[buffer.pos++] = (byte) (x >> 8);
		buffer.buf[buffer.pos++] = (byte) (x >> 16);
		buffer.buf[buffer.pos++] = (byte) (x >> 24);
		buffer.buf[buffer.pos++] = (byte) (x >> 32);
		buffer.buf[buffer.pos++] = (byte) (x >> 40);
		buffer.buf[buffer.pos++] = (byte) (x >> 48);
		buffer.buf[buffer.pos++] = (byte) (x >> 56);
		this.pos += 8;
	}

	public void put(byte x) {
		if (this.buffer.pos + 1 > this.buffer.buf.length) {
			allocate(DEFAULT_PAGE_SIZE);
		}
		this.buffer.buf[this.buffer.pos++] =  x;	
		this.pos += 1;
	}
	
	public void putString(String str) {
		Utf8.write(this, UnsafeString.getChars(str), 0 , str.length());
	}

	public void putBytes(byte theByte, int count) {
		if (this.buffer.pos + count > this.buffer.buf.length) {
			allocate(DEFAULT_PAGE_SIZE);
		}
		ByteBufferArray buffer = this.buffer;
		for (int i = 0; i < count; i++) {
			buffer.buf[buffer.pos++] = theByte;
        }
		this.pos += count;
	}
	
	public void putStream(InputStream is, long readLength) throws IOException {
		ByteBufferArray array = this.buffer;
		if (array.pos + readLength < array.buf.length) {
			is.read(array.buf, array.pos, (int)readLength);
			this.pos += (int)readLength;
		} else {
			long remainingReadLength = readLength;
			int read;
			while (remainingReadLength > 0) {
				allocate(DEFAULT_PAGE_SIZE_STREAM); 
				read = is.read(this.buffer.buf, 0, Math.min((int)remainingReadLength, DEFAULT_PAGE_SIZE_STREAM));
				remainingReadLength -= read;
				this.buffer.pos = read;
				this.pos += read;
			}
		}
	}

	private void allocate(int size) {
		this.buffer = new ByteBufferArray(size);
		this.buffers.add(this.buffer);
	}
	
	private static final class ByteBufferArray {
		private byte[] buf;
		private int pos;

		private ByteBufferArray(int size) {
			this.buf = new byte[size];
			this.pos = 0;
		}

		public ByteBufferArray(byte[] bytes) {
			this.buf = bytes;
			this.pos = bytes.length;
		}
		
		public int remaining() {
			return buf.length - pos;
		}

		/**
		 * {@inheritDoc}
		 */
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

	public void assureBufferCapacity(int size) {
		if (buffer.pos + size > buffer.buf.length) {			
			allocate(DEFAULT_PAGE_SIZE);
		}
	}


}
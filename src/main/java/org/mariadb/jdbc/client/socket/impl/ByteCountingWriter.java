package org.mariadb.jdbc.client.socket.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.socket.Writer;

/** A Writer implementation that just counts the number of bytes written */
public class ByteCountingWriter implements Writer {

	private static final byte QUOTE = (byte) '\'';
	private static final byte ZERO_BYTE = (byte) '\0';
	private static final byte BACKSLASH = (byte) '\\';

	int count = 0;
	
	public int getByteCount() {
		return count;
	}
	
	public void resetCount() {
		count = 0;
	}
	
	@Override
	public int pos() {
		return 0;
	}

	@Override
	public byte[] buf() {
		return null;
	}

	@Override
	public void pos(int pos) throws IOException {
	}

	@Override
	public void writeByte(int value) throws IOException {
		count++;
	}

	@Override
	public void writeShort(short value) throws IOException {
		count += 2;
	}

	@Override
	public void writeInt(int value) throws IOException {
		count += 4;
	}

	@Override
	public void writeLong(long value) throws IOException {
		count += 8;
	}

	@Override
	public void writeDouble(double value) throws IOException {
		count += 8;
	}

	@Override
	public void writeFloat(float value) throws IOException {
		count += 4;
	}

	@Override
	public void writeBytes(byte[] arr) throws IOException {
		count += arr.length;
	}

	@Override
	public void writeBytesAtPos(byte[] arr, int pos) {
		// writes to buffer but does not change position
	}

	@Override
	public void writeBytes(byte[] arr, int off, int len) throws IOException {
		count += len;
	}

	@Override
	public void writeLength(long length) throws IOException {
		// variable length
		if (length < 251) {
	      count++;
	      return;
	    }
	    if (length < 65536) {
	      count += 3;
	      return;
	    }
	    if (length < 16777216) {
	      count += 4;
	      return;
	    }
	    count += 9;
	}

	@Override
	public void writeAscii(String str) throws IOException {
		count += str.length();
	}

	@Override
	public void writeString(String str) throws IOException {
		count += str.getBytes(StandardCharsets.UTF_8).length;
	}

	@Override
	public void writeStringEscaped(String str, boolean noBackslashEscapes) throws IOException {
		byte[] arr = str.getBytes(StandardCharsets.UTF_8);
	    writeBytesEscaped(arr, arr.length, noBackslashEscapes);
	}

	@Override
	public void writeBytesEscaped(byte[] bytes, int len, boolean noBackslashEscapes) throws IOException {
	    if (noBackslashEscapes) {
	      for (int i = 0; i < len; i++) {
	        if (QUOTE == bytes[i]) {
	          count++; // QUOTE;
	        }
	        count++; // bytes[i];
	      }
	    } else {
	      for (int i = 0; i < len; i++) {
	        if (bytes[i] == QUOTE
	            || bytes[i] == BACKSLASH
	            || bytes[i] == '"'
	            || bytes[i] == ZERO_BYTE) {
	          count++; // BACKSLASH;
	        }
	        count++; // bytes[i];
	      }
	    }

	}

	@Override
	public void writeEmptyPacket() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void flush() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void flushPipeline() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean throwMaxAllowedLength(int length) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getCmdLength() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void permitTrace(boolean permitTrace) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setServerThreadId(Long serverThreadId, HostAddress hostAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void mark() {
		throw new UnsupportedOperationException();

	}

	@Override
	public boolean isMarked() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasFlushed() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void flushBufferStopAtMark() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean bufIsDataAfterMark() {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] resetMark() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void initPacket() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte getSequence() {
		// TODO Auto-generated method stub
		return 0;
	}

}

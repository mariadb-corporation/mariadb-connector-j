package org.mariadb.jdbc.internal.stream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.zip.DeflaterOutputStream;

import org.mariadb.jdbc.internal.util.ByteBuffer;

public class PacketOutputStream extends OutputStream {
	// private final static Logger log =
	// LoggerFactory.getLogger(PacketOutputStream.class);
	private static final int MIN_COMPRESSION_SIZE = 16 * 1024;
	private static final float MIN_COMPRESSION_RATIO = 0.9f;
	private static final int MAX_PACKET_LENGTH = 0x00ffffff;
	private static final int HEADER_LENGTH = 4;

	public ByteBuffer buffer;
	int seqNo;
	int lastSeq;
	int maxAllowedPacket;
	int maxPacketSize = MAX_PACKET_LENGTH;
	boolean checkPacketLength;
	int maxRewritableLengthAllowed;
	boolean useCompression;

	private final OutputStream outputStream;
	private volatile boolean closed = false;

	/**
	 * Initialization with server outputStream.
	 * 
	 * @param outputStream
	 *            server outPutStream
	 */
	public PacketOutputStream(OutputStream outputStream) {
		this.outputStream = outputStream;
		this.buffer = new ByteBuffer();
		this.seqNo = -1;
		useCompression = false;
	}

	public void setUseCompression(boolean useCompression) {
		this.useCompression = useCompression;
	}

	/**
	 * Initialize stream sequence. Max stream allowed size will be checked.
	 * 
	 * @param seqNo
	 *            stream sequence number
	 * @param checkPacketLength
	 *            indication that max stream allowed size will be checked.
	 * @throws IOException
	 *             if any error occur during data send to server
	 */
	public void startPacket(int seqNo, boolean checkPacketLength) throws IOException {
		if (closed) {
			throw new IOException("Stream has already closed");
		}
		if (this.seqNo != -1) {
			throw new IOException("Last stream not finished");
		}
		this.seqNo = seqNo;
		this.checkPacketLength = checkPacketLength;
	}

	/**
	 * Initialize stream sequence.
	 * 
	 * @param seqNo
	 *            stream sequence number
	 * @throws IOException
	 *             if any error occur during data send to server
	 */
	public void startPacket(int seqNo) throws IOException {
		startPacket(seqNo, true);
	}

	/**
	 * Send an empty stream to server.
	 * 
	 * @param seqNo
	 *            stream sequence number
	 * @throws IOException
	 *             if any error occur during data send to server
	 */
	public void writeEmptyPacket(int seqNo) throws IOException {
		byte[] buf = new byte[4];
		buf[0] = ((byte) 0);
		buf[1] = ((byte) 0);
		buf[2] = ((byte) 0);
		buf[3] = ((byte) seqNo);
		outputStream.write(buf, 0, 4);
	}

	/**
	 * Used to send LOAD DATA INFILE. End of data is indicated by stream of
	 * length 0.
	 * 
	 * @param is
	 *            inputStream to send
	 * @param seq
	 *            stream sequence number
	 * @throws IOException
	 *             if any error occur during data send to server
	 */
	public void sendFile(InputStream is, int seq) throws IOException {
		this.seqNo = seq;
		buffer.recycle();
		this.checkPacketLength = false;
		byte[] buffer = new byte[8192];
		int len;
		while ((len = is.read(buffer)) > 0) {
			write(buffer, 0, len);
		}
		finishPacket();
		writeEmptyPacket(lastSeq);
	}

	/**
	 * Send stream to server.
	 * 
	 * @param is
	 *            inputStream to send
	 * @throws IOException
	 *             if any error occur during data send to server
	 */
	public void sendStream(InputStream is) throws IOException {
		byte[] buffer = new byte[8192];
		int len;
		while ((len = is.read(buffer)) > 0) {
			write(buffer, 0, len);
		}
	}

	/**
	 * Send stream to server.
	 * 
	 * @param is
	 *            inputStream to send
	 * @param readLength
	 *            max size to send
	 * @throws IOException
	 *             if any error occur during data send to server
	 */
	public void sendStream(InputStream is, long readLength) throws IOException {
		this.buffer.putStream(is, readLength);
	}

	/**
	 * Send reader stream to server.
	 * 
	 * @param reader
	 *            reader to send
	 * @throws IOException
	 *             if any error occur during data send to server
	 */
	public void sendStream(Reader reader) throws IOException {
		char[] buffer = new char[8192];
		int len;
		while ((len = reader.read(buffer)) > 0) {
			byte[] bytes = new String(buffer, 0, len).getBytes("UTF-8");
			write(bytes, 0, bytes.length);
		}
	}

	/**
	 * Send reader stream to server.
	 * 
	 * @param reader
	 *            reader to send
	 * @param readLength
	 *            max size to send
	 * @throws IOException
	 *             if any error occur during data send to server
	 */
	public void sendStream(Reader reader, long readLength) throws IOException {
		char[] buffer = new char[8192];
		long remainingReadLength = readLength;
		int read;
		while (remainingReadLength > 0) {
			read = reader.read(buffer, 0, Math.min((int) remainingReadLength, 8192));
			if (read == -1) {
				return;
			}
			byte[] bytes = new String(buffer, 0, read).getBytes("UTF-8");
			write(bytes, 0, bytes.length);
			remainingReadLength -= read;
		}
	}

	/**
	 * Ending command that tell to send buffer to server.
	 * 
	 * @throws IOException
	 *             if any connection error occur
	 */
	public void finishPacket() throws IOException {
		/*
		 * if (this.seqNo == -1) { throw new AssertionError("Packet not started"
		 * ); }
		 */
		internalFlush();
		this.buffer.recycle();
		this.lastSeq = this.seqNo;
		this.seqNo = -1;
	}

	@Override
	public void write(byte[] bytes) throws IOException {
		write(bytes, 0, bytes.length);
	}

	@Override
	public void write(int byteInt) throws IOException {
		this.buffer.put((byte) byteInt);
	}

	@Override
	public void write(byte[] bytes, int off, int len) throws IOException {
		if (this.seqNo == -1) {
			throw new AssertionError("Use PacketOutputStream.startPacket() before write()");
		}
		buffer.put(bytes, off, len);
	}

	@Override
	public void flush() throws IOException {
		throw new AssertionError("Do not call flush() on PacketOutputStream. use finishPacket() instead.");
	}

	/**
	 * Check that current buffer + length will not be superior to
	 * max_allowed_packet + header size. That permit to separate rewritable
	 * queries to be separate in multiple stream.
	 * 
	 * @param length
	 *            additionnal length
	 * @return true if with this additional length stream can be send in the
	 *         same stream
	 */
	public boolean checkRewritableLength(int length) {
		if (checkPacketLength && buffer.position() + length > maxRewritableLengthAllowed) {
			return false;
		}
		return true;
	}

	private void checkPacketMaxSize(int limit) throws MaxAllowedPacketException {
		if (checkPacketLength && maxAllowedPacket > 0 && limit > (maxAllowedPacket - 1)) {
			this.seqNo = -1;
			throw new MaxAllowedPacketException("max_allowed_packet exceeded. stream size " + limit
					+ " is > to max_allowed_packet = " + (maxAllowedPacket - 1), this.seqNo != 0);
		}
	}

	private void internalFlush() throws IOException {
		buffer.prepare();
		int limit = buffer.remaining();
		if (limit > 0) {
			checkPacketMaxSize(limit);
			if (useCompression) {
				flushWithCompression(limit);
			} else {
				flushRaw(limit);
			}
		}
	}

	private void flushWithCompression(int limit) throws IOException {
		int notCompressPosition = 0;
		int expectedPacketSize = limit + HEADER_LENGTH * ((limit / maxPacketSize) + 1);
		byte[] bufferBytes = new byte[expectedPacketSize];
		
		while (notCompressPosition < expectedPacketSize) {
			int length = buffer.remaining();
			if (length > maxPacketSize) {
				length = maxPacketSize;
			}
			
			int indexHeader = notCompressPosition;
			bufferBytes[notCompressPosition++] = 0x00;
			bufferBytes[notCompressPosition++] = 0x00;
			bufferBytes[notCompressPosition++] = 0x00;
			bufferBytes[notCompressPosition++] = 0x00;
			
			if (length > 0) {
				int read = buffer.get(bufferBytes, indexHeader, notCompressPosition, length);
				notCompressPosition += read;
				bufferBytes[indexHeader++] = (byte) (read & 0xff);
				bufferBytes[indexHeader++] = (byte) (read >>> 8);
				bufferBytes[indexHeader++] = (byte) (read >>> 16);
				bufferBytes[indexHeader++] = (byte) seqNo++;

			}
		}

		// now bufferBytes in filled with uncompressed data
		compressedAndSend(notCompressPosition, bufferBytes);
	}

	private void flushRaw(int limit) throws IOException {
		int expectedPacketSize = limit + HEADER_LENGTH * ((limit / maxPacketSize) + 1);
		int notCompressPosition = 0;

		while (notCompressPosition < expectedPacketSize) {
			int length = buffer.remaining();
			if (length > maxPacketSize) {
				length = maxPacketSize;
			}
			byte[] header = new byte[HEADER_LENGTH];
			header[0] = (byte) (length & 0xff);
			header[1] = (byte) (length >>> 8);
			header[2] = (byte) (length >>> 16);
			header[3] = (byte) seqNo++;
			outputStream.write(header, 0, HEADER_LENGTH);
			notCompressPosition += 4;

			if (length > 0) {
				this.buffer.writeTo(this.outputStream);
				this.outputStream.flush();
				notCompressPosition += length;
			}
		}

	}

	/**
	 * Compress datas and send them to database.
	 * 
	 * @param notCompressPosition
	 *            notCompressPosition
	 * @param bufferBytes
	 *            not compressed data buffer
	 * @throws IOException
	 *             if any compression or connection error occur
	 */
	private void compressedAndSend(int notCompressPosition, byte[] bufferBytes) throws IOException {
		this.seqNo = 0;
		int position = 0;
		int packetLength;

		while (position - notCompressPosition < 0) {
			packetLength = Math.min(notCompressPosition - position, maxPacketSize);
			boolean compressedPacketSend = false;

			if (packetLength > MIN_COMPRESSION_SIZE) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DeflaterOutputStream deflater = new DeflaterOutputStream(baos);

				deflater.write(bufferBytes, position, packetLength);
				deflater.finish();
				deflater.close();

				byte[] compressedBytes = baos.toByteArray();
				baos.close();

				if (compressedBytes.length < (int) (MIN_COMPRESSION_RATIO * packetLength)) {

					int compressedLength = compressedBytes.length;
					writeCompressedHeader(compressedLength, packetLength, outputStream);
					outputStream.write(compressedBytes, 0, compressedLength);
					compressedPacketSend = true;
				}
			}

			if (!compressedPacketSend) {
				writeCompressedHeader(packetLength, 0, outputStream);
				outputStream.write(bufferBytes, position, packetLength);
			}

			position += packetLength;
			outputStream.flush();
		}
	}

	private void writeCompressedHeader(int packetLength, int initialLength, OutputStream outputStream)
			throws IOException {
		byte[] header = new byte[7];
		header[0] = (byte) (packetLength & 0xff);
		header[1] = (byte) ((packetLength >> 8) & 0xff);
		header[2] = (byte) ((packetLength >> 16) & 0xff);
		header[3] = (byte) seqNo++;
		header[4] = (byte) (initialLength & 0xff);
		header[5] = (byte) ((initialLength >> 8) & 0xff);
		header[6] = (byte) ((initialLength >> 16) & 0xff);
		outputStream.write(header);
	}

	@Override
	public void close() throws IOException {
		outputStream.close();
		buffer = null;
		closed = true;
	}

	/**
	 * Initialize maximal send size (can be send in multiple stream).
	 * 
	 * @param maxAllowedPacket
	 *            value of server maxAllowedPacket
	 */
	public void setMaxAllowedPacket(int maxAllowedPacket) {
		this.maxAllowedPacket = maxAllowedPacket;
		if (maxAllowedPacket > 0) {
			maxPacketSize = Math.min(maxAllowedPacket - 1, MAX_PACKET_LENGTH);
			maxRewritableLengthAllowed = (int) (maxAllowedPacket
					- 4 * Math.ceil(((double) maxAllowedPacket) / maxPacketSize));
		} else {
			maxPacketSize = MAX_PACKET_LENGTH;
		}
	}

	// /**
	// * Ensure that the buffer remaining size permit to write a data with a
	// size len.
	// * @param len size of the data
	// * @return this
	// */
	// public PacketOutputStream assureBufferCapacity(final int len) {
	// while (len > buffer.remaining()) {
	//
	// throw new RuntimeException();
	// // int newCapacity = Math.max(len + buffer.position(), (int)
	// (buffer.capacity() * increasing));
	// // increase(newCapacity);
	// }
	// return this;
	// }

	/**
	 * Write a byte data to buffer.
	 * 
	 * @param theByte
	 *            byte to write
	 * @return this
	 */
	public PacketOutputStream writeByte(byte theByte) {
		buffer.put(theByte);
		return this;
	}

	/**
	 * Write count time the byte value.
	 * 
	 * @param theByte
	 *            byte to write to buffer
	 * @param count
	 *            number of time the value will be put to buffer
	 * @return this
	 */
	public PacketOutputStream writeBytes(final byte theByte, final int count) {
		this.buffer.putBytes(theByte, count);
		return this;
	}

	/**
	 * Write byte array to buffer.
	 * 
	 * @param bytes
	 *            byte array
	 * @return this.
	 */
	public PacketOutputStream writeByteArray(final byte[] bytes) {
		buffer.put(bytes);
		return this;
	}

	/**
	 * Write byte array data to binary data.
	 * 
	 * @param bytes
	 *            byte array to encode
	 * @return this.
	 */
	public PacketOutputStream writeByteArrayLength(final byte[] bytes) {
		writeFieldLength(bytes.length);
		buffer.put(bytes);
		return this;
	}

	/**
	 * Write string data in binary format.
	 * 
	 * @param str
	 *            string value to encode
	 * @return this.
	 */
	public PacketOutputStream writeString(String str) {
		this.buffer.putString(str);
		return this;
	}

	/**
	 * Write short data in binary format.
	 * 
	 * @param theShort
	 *            short data to encode
	 * @return this
	 */
	public PacketOutputStream writeShort(final short theShort) {
		buffer.putShort(theShort);
		return this;
	}

	/**
	 * Write int data in binary format.
	 * 
	 * @param theInt
	 *            int data
	 * @return this.
	 */
	public PacketOutputStream writeInt(final int theInt) {
		// assureBufferCapacity(4);
		buffer.putInt(theInt);
		return this;
	}

	/**
	 * Write long data in binary format.
	 * 
	 * @param theLong
	 *            long data
	 * @return this
	 */
	public PacketOutputStream writeLong(final long theLong) {
		// assureBufferCapacity(8);
		buffer.putLong(theLong);
		return this;
	}

	/**
	 * Write field length to encode in binary format.
	 * 
	 * @param length
	 *            data length to encode
	 * @return this.
	 */
	public PacketOutputStream writeFieldLength(long length) {
		if (length < 251) {
			buffer.put((byte) length);
		} else if (length < 65536) {
			// assureBufferCapacity(3);
			buffer.put((byte) 0xfc);
			buffer.putShort((short) length);
		} else if (length < 16777216) {
			// assureBufferCapacity(4);
			buffer.put((byte) 0xfd);
			buffer.put((byte) (length & 0xff));
			buffer.put((byte) (length >>> 8));
			buffer.put((byte) (length >>> 16));
		} else {
			// assureBufferCapacity(9);
			buffer.put((byte) 0xfe);
			buffer.putLong(length);
		}
		return this;
	}

	/**
	 * Write string in binary format.
	 * 
	 * @param str
	 *            string to encode
	 * @return this.
	 */
	public PacketOutputStream writeStringLength(final String str) {
		try {
			final byte[] strBytes = str.getBytes("UTF-8");
			// assureBufferCapacity(strBytes.length + 9);
			writeFieldLength(strBytes.length);
			buffer.put(strBytes);
		} catch (UnsupportedEncodingException u) {
		}
		return this;
	}

	/**
	 * Write timestamp in binary format.
	 * 
	 * @param calendar
	 *            session calendar
	 * @param ts
	 *            timestamp to send
	 * @param fractionalSeconds
	 *            must fractionnal second be send to server
	 * @return this
	 */
	public PacketOutputStream writeTimestampLength(final Calendar calendar, Timestamp ts, boolean fractionalSeconds) {
		// assureBufferCapacity(12);
		buffer.put((byte) 11);// length

		buffer.putShort((short) calendar.get(Calendar.YEAR));
		buffer.put((byte) ((calendar.get(Calendar.MONTH) + 1) & 0xff));
		buffer.put((byte) (calendar.get(Calendar.DAY_OF_MONTH) & 0xff));
		buffer.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
		buffer.put((byte) calendar.get(Calendar.MINUTE));
		buffer.put((byte) calendar.get(Calendar.SECOND));
		if (fractionalSeconds) {
			buffer.putInt(ts.getNanos() / 1000);
		}
		return this;
	}

	/**
	 * Write date in binary format.
	 * 
	 * @param calendar
	 *            date
	 * @return this
	 */
	public PacketOutputStream writeDateLength(final Calendar calendar) {
		// assureBufferCapacity(8);
		buffer.put((byte) 7);// length
		buffer.putShort((short) calendar.get(Calendar.YEAR));
		buffer.put((byte) ((calendar.get(Calendar.MONTH) + 1) & 0xff));
		buffer.put((byte) (calendar.get(Calendar.DAY_OF_MONTH) & 0xff));
		buffer.put((byte) 0);
		buffer.put((byte) 0);
		buffer.put((byte) 0);
		return this;
	}

	/**
	 * Write time in binary format.
	 * 
	 * @param calendar
	 *            session calendar.
	 * @param fractionalSeconds
	 *            fractional seconds must be send
	 * @return this
	 */
	public PacketOutputStream writeTimeLength(final Calendar calendar, final boolean fractionalSeconds) {
		if (fractionalSeconds) {
			// assureBufferCapacity(13);
			buffer.put((byte) 12);
			buffer.put((byte) 0);
			buffer.putInt(0);
			buffer.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
			buffer.put((byte) calendar.get(Calendar.MINUTE));
			buffer.put((byte) calendar.get(Calendar.SECOND));
			buffer.putInt(calendar.get(Calendar.MILLISECOND) * 1000);
		} else {
			// assureBufferCapacity(9);
			buffer.put((byte) 8);// length
			buffer.put((byte) 0);
			buffer.putInt(0);
			buffer.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
			buffer.put((byte) calendar.get(Calendar.MINUTE));
			buffer.put((byte) calendar.get(Calendar.SECOND));
		}
		return this;
	}

	public void oneBlock(int size) {
		this.buffer.assureBufferCapacity(size);
	}

}

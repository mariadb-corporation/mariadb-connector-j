// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client.socket;

import java.io.IOException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.socket.impl.ByteCountingWriter;

class ByteCountingWriterTest {

	@Test
	void testCounts() throws IOException {
		ByteCountingWriter w = new ByteCountingWriter();
		Assertions.assertEquals(0, w.getSequence());
		Assertions.assertEquals(0, w.getByteCount());
		
		w.writeByte(0);
		Assertions.assertEquals(1, w.getByteCount());

		w.writeByte(0);
		Assertions.assertEquals(2, w.getByteCount());

		w.resetCount();
		Assertions.assertEquals(0, w.getByteCount());
		
		w.writeShort((short) 0);
		Assertions.assertEquals(2, w.getByteCount());
		
		w.resetCount();
		w.writeInt((short) 0);
		Assertions.assertEquals(4, w.getByteCount());
		
		w.resetCount();
		w.writeLong((short) 0);
		Assertions.assertEquals(8, w.getByteCount());
		
		w.resetCount();
		w.writeDouble(0d);
		Assertions.assertEquals(8, w.getByteCount());

		w.resetCount();
		w.writeFloat(0f);
		Assertions.assertEquals(4, w.getByteCount());

		w.resetCount();
		w.writeBytes(new byte[] {} );
		Assertions.assertEquals(0, w.getByteCount());

		w.resetCount();
		w.writeBytes(new byte[] { 1, 2, 3 } );
		Assertions.assertEquals(3, w.getByteCount());

		w.resetCount();
		w.writeBytes(new byte[] { 1, 2, 3 }, 1, 2 );
		Assertions.assertEquals(2, w.getByteCount());
		
		w.resetCount();
		w.writeLength(0);
		Assertions.assertEquals(1, w.getByteCount());

		w.resetCount();
		w.writeLength(1000);
		Assertions.assertEquals(3, w.getByteCount());

		w.resetCount();
		w.writeLength(70000);
		Assertions.assertEquals(4, w.getByteCount());

		w.resetCount();
		w.writeLength(20000000);
		Assertions.assertEquals(9, w.getByteCount());

		w.resetCount();
		w.writeAscii("Testing");
		Assertions.assertEquals(7, w.getByteCount());

		w.resetCount();
		w.writeString("Testing \uD83D\uDE00");
		Assertions.assertEquals(12, w.getByteCount());

		
		// noBackslashEscapes = false
		w.resetCount();
		w.writeStringEscaped("Testing \uD83D\uDE00", false);
		Assertions.assertEquals(12, w.getByteCount());
		
		w.resetCount();
		w.writeStringEscaped("Testing \uD83D\uDE00 '", false);
		Assertions.assertEquals(15, w.getByteCount()); // quote is backslash-escaped

		w.resetCount();
		w.writeStringEscaped("Testing \uD83D\uDE00 '\\\"\0'", false); // multiple characters escaped
		Assertions.assertEquals(23, w.getByteCount());

		// noBackslashEscapes = true
		w.resetCount();
		w.writeStringEscaped("Testing \uD83D\uDE00", true);
		Assertions.assertEquals(12, w.getByteCount());
		
		w.resetCount();
		w.writeStringEscaped("Testing \uD83D\uDE00 '", true);
		Assertions.assertEquals(15, w.getByteCount()); // quote is backslash-escaped

		w.resetCount();
		w.writeStringEscaped("Testing \uD83D\uDE00 '\\\"\0'", true); // only quotes are backslash-escaped
		Assertions.assertEquals(20, w.getByteCount());


	}
	
	/** The ByteCountingWriter has some methods which are never called but must be 
	 * defined in order to implement the  {@link org.mariadb.jdbc.client.socket.Writer} interface.
	 * 
	 * This test checks those methods all throw UnsupportedOperationException.
	 */
	@Test
	void testUnsupported() {
		ByteCountingWriter w = new ByteCountingWriter();
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.pos(); });

		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.buf(); });

		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.pos(0); });

		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.writeBytesAtPos(new byte[] { 1, 2, 3}, 0); });

		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.writeEmptyPacket(); });

		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.flush(); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.flushPipeline(); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.throwMaxAllowedLength(1); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.getCmdLength(); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.permitTrace(true); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.setServerThreadId(null, null); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.mark(); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.isMarked(); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.hasFlushed(); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.flushBufferStopAtMark(); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.bufIsDataAfterMark(); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.resetMark(); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.initPacket(); });
		
		Assertions.assertThrows(UnsupportedOperationException.class, 
			() -> { w.close(); });
	}
}

// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit.plugin.codec;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.plugin.codec.FloatArrayCodec;
import org.mariadb.jdbc.plugin.codec.FloatObjectArrayCodec;

/**
 * {@link FloatObjectArrayCodec#toFloatArray(byte[])} used to hand-roll its own bit-shifting with a
 * bounds check comparing a float-index to a float-count instead of validating byte offsets,
 * throwing {@link ArrayIndexOutOfBoundsException} on any byte array not a multiple of 4. It now
 * delegates to {@link FloatArrayCodec#toFloatArray(byte[])} and boxes the result.
 */
public class FloatObjectArrayCodecTest {

  @Test
  public void classNameReportsBoxedType() {
    assertEquals(Float[].class.getName(), FloatObjectArrayCodec.INSTANCE.className());
  }

  @Test
  public void emptyArray() {
    assertArrayEquals(new Float[0], FloatObjectArrayCodec.toFloatArray(new byte[0]));
  }

  @Test
  public void roundTripsThroughToByteArray() {
    Float[] values = {1.5f, -2.25f, 0f, Float.MAX_VALUE, Float.MIN_VALUE};
    byte[] encoded = FloatObjectArrayCodec.toByteArray(values);
    assertArrayEquals(values, FloatObjectArrayCodec.toFloatArray(encoded));
  }

  @Test
  public void matchesPrimitiveCodecForAlignedData() {
    byte[] aligned = {1, 2, 3, 4, 5, 6, 7, 8};
    float[] primitive = FloatArrayCodec.toFloatArray(aligned);
    Float[] boxed = FloatObjectArrayCodec.toFloatArray(aligned);
    assertEquals(primitive.length, boxed.length);
    for (int i = 0; i < primitive.length; i++) {
      assertEquals(primitive[i], boxed[i]);
    }
  }

  @Test
  public void misalignedByteArrayNoLongerThrows() {
    // 6 bytes: one full float (4 bytes) + a partial trailing group (2 bytes). The old
    // implementation indexed byteArray[pos * 4 + 2]/[+3] out of bounds for this case.
    byte[] misaligned = {1, 2, 3, 4, 5, 6};
    Float[] result = assertDoesNotThrow(() -> FloatObjectArrayCodec.toFloatArray(misaligned));
    // matches FloatArrayCodec's own (already-shipped) truncating behavior for misaligned data
    assertEquals(FloatArrayCodec.toFloatArray(misaligned).length, result.length);
  }
}

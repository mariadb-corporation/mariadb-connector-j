// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz.support;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.plugin.codec.BigDecimalCodec;
import org.mariadb.jdbc.plugin.codec.BooleanCodec;
import org.mariadb.jdbc.plugin.codec.ByteArrayCodec;
import org.mariadb.jdbc.plugin.codec.ByteCodec;
import org.mariadb.jdbc.plugin.codec.DateCodec;
import org.mariadb.jdbc.plugin.codec.DoubleCodec;
import org.mariadb.jdbc.plugin.codec.FloatCodec;
import org.mariadb.jdbc.plugin.codec.IntCodec;
import org.mariadb.jdbc.plugin.codec.LongCodec;
import org.mariadb.jdbc.plugin.codec.ShortCodec;
import org.mariadb.jdbc.plugin.codec.StringCodec;
import org.mariadb.jdbc.plugin.codec.TimeCodec;
import org.mariadb.jdbc.plugin.codec.TimestampCodec;

/** Unified value and codec generator for fuzzing. */
public class FuzzValueGenerator {
  public static final Codec<?>[] CODECS =
      new Codec<?>[] {
        BigDecimalCodec.INSTANCE,
        BooleanCodec.INSTANCE,
        ByteArrayCodec.INSTANCE,
        ByteCodec.INSTANCE,
        DateCodec.INSTANCE,
        DoubleCodec.INSTANCE,
        FloatCodec.INSTANCE,
        IntCodec.INSTANCE,
        LongCodec.INSTANCE,
        ShortCodec.INSTANCE,
        StringCodec.INSTANCE,
        TimeCodec.INSTANCE,
        TimestampCodec.INSTANCE
      };

  public static Codec<?> pickCodec(FuzzedDataProvider data) {
    return data.pickValue(CODECS);
  }

  public static Object generateValue(FuzzedDataProvider data) {
    int type = data.consumeInt(0, 12);
    switch (type) {
      case 0: return BigDecimal.valueOf(data.consumeDouble());
      case 1: return data.consumeBoolean();
      case 2: return data.consumeBytes(data.consumeInt(0, 100));
      case 3: return data.consumeByte();
      case 4: return new Date(data.consumeLong());
      case 5: return data.consumeDouble();
      case 6: return data.consumeFloat();
      case 7: return data.consumeInt();
      case 8: return data.consumeLong();
      case 9: return data.consumeShort();
      case 10: return data.consumeString(100);
      case 11: return new Time(data.consumeLong());
      case 12: return new Timestamp(data.consumeLong());
      default: return null;
    }
  }

  /**
   * Generates a value for a specific codec.
   * Required by MariaDbCodecFuzzer.
   */
  public static Object generate(FuzzedDataProvider data, Codec<?> codec) {
    return generateValue(data);
  }
}

// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.codec;

import org.mariadb.jdbc.codec.list.*;

public class DefaultCodecList implements CodecList {

  public Codec<?>[] getCodecs() {
    return new Codec<?>[] {
      BigDecimalCodec.INSTANCE,
      BigIntegerCodec.INSTANCE,
      BooleanCodec.INSTANCE,
      BlobCodec.INSTANCE,
      ByteArrayCodec.INSTANCE,
      ByteCodec.INSTANCE,
      BitSetCodec.INSTANCE,
      ClobCodec.INSTANCE,
      DoubleCodec.INSTANCE,
      LongCodec.INSTANCE,
      FloatCodec.INSTANCE,
      IntCodec.INSTANCE,
      LocalDateCodec.INSTANCE,
      LocalDateTimeCodec.INSTANCE,
      LocalTimeCodec.INSTANCE,
      DurationCodec.INSTANCE,
      ReaderCodec.INSTANCE,
      TimeCodec.INSTANCE,
      ZonedDateTimeCodec.INSTANCE,
      TimestampCodec.INSTANCE,
      DateCodec.INSTANCE,
      ShortCodec.INSTANCE,
      StreamCodec.INSTANCE,
      StringCodec.INSTANCE,
      PointCodec.INSTANCE,
      LineStringCodec.INSTANCE,
      PolygonCodec.INSTANCE,
      MultiPointCodec.INSTANCE,
      MultiLinestringCodec.INSTANCE,
      MultiPolygonCodec.INSTANCE,
      GeometryCollectionCodec.INSTANCE
    };
  }
}

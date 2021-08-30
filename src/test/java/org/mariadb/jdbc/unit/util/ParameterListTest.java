// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.codec.Parameter;
import org.mariadb.jdbc.plugin.codec.StringCodec;
import org.mariadb.jdbc.util.ParameterList;

public class ParameterListTest {

  @Test
  public void sizeLimit() {
    ParameterList p = new ParameterList();
    p.set(2, new Parameter<>(StringCodec.INSTANCE, "test2"));
    assertNull(p.get(1));
    assertNotNull(p.get(2));
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> p.get(3));
  }
}

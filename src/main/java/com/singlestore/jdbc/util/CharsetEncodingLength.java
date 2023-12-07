// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.util;

import java.util.HashMap;
import java.util.Map;

public final class CharsetEncodingLength {

  // This array stored character length for every collation
  // query to generate:
  //   select  id, maxlen
  //   from information_schema.character_sets, information_schema.collations
  //   where character_sets.character_set_name = collations.character_set_name
  //   order by id
  public static final Map<Integer, Integer> maxCharlen;

  static {
    maxCharlen = new HashMap<>();
    maxCharlen.put(33, 3);
    maxCharlen.put(45, 4);
    maxCharlen.put(46, 4);
    maxCharlen.put(63, 1);
    maxCharlen.put(83, 3);
    maxCharlen.put(192, 3);
    maxCharlen.put(193, 3);
    maxCharlen.put(194, 3);
    maxCharlen.put(195, 3);
    maxCharlen.put(196, 3);
    maxCharlen.put(197, 3);
    maxCharlen.put(198, 3);
    maxCharlen.put(199, 3);
    maxCharlen.put(200, 3);
    maxCharlen.put(201, 3);
    maxCharlen.put(202, 3);
    maxCharlen.put(203, 3);
    maxCharlen.put(204, 3);
    maxCharlen.put(205, 3);
    maxCharlen.put(206, 3);
    maxCharlen.put(207, 3);
    maxCharlen.put(208, 3);
    maxCharlen.put(209, 3);
    maxCharlen.put(210, 3);
    maxCharlen.put(211, 3);
    maxCharlen.put(224, 4);
    maxCharlen.put(225, 4);
    maxCharlen.put(226, 4);
    maxCharlen.put(227, 4);
    maxCharlen.put(228, 4);
    maxCharlen.put(229, 4);
    maxCharlen.put(230, 4);
    maxCharlen.put(231, 4);
    maxCharlen.put(232, 4);
    maxCharlen.put(233, 4);
    maxCharlen.put(234, 4);
    maxCharlen.put(235, 4);
    maxCharlen.put(236, 4);
    maxCharlen.put(237, 4);
    maxCharlen.put(238, 4);
    maxCharlen.put(239, 4);
    maxCharlen.put(240, 4);
    maxCharlen.put(241, 4);
    maxCharlen.put(242, 4);
    maxCharlen.put(243, 4);
  }
}

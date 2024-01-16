// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.util;

import org.mariadb.jdbc.integration.ConnectionTest;

public class WrongSocketFactoryTest {

  static {
    ConnectionTest.staticTestValue = 50;
  }

  public WrongSocketFactoryTest() {
    ConnectionTest.staticTestValue = 100;
  }
}

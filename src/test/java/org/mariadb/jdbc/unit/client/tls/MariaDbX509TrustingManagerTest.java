// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.client.tls;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.tls.MariaDbX509TrustingManager;

public class MariaDbX509TrustingManagerTest {

  @Test
  public void check() {
    MariaDbX509TrustingManager trustingManager = new MariaDbX509TrustingManager();
    assertNull(trustingManager.getAcceptedIssuers());
    trustingManager.checkClientTrusted(null, null);
    trustingManager.checkServerTrusted(null, null);
  }
}

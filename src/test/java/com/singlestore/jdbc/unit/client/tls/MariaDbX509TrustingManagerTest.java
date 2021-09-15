// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.unit.client.tls;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.client.tls.MariaDbX509TrustingManager;
import org.junit.jupiter.api.Test;

public class MariaDbX509TrustingManagerTest {

  @Test
  public void check() throws Exception {
    MariaDbX509TrustingManager trustingManager = new MariaDbX509TrustingManager();
    assertNull(trustingManager.getAcceptedIssuers());
    trustingManager.checkClientTrusted(null, null);
    trustingManager.checkServerTrusted(null, null);
  }
}

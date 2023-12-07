// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.unit.util.constant;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.client.impl.ConnectionHelper;
import com.singlestore.jdbc.export.HaMode;
import com.singlestore.jdbc.plugin.authentication.AuthenticationPluginLoader;
import com.singlestore.jdbc.plugin.credential.CredentialPluginLoader;
import com.singlestore.jdbc.plugin.tls.TlsSocketPluginLoader;
import com.singlestore.jdbc.pool.Pools;
import com.singlestore.jdbc.util.CharsetEncodingLength;
import com.singlestore.jdbc.util.NativeSql;
import com.singlestore.jdbc.util.Security;
import com.singlestore.jdbc.util.VersionFactory;
import com.singlestore.jdbc.util.constants.Capabilities;
import com.singlestore.jdbc.util.constants.ColumnFlags;
import com.singlestore.jdbc.util.constants.ConnectionState;
import com.singlestore.jdbc.util.constants.ServerStatus;
import com.singlestore.jdbc.util.constants.StateChange;
import com.singlestore.jdbc.util.log.LoggerHelper;
import com.singlestore.jdbc.util.log.Loggers;
import com.singlestore.jdbc.util.options.OptionAliases;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HaModeTest {
  @Test
  public void instantiateStaticOnlyClass() {
    Capabilities capabilities = new Capabilities();
    ColumnFlags columnFlags = new ColumnFlags();
    ConnectionState connectionState = new ConnectionState();
    ServerStatus serverStatus = new ServerStatus();
    StateChange stateChange = new StateChange();
    CharsetEncodingLength c = new CharsetEncodingLength();
    NativeSql n = new NativeSql();
    Security s = new Security();
    OptionAliases oa = new OptionAliases();
    CredentialPluginLoader cp = new CredentialPluginLoader();
    AuthenticationPluginLoader ap = new AuthenticationPluginLoader();
    TlsSocketPluginLoader tp = new TlsSocketPluginLoader();
    LoggerHelper lh = new LoggerHelper();
    ConnectionHelper ch = new ConnectionHelper();
    Pools p = new Pools();
    Loggers l = new Loggers();
    VersionFactory vv = new VersionFactory();
  }

  @Test
  public void replicationEndOfBlacklistTest() {
    HostAddress host1 = HostAddress.from("1", 3306, true);
    HostAddress host2 = HostAddress.from("2", 3306, false);
    HostAddress host3 = HostAddress.from("3", 3306, false);

    List<HostAddress> available = new ArrayList<>();
    available.add(host1);
    available.add(host2);
    available.add(host3);

    ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
    denyList.putIfAbsent(host1, System.currentTimeMillis() - 10);

    Optional<HostAddress> res = HaMode.REPLICATION.getAvailableHost(available, denyList, true);
    Assertions.assertTrue(res.isPresent());
    Assertions.assertEquals(host1, res.get());

    int replica1 = 0;
    int replica2 = 0;
    for (int i = 0; i < 1000; i++) {
      res = HaMode.REPLICATION.getAvailableHost(available, denyList, false);
      Assertions.assertTrue(res.isPresent());
      if (host2.equals(res.get())) replica1++;
      if (host3.equals(res.get())) replica2++;
    }
    assertTrue(replica1 > 350 && replica2 > 350, "bad distribution :" + replica1 + "/" + replica2);

    replica1 = 0;
    replica2 = 0;
    denyList.putIfAbsent(host2, System.currentTimeMillis() - 10);
    for (int i = 0; i < 1000; i++) {
      res = HaMode.REPLICATION.getAvailableHost(available, denyList, false);
      Assertions.assertTrue(res.isPresent());
      if (host2.equals(res.get())) replica1++;
      if (host3.equals(res.get())) replica2++;
    }
    assertTrue(replica1 > 350 && replica2 > 350, "bad distribution :" + replica1 + "/" + replica2);

    for (int i = 0; i < 1000; i++) {
      denyList.putIfAbsent(host2, System.currentTimeMillis() + 1000);
      res = HaMode.REPLICATION.getAvailableHost(available, denyList, false);
      Assertions.assertTrue(res.isPresent());
      Assertions.assertEquals(host3, res.get());
    }
  }

  @Test
  public void loadBalanceTest() throws InterruptedException {
    HostAddress host1 = HostAddress.from("1", 3306, true);
    HostAddress host2 = HostAddress.from("2", 3306, true);
    HostAddress host3 = HostAddress.from("3", 3306, true);
    HostAddress host4 = HostAddress.from("4", 3306, false);
    HostAddress host5 = HostAddress.from("5", 3306, false);
    HostAddress host6 = HostAddress.from("6", 3306, false);

    List<HostAddress> available = new ArrayList<>();
    available.add(host1);
    available.add(host2);
    available.add(host3);
    available.add(host4);
    available.add(host5);
    available.add(host6);

    ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
    Map<HostAddress, Integer> res = loopPercReturn(available, denyList, true);
    Assertions.assertEquals(334, res.get(host1));
    Assertions.assertEquals(333, res.get(host2));
    Assertions.assertEquals(333, res.get(host3));

    denyList.putIfAbsent(host1, System.currentTimeMillis() + 1000000);

    res = loopPercReturn(available, denyList, true);
    Assertions.assertNull(res.get(host1));
    Assertions.assertEquals(500, res.get(host2));
    Assertions.assertEquals(500, res.get(host3));

    denyList.clear();
    denyList.putIfAbsent(host1, System.currentTimeMillis() - 1000000);

    res = loopPercReturn(available, denyList, true);
    Assertions.assertEquals(334, res.get(host1));
    Assertions.assertEquals(333, res.get(host2));
    Assertions.assertEquals(333, res.get(host3));

    res = loopPercReturn(available, denyList, false);
    Assertions.assertEquals(334, res.get(host4));
    Assertions.assertEquals(333, res.get(host5));
    Assertions.assertEquals(333, res.get(host6));
  }

  @Test
  public void noneTest() {
    HostAddress host1 = HostAddress.from("1", 3306, true);

    List<HostAddress> available = new ArrayList<>();
    available.add(host1);

    ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();

    Optional<HostAddress> res = HaMode.NONE.getAvailableHost(available, denyList, true);
    Assertions.assertTrue(res.isPresent());
    Assertions.assertEquals(host1, res.get());

    res = HaMode.NONE.getAvailableHost(new ArrayList<>(), denyList, true);
    Assertions.assertFalse(res.isPresent());
  }

  private Map<HostAddress, Integer> loopPercReturn(
      List<HostAddress> available, ConcurrentMap<HostAddress, Long> denyList, boolean primary) {
    Map<HostAddress, Integer> resMap = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      Optional<HostAddress> res = HaMode.LOADBALANCE.getAvailableHost(available, denyList, primary);
      if (res.isPresent()) {
        if (resMap.containsKey(res.get())) {
          resMap.put(res.get(), resMap.get(res.get()) + 1);
        } else {
          resMap.put(res.get(), 1);
        }
      }
    }
    return resMap;
  }
}

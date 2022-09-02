// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.util.constant;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.impl.ConnectionHelper;
import org.mariadb.jdbc.client.socket.impl.SocketUtility;
import org.mariadb.jdbc.export.HaMode;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPluginLoader;
import org.mariadb.jdbc.plugin.credential.CredentialPluginLoader;
import org.mariadb.jdbc.plugin.tls.TlsSocketPluginLoader;
import org.mariadb.jdbc.pool.Pools;
import org.mariadb.jdbc.util.CharsetEncodingLength;
import org.mariadb.jdbc.util.NativeSql;
import org.mariadb.jdbc.util.Security;
import org.mariadb.jdbc.util.VersionFactory;
import org.mariadb.jdbc.util.constants.*;
import org.mariadb.jdbc.util.log.LoggerHelper;
import org.mariadb.jdbc.util.log.Loggers;
import org.mariadb.jdbc.util.options.OptionAliases;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HaModeTest {
  @Test
  public void instantiateStaticOnlyClass() {
    new Capabilities();
    new ColumnFlags();
    new ConnectionState();
    new ServerStatus();
    new StateChange();
    new CharsetEncodingLength();
    new NativeSql();
    new Security();
    new OptionAliases();
    new CredentialPluginLoader();
    new AuthenticationPluginLoader();
    new TlsSocketPluginLoader();
    new LoggerHelper();
    new ConnectionHelper();
    new Pools();
    new Loggers();
    new VersionFactory();
    new SocketUtility();
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
    assertTrue(
            replica1 > 350 && replica2 > 350, "bad distribution :" + replica1 + "/" + replica2);


    replica1 = 0;
    replica2 = 0;
    denyList.putIfAbsent(host2, System.currentTimeMillis() - 10);
    for (int i = 0; i < 1000; i++) {
      res = HaMode.REPLICATION.getAvailableHost(available, denyList, false);
      Assertions.assertTrue(res.isPresent());
      if (host2.equals(res.get())) replica1++;
      if (host3.equals(res.get())) replica2++;
    }
    assertTrue(
            replica1 > 350 && replica2 > 350, "bad distribution :" + replica1 + "/" + replica2);

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
    Map<HostAddress, Integer> res = loopPercReturn(available, denyList);
    Integer use = res.get(host1);
    Assertions.assertTrue(use > 250 && use < 400, "Expect 33% host1, 33% host2 and 33% host 3");

    denyList.putIfAbsent(host1, System.currentTimeMillis() + 10000);

    res = loopPercReturn(available, denyList);
    use = res.get(host2);
    Assertions.assertTrue(
        use > 400 && use < 600, "Expect 50% host2 and 50% host 3, but was " + use);
    denyList.clear();

    res = loopPercReturn(available, denyList);
    use = res.get(host1);
    Assertions.assertTrue(use > 250 && use < 400, "Expect 33% host1, 33% host2 and 33% host 3");
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
      List<HostAddress> available, ConcurrentMap<HostAddress, Long> denyList) {
    Map<HostAddress, Integer> resMap = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      Optional<HostAddress> res = HaMode.LOADBALANCE.getAvailableHost(available, denyList, true);
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

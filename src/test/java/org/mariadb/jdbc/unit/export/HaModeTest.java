// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.export;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.export.HaMode;

public class HaModeTest {
  @Test
  public void getAvailableHostWithoutConnectionNumber() {
    getAvailableHostWithoutConnectionNumber(HaMode.REPLICATION);
    getAvailableHostWithoutConnectionNumber(HaMode.LOADBALANCE);
  }

  private void getAvailableHostWithoutConnectionNumber(HaMode haMode) {
    List<HostAddress> hostAddresses = new ArrayList<>();
    hostAddresses.add(HostAddress.from("prim1", 3306, true));
    hostAddresses.add(HostAddress.from("prim2", 3306, true));
    hostAddresses.add(HostAddress.from("prim3", 3306, true));
    hostAddresses.add(HostAddress.from("replica1", 3306, false));
    hostAddresses.add(HostAddress.from("replica2", 3306, false));
    hostAddresses.add(HostAddress.from("replica3", 3306, false));

    haMode.resetLast();
    ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
    HostCounter hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, true);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals("prim1:34,prim2:33,prim3:33", hostCounter.results());

    haMode.resetLast();
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, false);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals("replica1:34,replica2:33,replica3:33", hostCounter.results());

    haMode.resetLast();
    denyList.put(hostAddresses.get(0), System.currentTimeMillis() - 100);
    denyList.put(hostAddresses.get(1), System.currentTimeMillis() + 1000);

    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, true);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals("prim1:50,prim3:50", hostCounter.results());

    haMode.resetLast();
    denyList.clear();
    denyList.put(hostAddresses.get(3), System.currentTimeMillis() - 100);
    denyList.put(hostAddresses.get(4), System.currentTimeMillis() + 1000);
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, false);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals("replica1:50,replica3:50", hostCounter.results());
  }

  @Test
  public void loadBalanceRead() {

    HaMode haMode = HaMode.LOAD_BALANCE_READ;
    List<HostAddress> hostAddresses = new ArrayList<>();
    hostAddresses.add(HostAddress.from("prim1", 3306, true));
    hostAddresses.add(HostAddress.from("prim2", 3306, true));
    hostAddresses.add(HostAddress.from("prim3", 3306, true));
    hostAddresses.add(HostAddress.from("replica1", 3306, false));
    hostAddresses.add(HostAddress.from("replica2", 3306, false));
    hostAddresses.add(HostAddress.from("replica3", 3306, false));

    haMode.resetLast();
    ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
    HostCounter hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, true);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals("prim1:100", hostCounter.results());

    haMode.resetLast();
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, false);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals("replica1:34,replica2:33,replica3:33", hostCounter.results());

    haMode.resetLast();
    denyList.put(hostAddresses.get(1), System.currentTimeMillis() + 1000);
    denyList.put(hostAddresses.get(0), System.currentTimeMillis() - 100);

    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, true);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals("prim1:100", hostCounter.results());

    haMode.resetLast();
    denyList.clear();
    denyList.put(hostAddresses.get(3), System.currentTimeMillis() - 100);
    denyList.put(hostAddresses.get(4), System.currentTimeMillis() + 1000);
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, false);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), false);
    }
    assertEquals("replica1:50,replica3:50", hostCounter.results());
  }

  @Test
  public void getAvailableHostWithConnectionNumber() {
    getAvailableHostWithConnectionNumber(HaMode.LOADBALANCE);
    getAvailableHostWithConnectionNumber(HaMode.REPLICATION);
  }

  private void getAvailableHostWithConnectionNumber(HaMode haMode) {
    List<HostAddress> hostAddresses = new ArrayList<>();

    HostAddress host1 = HostAddress.from("prim1", 3306, true);
    HostAddress host2 = HostAddress.from("prim2", 3306, true);
    HostAddress host3 = HostAddress.from("prim3", 3306, true);
    host1.setThreadsConnected(200);
    host2.setThreadsConnected(150);
    host3.setThreadsConnected(100);
    hostAddresses.add(host1);
    hostAddresses.add(host2);
    hostAddresses.add(host3);
    HostAddress replica1 = HostAddress.from("replica1", 3306, false);
    HostAddress replica2 = HostAddress.from("replica2", 3306, false);
    HostAddress replica3 = HostAddress.from("replica3", 3306, false);
    replica1.setThreadsConnected(200);
    replica2.setThreadsConnected(150);
    replica3.setThreadsConnected(100);
    hostAddresses.add(replica1);
    hostAddresses.add(replica2);
    hostAddresses.add(replica3);

    ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
    HostCounter hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, true);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), true);
    }
    assertEquals("prim2:25,prim3:75", hostCounter.results());

    host1.forceThreadsConnected(200, System.currentTimeMillis() - 50000);
    host2.setThreadsConnected(150);
    host3.setThreadsConnected(100);
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, true);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), true);
    }
    assertEquals("prim1:34,prim2:33,prim3:33", hostCounter.results());

    replica1.setThreadsConnected(200);
    replica2.setThreadsConnected(150);
    replica3.setThreadsConnected(100);
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, false);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), true);
    }
    assertEquals("replica2:25,replica3:75", hostCounter.results());

    denyList.put(hostAddresses.get(0), System.currentTimeMillis() - 100);
    denyList.put(hostAddresses.get(1), System.currentTimeMillis() + 1000);
    host1.setThreadsConnected(150);
    host2.setThreadsConnected(150);
    host3.setThreadsConnected(100);
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, true);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), true);
    }
    assertEquals("prim1:25,prim3:75", hostCounter.results());

    denyList.clear();
    denyList.put(hostAddresses.get(3), System.currentTimeMillis() - 100);
    denyList.put(hostAddresses.get(4), System.currentTimeMillis() + 1000);
    replica1.setThreadsConnected(150);
    replica2.setThreadsConnected(150);
    replica3.setThreadsConnected(100);
    hostCounter = new HostCounter();
    for (int i = 0; i < 100; i++) {
      Optional<HostAddress> availHost = haMode.getAvailableHost(hostAddresses, denyList, false);
      if (availHost.isPresent()) hostCounter.add(availHost.get(), true);
    }
    assertEquals("replica1:25,replica3:75", hostCounter.results());
  }

  private static class HostCounter {
    Map<HostAddress, Integer> hosts = new HashMap<>();

    public void add(HostAddress hostAddress, boolean increment) {
      Integer counter = hosts.get(hostAddress);
      if (counter == null) {
        hosts.put(hostAddress, 1);
      } else {
        hosts.replace(hostAddress, counter + 1);
      }
      if (increment) {
        if (hostAddress.getThreadsConnected() != null) {
          hostAddress.forceThreadsConnected(
              hostAddress.getThreadsConnected() + 1, hostAddress.getThreadConnectedTimeout());
        } else {
          hostAddress.forceThreadsConnected(1, System.currentTimeMillis() + 1000);
        }
      }
    }

    public String results() {
      List<String> res = new ArrayList<>();
      for (Map.Entry<HostAddress, Integer> hostEntry : hosts.entrySet()) {
        res.add(hostEntry.getKey().host + ':' + hostEntry.getValue());
      }
      Collections.sort(res);
      return String.join(",", res);
    }
  }
}

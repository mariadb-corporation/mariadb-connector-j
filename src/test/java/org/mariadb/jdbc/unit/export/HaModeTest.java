//  SPDX-License-Identifier: LGPL-2.1-or-later
//  Copyright (c) 2012-2014 Monty Program Ab
//  Copyright (c) 2023 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.export;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.export.HaMode;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        hostAddresses.add(HostAddress.from("slave1", 3306, false));
        hostAddresses.add(HostAddress.from("slave2", 3306, false));
        hostAddresses.add(HostAddress.from("slave3", 3306, false));

        haMode.resetLast();
        ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
        HostCounter hostCounter = new HostCounter();
        for (int i = 0; i < 100; i++) {
            hostCounter.add(haMode.getAvailableHost(hostAddresses, denyList, true).get(), false);
        }
        assertEquals("prim1:34,prim2:33,prim3:33", hostCounter.results());

        haMode.resetLast();
        hostCounter = new HostCounter();
        for (int i = 0; i < 100; i++) {
            hostCounter.add(haMode.getAvailableHost(hostAddresses, denyList, false).get(), false);
        }
        assertEquals("slave1:34,slave2:33,slave3:33", hostCounter.results());

        haMode.resetLast();
        denyList.put(hostAddresses.get(0), System.currentTimeMillis() - 100);
        denyList.put(hostAddresses.get(1), System.currentTimeMillis() + 1000);

        hostCounter = new HostCounter();
        for (int i = 0; i < 100; i++) {
            hostCounter.add(haMode.getAvailableHost(hostAddresses, denyList, true).get(), false);
        }
        assertEquals("prim1:50,prim3:50", hostCounter.results());

        haMode.resetLast();
        denyList.clear();
        denyList.put(hostAddresses.get(3), System.currentTimeMillis() - 100);
        denyList.put(hostAddresses.get(4), System.currentTimeMillis() + 1000);
        hostCounter = new HostCounter();
        for (int i = 0; i < 100; i++) {
            hostCounter.add(haMode.getAvailableHost(hostAddresses, denyList, false).get(), false);
        }
        assertEquals("slave1:50,slave3:50", hostCounter.results());
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
        HostAddress slave1 = HostAddress.from("slave1", 3306, false);
        HostAddress slave2 = HostAddress.from("slave2", 3306, false);
        HostAddress slave3 = HostAddress.from("slave3", 3306, false);
        slave1.setThreadsConnected(200);
        slave2.setThreadsConnected(150);
        slave3.setThreadsConnected(100);
        hostAddresses.add(slave1);
        hostAddresses.add(slave2);
        hostAddresses.add(slave3);

        ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
        HostCounter hostCounter = new HostCounter();
        for (int i = 0; i < 100; i++) {
            hostCounter.add(haMode.getAvailableHost(hostAddresses, denyList, true).get(), true);
        }
        assertEquals("prim2:25,prim3:75", hostCounter.results());

        host1.forceThreadsConnected(200, System.currentTimeMillis() - 50000);
        host2.setThreadsConnected(150);
        host3.setThreadsConnected(100);
        hostCounter = new HostCounter();
        for (int i = 0; i < 100; i++) {
            hostCounter.add(haMode.getAvailableHost(hostAddresses, denyList, true).get(), true);
        }
        assertEquals("prim1:34,prim2:33,prim3:33", hostCounter.results());

        slave1.setThreadsConnected(200);
        slave2.setThreadsConnected(150);
        slave3.setThreadsConnected(100);
        hostCounter = new HostCounter();
        for (int i = 0; i < 100; i++) {
            hostCounter.add(haMode.getAvailableHost(hostAddresses, denyList, false).get(), true);
        }
        assertEquals("slave2:25,slave3:75", hostCounter.results());

        denyList.put(hostAddresses.get(0), System.currentTimeMillis() - 100);
        denyList.put(hostAddresses.get(1), System.currentTimeMillis() + 1000);
        host1.setThreadsConnected(150);
        host2.setThreadsConnected(150);
        host3.setThreadsConnected(100);
        hostCounter = new HostCounter();
        for (int i = 0; i < 100; i++) {
            hostCounter.add(haMode.getAvailableHost(hostAddresses, denyList, true).get(), true);
        }
        assertEquals("prim1:25,prim3:75", hostCounter.results());

        denyList.clear();
        denyList.put(hostAddresses.get(3), System.currentTimeMillis() - 100);
        denyList.put(hostAddresses.get(4), System.currentTimeMillis() + 1000);
        slave1.setThreadsConnected(150);
        slave2.setThreadsConnected(150);
        slave3.setThreadsConnected(100);
        hostCounter = new HostCounter();
        for (int i = 0; i < 100; i++) {
            hostCounter.add(haMode.getAvailableHost(hostAddresses, denyList, false).get(), true);
        }
        assertEquals("slave1:25,slave3:75", hostCounter.results());
    }


    private class HostCounter {
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
                    hostAddress.forceThreadsConnected(hostAddress.getThreadsConnected() + 1, hostAddress.getThreadConnectedTimeout());
                } else {
                    hostAddress.forceThreadsConnected(1, System.currentTimeMillis() + 1000);
                }
            }
        }

        public String results() {
            List<String> res = new ArrayList<>();
            for (Map.Entry<HostAddress, Integer> hostEntry : hosts.entrySet()) {
                res.add(hostEntry.getKey().host + ':' +hostEntry.getValue());
            }
            Collections.sort(res);
            return String.join(",", res);
        }
    }
}

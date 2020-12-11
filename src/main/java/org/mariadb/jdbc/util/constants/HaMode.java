/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.util.constants;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import org.mariadb.jdbc.HostAddress;

public enum HaMode {
  REPLICATION("replication") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses,
        ConcurrentMap<HostAddress, Long> denyList,
        boolean primary) {
      return HaMode.getAvailableHostInOrder(hostAddresses, denyList, primary);
    }
  },
  SEQUENTIAL("sequential") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses,
        ConcurrentMap<HostAddress, Long> denyList,
        boolean primary) {
      return getAvailableHostInOrder(hostAddresses, denyList, primary);
    }
  },
  LOADBALANCE("load-balance") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses,
        ConcurrentMap<HostAddress, Long> denyList,
        boolean primary) {
      // use in order not blacklisted server
      List<HostAddress> loopAddress = new ArrayList<>(hostAddresses);
      loopAddress.removeAll(denyList.keySet());
      Collections.shuffle(loopAddress);

      return loopAddress.stream().filter(e -> e.primary == primary).findFirst();
    }
  },
  NONE("") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses,
        ConcurrentMap<HostAddress, Long> denyList,
        boolean primary) {
      return hostAddresses.isEmpty() ? Optional.empty() : Optional.of(hostAddresses.get(0));
    }
  };

  private final String value;

  HaMode(String value) {
    this.value = value;
  }

  public static HaMode from(String value) {
    for (HaMode haMode : values()) {
      if (haMode.value.equalsIgnoreCase(value) || haMode.name().equalsIgnoreCase(value)) {
        return haMode;
      }
    }
    throw new IllegalArgumentException(
        String.format("Wrong argument value '%s' for HaMode", value));
  }

  public static Optional<HostAddress> getAvailableHostInOrder(
      List<HostAddress> hostAddresses,
      ConcurrentMap<HostAddress, Long> blacklist,
      boolean primary) {
    // use in order not blacklisted server
    HostAddress hostAddress;
    for (int i = 0; i < hostAddresses.size(); i++) {
      hostAddress = hostAddresses.get(i);
      if (hostAddress.primary == primary) {
        if (!blacklist.containsKey(hostAddress)) return Optional.of(hostAddress);
        if (blacklist.get(hostAddress) < System.currentTimeMillis()) {
          // timeout reached
          blacklist.remove(hostAddress);
          return Optional.of(hostAddress);
        }
      }
    }
    return Optional.empty();
  }

  public String value() {
    return value;
  }

  public abstract Optional<HostAddress> getAvailableHost(
      List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList, boolean primary);
}

// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.export;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import org.tidb.jdbc.HostAddress;

/** Failover (High-availability) mode */
public enum HaMode {
  /** replication mode : first is primary, other are replica */
  REPLICATION("replication") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses,
        ConcurrentMap<HostAddress, Long> denyList,
        boolean primary) {
      return HaMode.getAvailableHostInOrder(hostAddresses, denyList, primary);
    }
  },
  /** sequential: driver will always connect according to connection string order */
  SEQUENTIAL("sequential") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses,
        ConcurrentMap<HostAddress, Long> denyList,
        boolean primary) {
      return getAvailableHostInOrder(hostAddresses, denyList, primary);
    }
  },
  /** load-balance: driver will randomly connect to any host, permitting balancing connections */
  LOADBALANCE("load-balance") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses,
        ConcurrentMap<HostAddress, Long> denyList,
        boolean primary) {
      // use in order not blacklisted server
      List<HostAddress> loopAddress = new ArrayList<>(hostAddresses);

      // ensure denied server have not reached denied timeout
      denyList.entrySet().stream()
          .filter(e -> e.getValue() < System.currentTimeMillis())
          .forEach(e -> denyList.remove(e.getKey()));

      loopAddress.removeAll(denyList.keySet());

      Collections.shuffle(loopAddress);
      return loopAddress.stream().filter(e -> e.primary == primary).findFirst();
    }
  },
  /** no ha-mode. Connect to first host only */
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

  /**
   * Get HAMode from values or aliases
   *
   * @param value value or alias
   * @return HaMode if corresponding mode is found
   */
  public static HaMode from(String value) {
    for (HaMode haMode : values()) {
      if (haMode.value.equalsIgnoreCase(value) || haMode.name().equalsIgnoreCase(value)) {
        return haMode;
      }
    }
    throw new IllegalArgumentException(
        String.format("Wrong argument value '%s' for HaMode", value));
  }

  /**
   * return hosts of corresponding type (primary or not) without blacklisted hosts. hosts in
   * blacklist reaching blacklist timeout will be present. order corresponds to connection string
   * order.
   *
   * @param hostAddresses hosts
   * @param denyList blacklist
   * @param primary returns primary hosts or replica
   * @return list without denied hosts
   */
  public static Optional<HostAddress> getAvailableHostInOrder(
      List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList, boolean primary) {
    // use in order not blacklisted server
    for (HostAddress hostAddress : hostAddresses) {
      if (hostAddress.primary == primary) {
        if (!denyList.containsKey(hostAddress)) return Optional.of(hostAddress);
        if (denyList.get(hostAddress) < System.currentTimeMillis()) {
          // timeout reached
          denyList.remove(hostAddress);
          return Optional.of(hostAddress);
        }
      }
    }
    return Optional.empty();
  }

  /**
   * List of hosts without blacklist entries, ordered according to HA mode
   *
   * @param hostAddresses hosts
   * @param denyList hosts temporary denied
   * @param primary type
   * @return list without denied hosts
   */
  public abstract Optional<HostAddress> getAvailableHost(
      List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList, boolean primary);
}

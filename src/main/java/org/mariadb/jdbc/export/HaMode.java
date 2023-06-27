// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.export;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import org.mariadb.jdbc.HostAddress;

/** Failover (High-availability) mode */
public enum HaMode {
  /** replication mode : first is primary, other are replica */
  REPLICATION("replication") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses,
        ConcurrentMap<HostAddress, Long> denyList,
        boolean primary) {
      return HaMode.getAvailableRoundRobinHost(this, hostAddresses, denyList, primary);
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
  /**
   * load-balance: driver will connect to any host using round-robin, permitting balancing
   * connections
   */
  LOADBALANCE("load-balance") {
    public Optional<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses,
        ConcurrentMap<HostAddress, Long> denyList,
        boolean primary) {
      return HaMode.getAvailableRoundRobinHost(this, hostAddresses, denyList, primary);
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
  private HostAddress lastRoundRobinPrimaryHost = null;
  private HostAddress lastRoundRobinSecondaryHost = null;

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
   * return hosts of corresponding type (primary or not) without blacklisted hosts. hosts in
   * blacklist reaching blacklist timeout will be present, RoundRobin Order.
   *
   * @param haMode current haMode
   * @param hostAddresses hosts
   * @param denyList blacklist
   * @param primary returns primary hosts or replica
   * @return list without denied hosts
   */
  public static Optional<HostAddress> getAvailableRoundRobinHost(
      HaMode haMode,
      List<HostAddress> hostAddresses,
      ConcurrentMap<HostAddress, Long> denyList,
      boolean primary) {
    HostAddress lastChosenHost =
        primary ? haMode.lastRoundRobinPrimaryHost : haMode.lastRoundRobinSecondaryHost;

    List<HostAddress> loopList;
    if (lastChosenHost == null) {
      loopList = hostAddresses;
    } else {
      int lastChosenIndex = hostAddresses.indexOf(lastChosenHost);
      loopList = new ArrayList<>();
      loopList.addAll(hostAddresses.subList(lastChosenIndex + 1, hostAddresses.size()));
      loopList.addAll(hostAddresses.subList(0, lastChosenIndex + 1));
    }

    for (HostAddress hostAddress : loopList) {
      if (hostAddress.primary == primary) {
        if (denyList.containsKey(hostAddress)) {
          // take in account denied server that have reached denied timeout
          if (denyList.get(hostAddress) > System.currentTimeMillis()) {
            continue;
          } else {
            denyList.remove(hostAddress);
          }
        }
        if (primary) {
          haMode.lastRoundRobinPrimaryHost = hostAddress;
        } else {
          haMode.lastRoundRobinSecondaryHost = hostAddress;
        }
        return Optional.of(hostAddress);
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

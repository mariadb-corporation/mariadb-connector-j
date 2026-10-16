// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.util;

import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Lookup of a plugin by type among the providers registered for a service, with the provider list
 * read once (through {@link ServiceLoader#stream()}, without instantiating anything) and the
 * provider of each already resolved type remembered. A new plugin instance is still created on
 * every lookup, as a plain {@link ServiceLoader} iteration would do, so plugin lifecycle is
 * unchanged: only the repeated classpath scan and the instantiation of the plugins listed before
 * the match are avoided.
 *
 * @param <T> service type
 */
public final class ServiceProviders<T> {

  private final Class<T> service;
  private final ClassLoader classLoader;
  private volatile List<ServiceLoader.Provider<T>> providers;
  private final ConcurrentHashMap<String, ServiceLoader.Provider<T>> byType =
      new ConcurrentHashMap<>();

  /**
   * Constructor.
   *
   * @param service service interface
   * @param classLoader class loader used to locate the providers
   */
  public ServiceProviders(Class<T> service, ClassLoader classLoader) {
    this.service = service;
    this.classLoader = classLoader;
  }

  private List<ServiceLoader.Provider<T>> providers() {
    List<ServiceLoader.Provider<T>> list = providers;
    if (list == null) {
      synchronized (this) {
        list = providers;
        if (list == null) {
          list = ServiceLoader.load(service, classLoader).stream().toList();
          providers = list;
        }
      }
    }
    return list;
  }

  /**
   * Return a new instance of the plugin registered with the given type.
   *
   * @param type plugin type
   * @param typeOf function returning the type of a plugin instance
   * @return a new plugin instance, or null when no provider has this type
   */
  public T get(String type, Function<T, String> typeOf) {
    ServiceLoader.Provider<T> known = byType.get(type);
    if (known != null) {
      return known.get();
    }
    for (ServiceLoader.Provider<T> provider : providers()) {
      T instance = provider.get();
      if (type.equals(typeOf.apply(instance))) {
        byType.put(type, provider);
        return instance;
      }
    }
    return null;
  }
}

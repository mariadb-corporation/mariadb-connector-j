// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz.support;

import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;

/** Unified mock for MariaDB parameters container. */
public class FuzzParameters implements Parameters {
  private final Parameter[] parameters;

  public FuzzParameters(Parameter[] parameters) {
    this.parameters = parameters;
  }

  @Override
  public Parameter get(int index) {
    return parameters[index];
  }

  @Override
  public boolean containsKey(int index) {
    return index >= 0 && index < parameters.length;
  }

  @Override
  public void set(int index, Parameter element) {
    parameters[index] = element;
  }

  @Override
  public int size() {
    return parameters.length;
  }

  @Override
  public Parameters clone() {
    return new FuzzParameters(parameters.clone());
  }
}

// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin;

/**
 * <p>Typical use: a JDBC client registers an implementation that surfaces
 * {@code prompt} in its UI and returns the user's answer.
 */
public interface AuthDialogCallback {

  /**
   * Answer a server-issued prompt.
   *
   * @param echo {@code true} if the server expects normal input (info / echoed input); {@code
   *     false} if it's a password-style prompt where the input should be hidden
   * @param prompt prompt text the server sent (UTF-8, may be empty)
   * @param round the round number (>= 2)
   * @return the user's answer, or {@code null} to fall through to the legacy {@code passwordN}
   *     URL options
   */
  String prompt(boolean echo, String prompt, int round);
}

// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin;

/**
 * Credential returned by a {@link CredentialPlugin}.
 *
 * @param user user name
 * @param password password, may be null
 */
public record Credential(String user, String password) {}

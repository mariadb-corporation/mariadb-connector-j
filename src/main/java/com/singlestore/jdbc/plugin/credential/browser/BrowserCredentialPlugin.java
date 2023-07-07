// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.browser;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.plugin.Credential;
import com.singlestore.jdbc.plugin.CredentialPlugin;
import com.singlestore.jdbc.plugin.credential.browser.keyring.Keyring;
import java.sql.SQLException;

public class BrowserCredentialPlugin implements CredentialPlugin {
  private static final String baseURL = "https://portal.singlestore.com/engine-sso";

  protected BrowserCredentialGenerator generator;
  private final Keyring keyring;

  private String userEmail;
  private ExpiringCredential credential;

  @Override
  public String type() {
    return "BROWSER_SSO";
  }

  @Override
  public boolean mustUseSsl() {
    return true;
  }

  @Override
  public String defaultAuthenticationPluginType() {
    return "mysql_clear_password";
  }

  public BrowserCredentialPlugin() {
    this.keyring = Keyring.buildForCurrentOS();
  }

  @Override
  public CredentialPlugin initialize(Configuration conf, String userName, HostAddress hostAddress)
      throws SQLException {
    this.generator = new BrowserCredentialGenerator(baseURL);
    return this;
  }

  @Override
  // get() is synchronized to avoid requesting credentials twice if a second thread tries to
  // establish a connection while user sign-in is still in progress
  public synchronized Credential get() throws SQLException {
    if (credential != null && credential.isValid()) {
      return credential.getCredential();
    }

    ExpiringCredential cred = null;
    if (keyring != null) {
      cred = keyring.getCredential();
    }

    if (cred == null || !cred.isValid()) {
      cred = generator.getCredential(userEmail);

      if (keyring != null) {
        keyring.setCredential(cred);
      }
    }

    credential = cred;
    userEmail = cred.getEmail();
    return cred.getCredential();
  }

  public void clearLocalCache() {
    userEmail = null;
    credential = null;
  }

  public void clearKeyring() {
    if (keyring != null) {
      keyring.deleteCredential();
    }
  }
}

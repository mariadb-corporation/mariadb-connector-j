// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.browser;

import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import org.apache.http.client.utils.URIBuilder;

public class BrowserCredentialGenerator {
  private static final Logger logger = Loggers.getLogger(BrowserCredentialGenerator.class);
  protected String baseURL;

  public BrowserCredentialGenerator(String baseURL) {
    this.baseURL = baseURL;
  }

  public ExpiringCredential getCredential(String email) throws SQLException {
    TokenWaiterServer server = new TokenWaiterServer();
    String listenPath = server.getListenPath();
    logger.debug("Listening on " + listenPath);

    URIBuilder ub;
    try {
      ub = new URIBuilder(baseURL);
      ub.addParameter("returnTo", listenPath);
      if (email != null) {
        ub.addParameter("email", email);
      }
    } catch (URISyntaxException e) {
      throw new SQLException("Failed to build a URL while using BROSWER-SSO identity plugin", e);
    }

    openBrowser(ub.toString());

    try {
      return server.WaitForCredential();
    } catch (InterruptedException e) {
      throw new SQLException("Interrupted while waiting for JWT", e);
    } catch (TimeoutException e) {
      throw new SQLException("Timed out waiting for JWT", e);
    } catch (IOException e) {
      throw new SQLException("Could not acquire JWT", e);
    }
  }

  protected void openBrowser(String url) throws SQLException {
    Runtime rt = Runtime.getRuntime();
    String operSys = System.getProperty("os.name").toLowerCase();
    try {
      if (operSys.contains("win")) {
        rt.exec("rundll32 url.dll,FileProtocolHandler " + url);
      } else if (operSys.contains("nix") || operSys.contains("nux") || operSys.contains("aix")) {
        rt.exec("xdg-open " + url);
      } else if (operSys.contains("mac")) {
        rt.exec("open " + url);
      }
    } catch (IOException e) {
      throw new SQLException("Failed to open a browser while using BROSWER-SSO identity plugin", e);
    }
  }
}

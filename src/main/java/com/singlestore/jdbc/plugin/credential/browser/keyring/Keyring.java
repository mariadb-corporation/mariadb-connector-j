// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.browser.keyring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.singlestore.jdbc.plugin.credential.browser.ExpiringCredential;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.IOException;

public interface Keyring {
  String STORAGE_KEY = "SingleStore JDBC Safe Storage";

  ExpiringCredential getCredential();

  void setCredential(ExpiringCredential cred);

  static String makeBlob(ExpiringCredential cred) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    String credJson;
    try {
      credJson = mapper.writeValueAsString(cred);
      return credJson;
    } catch (JsonProcessingException e) {
      // shouldn't be able to happen
      e.printStackTrace();
    }
    return "";
  }

  static ExpiringCredential fromBlob(String blob) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    return mapper.readValue(blob, ExpiringCredential.class);
  }

  static Keyring buildForCurrentOS() {
    String operSys = System.getProperty("os.name").toLowerCase();
    try {
      if (operSys.contains("win")) {
        return new WindowsKeyring();
      } else if (operSys.contains("nix") || operSys.contains("nux") || operSys.contains("aix")) {
        return new LinuxKeyring();
      } else if (operSys.contains("mac")) {
        return new MacKeyring();
      }
    } catch (Exception e) {
      Loggers.getLogger(Keyring.class)
          .warn(
              "Could not connect to a "
                  + System.getProperty("os.name")
                  + " OS keyring. Credentials will not be persisted between sessions.",
              e);
    }
    return null;
  }

  void deleteCredential();
}

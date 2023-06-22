// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.browser.keyring;

import com.singlestore.jdbc.plugin.credential.browser.ExpiringCredential;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.freedesktop.secret.simple.SimpleCollection;

public class LinuxKeyring implements Keyring {
  private static final Map<String, String> ATTRIBUTES =
      Collections.singletonMap("application", "singlestore-jdbc");
  final SimpleCollection collection;
  private final Logger logger;

  public LinuxKeyring() throws IOException {
    try {
      this.collection = new SimpleCollection();
      this.logger = Loggers.getLogger(LinuxKeyring.class);
    } catch (IOException e) {
      throw new IOException("Error while accessing GNOME Keyring", e);
    }
  }

  @Override
  public ExpiringCredential getCredential() {
    String entryPath = getExistingEntry();
    if (entryPath == null) {
      return null;
    }
    try {
      return Keyring.fromBlob(String.valueOf(collection.getSecret(entryPath)));
    } catch (IOException e) {
      logger.debug("Error while parsing cached token from the GNOME Keyring", e);
      return null;
    }
  }

  @Override
  public void setCredential(ExpiringCredential cred) {
    String entryPath = getExistingEntry();
    String credBlob = Keyring.makeBlob(cred);
    if (entryPath == null) {
      entryPath = collection.createItem(STORAGE_KEY, credBlob);
    }
    if (entryPath != null) {
      collection.updateItem(entryPath, STORAGE_KEY, credBlob, ATTRIBUTES);
    } else {
      logger.warn("Failed to save credentials in the GNOME keyring");
    }
  }

  // deleteCredential sets the stored credential to an empty string if it exists
  // because we cannot fully delete the entry without user interaction.
  @Override
  public void deleteCredential() {
    String entryPath = getExistingEntry();
    if (entryPath != null) {
      collection.updateItem(entryPath, STORAGE_KEY, "", ATTRIBUTES);
    }
  }

  private String getExistingEntry() {
    String foundPath = null;
    List<String> entires = collection.getItems(ATTRIBUTES);
    if (entires != null) {
      for (String entry : collection.getItems(ATTRIBUTES)) {
        if (collection.getLabel(entry).equals(STORAGE_KEY)) {
          if (foundPath != null) {
            logger.debug("Found multiple keychain entries matching \"" + STORAGE_KEY + "\"");
            return null;
          }
          foundPath = entry;
        }
        ;
      }
    }
    return foundPath;
  }
}

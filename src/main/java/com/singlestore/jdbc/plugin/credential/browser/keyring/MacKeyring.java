// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.browser.keyring;

import com.singlestore.jdbc.plugin.credential.browser.ExpiringCredential;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class MacKeyring implements Keyring {
  // OS X keychain requires an account name (arbitrary) to be specified
  // in addition to the storage key
  private static final String ACCOUNT = "SingleStore";
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  private final SecurityLibrary secLib;

  private final Logger logger;

  public MacKeyring() {
    this.secLib = LibManager.getInstance();
    this.logger = Loggers.getLogger(MacKeyring.class);
  }

  @Override
  public ExpiringCredential getCredential() {
    byte[] serviceBytes = STORAGE_KEY.getBytes(CHARSET);
    byte[] userBytes = ACCOUNT.getBytes(CHARSET);

    int[] dataLength = new int[1];
    Pointer[] data = new Pointer[1];

    try {
      int status = 0;
      synchronized (secLib) {
        status =
            secLib.SecKeychainFindGenericPassword(
                null,
                serviceBytes.length,
                serviceBytes,
                userBytes.length,
                userBytes,
                dataLength,
                data,
                null);
      }

      if (status != SecurityLibrary.ERR_SEC_SUCCESS) {
        logger.debug(
            "Could not read from the OS X Keychain or the key does not exist. Error code: "
                + Native.getLastError());
        return null;
      }

      if (dataLength[0] == 0 || data[0] == null) {
        logger.debug("Found an empty blob when reading from the OS X Keychain");
        return null;
      }

      try {
        return Keyring.fromBlob(new String(data[0].getByteArray(0, dataLength[0]), CHARSET));
      } catch (IOException e) {
        logger.debug("Error while parsing cached token from the OS X Keychain", e);
        return null;
      }
    } finally {
      if (data[0] != null) {
        synchronized (secLib) {
          secLib.SecKeychainItemFreeContent(null, data[0]);
        }
      }
    }
  }

  @Override
  public void setCredential(ExpiringCredential cred) {
    byte[] credBlob = Keyring.makeBlob(cred).getBytes(CHARSET);
    byte[] serviceBytes = STORAGE_KEY.getBytes(CHARSET);
    byte[] userBytes = ACCOUNT.getBytes(CHARSET);

    Pointer[] itemRef = new Pointer[1];
    int status;
    synchronized (secLib) {
      status =
          secLib.SecKeychainFindGenericPassword(
              null,
              serviceBytes.length,
              serviceBytes,
              userBytes.length,
              userBytes,
              null,
              null,
              itemRef);
    }

    if (status != SecurityLibrary.ERR_SEC_SUCCESS
        && status != SecurityLibrary.ERR_SEC_ITEM_NOT_FOUND) {
      logger.debug(
          "Could not check the existence of key in the OS X Keychain. Error code: "
              + Native.getLastError());
      return;
    }

    if (itemRef[0] != null) {
      synchronized (secLib) {
        status = secLib.SecKeychainItemModifyContent(itemRef[0], null, credBlob.length, credBlob);
      }
    } else {
      synchronized (secLib) {
        status =
            secLib.SecKeychainAddGenericPassword(
                Pointer.NULL,
                serviceBytes.length,
                serviceBytes,
                userBytes.length,
                userBytes,
                credBlob.length,
                credBlob,
                null);
      }
    }

    if (status != SecurityLibrary.ERR_SEC_SUCCESS) {
      logger.debug(
          "Could not set/modify the item in the OS X Keychain. Error code: "
              + Native.getLastError());
    }
  }

  @Override
  public void deleteCredential() {
    byte[] serviceBytes = STORAGE_KEY.getBytes(CHARSET);
    byte[] userBytes = ACCOUNT.getBytes(CHARSET);

    Pointer[] itemRef = new Pointer[1];

    int status = 0;
    synchronized (secLib) {
      status =
          secLib.SecKeychainFindGenericPassword(
              null,
              serviceBytes.length,
              serviceBytes,
              userBytes.length,
              userBytes,
              null,
              null,
              itemRef);
    }

    if (status != SecurityLibrary.ERR_SEC_SUCCESS
        && status != SecurityLibrary.ERR_SEC_ITEM_NOT_FOUND) {
      logger.debug(
          "Could not check the existence of key in the OS X Keychain. Error code: "
              + Native.getLastError());
      return;
    }

    if (itemRef[0] == null) {
      return;
    }

    synchronized (secLib) {
      status = secLib.SecKeychainItemDelete(itemRef[0]);
    }

    if (status != SecurityLibrary.ERR_SEC_SUCCESS) {
      logger.info(
          "Could not delete the key from the OS X Keychain. Error code = " + Native.getLastError());
    }
  }

  /*
  OS X Security library
  https://developer.apple.com/documentation/security/keychain_services/keychain_items
   */
  interface SecurityLibrary extends Library {

    int ERR_SEC_SUCCESS = 0;
    int ERR_SEC_ITEM_NOT_FOUND = -25300;

    int // OSStatus
        SecKeychainFindGenericPassword(
        Pointer keychainOrArray, // CFTypeRef
        int serviceNameLength, // UInt32
        byte[] serviceName, // const char*
        int accountNameLength, // UInt32
        byte[] accountName, // const char*
        int[] passwordLength, // UInt32*
        Pointer[] passwordData, // void**
        Pointer[] itemRef); // SecKeychaingItemRef*

    int // OSStatus
        SecKeychainAddGenericPassword(
        Pointer keychain, // SecKeychainRef
        int serviceNameLength, // UInt32
        byte[] serviceName, // const char*
        int accountNameLength, // UInt32
        byte[] accountName, // const char*
        int passwordLength, // UInt32
        byte[] passwordData, // const void*
        Pointer itemRef); // SecKeychainItemRef*

    int // OSStatus
        SecKeychainItemModifyContent(
        Pointer itemRef, // SecKeychainItemRef
        Pointer attrList, // const SecKeychainAttributeList*
        int length, // UInt32
        byte[] data); // const void*

    int // OSStatus
        SecKeychainItemDelete(Pointer itemRef); // SecKeychainItemRef

    int // OSStatus
        SecKeychainItemFreeContent(
        Pointer[] attrList, // SecKeychainAttributeList*
        Pointer data); // void*
  }

  // initialization-on-demand
  // https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom
  private static class LibManager {
    public static SecurityLibrary getInstance() {
      return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {
      private static final SecurityLibrary INSTANCE =
          Native.load("Security", SecurityLibrary.class);
    }
  }
}

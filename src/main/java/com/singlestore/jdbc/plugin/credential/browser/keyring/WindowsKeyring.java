// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.browser.keyring;

import com.singlestore.jdbc.plugin.credential.browser.ExpiringCredential;
import com.sun.jna.*;
import com.sun.jna.platform.win32.WinBase;
import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.win32.StdCallLibrary;
import com.sun.jna.win32.W32APIOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class WindowsKeyring implements Keyring {
  // corresponds to CRED_TYPE_GENERIC: generic credentials
  private static final int CREDENTIAL_TYPE = 1;
  // corresponds to CRED_PERSIST_LOCAL_MACHINE: persist on disk
  private static final int CREDENTIAL_PERSIST_TYPE = 2;

  private final Advapi32Lib advapi32;

  public WindowsKeyring() {
    advapi32 = LibManager.getInstance();
  }

  @Override
  public ExpiringCredential getCredential() {
    PointerByReference pCredential = new PointerByReference();
    try {
      boolean res;
      synchronized (advapi32) {
        res = advapi32.CredReadW(STORAGE_KEY, CREDENTIAL_TYPE, 0, pCredential);
      }

      if (!res) {
        logger.debug(
            "Failed to read from Windows Credential Manager or the key does not exist. Error code: "
                + Native.getLastError());
        return null;
      }

      WindowsCredential cred = new WindowsCredential(pCredential.getValue());

      if (cred.CredentialBlobSize == 0) {
        return null;
      }

      byte[] credBytes = cred.CredentialBlob.getByteArray(0, cred.CredentialBlobSize);
      try {
        return Keyring.fromBlob(new String(credBytes, StandardCharsets.UTF_16LE));
      } catch (IOException e) {
        logger.debug("Error while parsing cached token from the Windows Credential Manager", e);
        return null;
      }
    } finally {
      if (pCredential.getValue() != null) {
        synchronized (advapi32) {
          advapi32.CredFree(pCredential.getValue());
        }
      }
    }
  }

  @Override
  public void setCredential(ExpiringCredential cred) {
    byte[] credBlob = Keyring.makeBlob(cred).getBytes(StandardCharsets.UTF_16LE);
    Memory credBlobMem = new Memory(credBlob.length);
    credBlobMem.write(0, credBlob, 0, credBlob.length);

    WindowsCredential winCred = new WindowsCredential();
    winCred.Type = CREDENTIAL_TYPE;
    winCred.TargetName = new WString(STORAGE_KEY);
    winCred.CredentialBlobSize = (int) credBlobMem.size();
    winCred.CredentialBlob = credBlobMem;
    winCred.Persist = CREDENTIAL_PERSIST_TYPE;
    winCred.UserName = new WString(cred.getCredential().getUser());

    boolean res;
    synchronized (advapi32) {
      res = advapi32.CredWriteW(winCred, 0);
    }

    if (!res) {
      logger.debug(
          "Could not write to Windows Credential Manager. Error code: " + Native.getLastError());
    }
  }

  @Override
  public void deleteCredential() {
    boolean res;
    synchronized (advapi32) {
      res = advapi32.CredDeleteW(STORAGE_KEY, CREDENTIAL_TYPE, 0);
    }

    if (!res) {
      logger.debug(
          "Could not delete from Windows Credential Manager. Error code: " + Native.getLastError());
    }
  }

  interface Advapi32Lib extends StdCallLibrary {
    /**
     * BOOL BOOL CredReadW( [in] LPCWSTR TargetName, [in] DWORD Type, [in] DWORD Flags, [out]
     * PCREDENTIALW *Credential );
     */
    boolean CredReadW(String targetName, int type, int flags, PointerByReference pcred);

    /** BOOL CredWriteW( [in] PCREDENTIALW Credential, [in] DWORD Flags ); */
    boolean CredWriteW(WindowsCredential cred, int flags);

    /** BOOL CredDeleteW( [in] LPCWSTR TargetName, [in] DWORD Type, [in] DWORD Flags ); */
    boolean CredDeleteW(String targetName, int type, int flags);

    /** void CredFree( [in] PVOID Buffer ); */
    void CredFree(Pointer cred);
  }

  // initialization-on-demand
  // https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom
  private static class LibManager {
    public static Advapi32Lib getInstance() {
      return LazyHolder.INSTANCE;
    }

    private static class LazyHolder {
      private static final Advapi32Lib INSTANCE =
          Native.load("advapi32", Advapi32Lib.class, W32APIOptions.UNICODE_OPTIONS);
    }
  }

  public static class WindowsCredential extends Structure {
    public int Flags;
    public int Type;
    public WString TargetName;
    public WString Comment;
    public WinBase.FILETIME LastWritten;
    public int CredentialBlobSize;
    public Pointer CredentialBlob; // <== discussed below
    public int Persist;
    public int AttributeCount;
    public Pointer Attributes;
    public WString TargetAlias;
    public WString UserName;

    public WindowsCredential() {}

    public WindowsCredential(Pointer ptr) {
      // initialize from the raw memory block returned to us by ADVAPI32
      super(ptr);
      read();
    }

    @Override
    protected List<String> getFieldOrder() {
      return Arrays.asList(
          "Flags",
          "Type",
          "TargetName",
          "Comment",
          "LastWritten",
          "CredentialBlobSize",
          "CredentialBlob",
          "Persist",
          "AttributeCount",
          "Attributes",
          "TargetAlias",
          "UserName");
    }
  }
}

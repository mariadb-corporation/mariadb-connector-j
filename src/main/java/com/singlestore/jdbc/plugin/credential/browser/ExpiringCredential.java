// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.browser;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.singlestore.jdbc.plugin.Credential;
import java.time.Instant;

public class ExpiringCredential {
  // consider a token expired if it's expiration time < now() + EXPIRATION_OFFSET
  // since some time has to be spent to connect to DB before auth
  private static final long EXPIRATION_OFFSET_MILLISECONDS = 100;

  private final Instant expiration;
  private final Credential credential;
  private final String email;

  public ExpiringCredential(
      @JsonProperty("credential") Credential credential,
      @JsonProperty("email") String email,
      @JsonProperty("expiration") Instant expiration) {
    this.credential = credential;
    this.expiration = expiration;
    this.email = email;
  }

  @JsonIgnore
  public boolean isValid() {
    return expiration.isAfter(Instant.now().plusMillis(EXPIRATION_OFFSET_MILLISECONDS));
  }

  public Credential getCredential() {
    return credential;
  }

  public String getEmail() {
    return email;
  }

  // for serialization
  public Instant getExpiration() {
    return expiration;
  }
}

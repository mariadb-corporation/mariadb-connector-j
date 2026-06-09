// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.util;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.mariadb.jdbc.plugin.AuthDialogCallback;

/**
 * Test-only {@link AuthDialogCallback}: records every call and answers each prompt by popping a
 * canned response from {@link #responses}. Reset {@link #responses} and {@link #calls} between
 * tests.
 */
public class RecordingAuthDialogCallback implements AuthDialogCallback {

  /** Canned answers, consumed in FIFO order; empty ⇒ {@code prompt} returns {@code null}. */
  public static final Deque<String> responses = new ConcurrentLinkedDeque<>();

  /** Recorded calls in invocation order. */
  public static final List<Call> calls = new ArrayList<>();

  public static final class Call {
    public final boolean echo;
    public final String prompt;
    public final int round;

    Call(boolean echo, String prompt, int round) {
      this.echo = echo;
      this.prompt = prompt;
      this.round = round;
    }
  }

  @Override
  public String prompt(boolean echo, String prompt, int round) {
    calls.add(new Call(echo, prompt, round));
    return responses.pollFirst();
  }
}

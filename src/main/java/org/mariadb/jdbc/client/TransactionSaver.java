package org.mariadb.jdbc.client;

import java.util.ArrayList;
import java.util.List;
import org.mariadb.jdbc.message.client.RedoableClientMessage;

public class TransactionSaver {
  private final List<RedoableClientMessage> buffers = new ArrayList<>();
  private transient boolean cleanState = true;

  public void add(RedoableClientMessage clientMessage) {
    buffers.add(clientMessage);
  }

  public void dirty() {
    buffers.clear();
    cleanState = false;
  }

  public void clear() {
    buffers.clear();
    cleanState = true;
  }

  public List<RedoableClientMessage> getBuffers() {
    return buffers;
  }

  public boolean isCleanState() {
    return cleanState;
  }
}

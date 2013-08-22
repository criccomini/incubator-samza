package org.apache.samza.system;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestPriorityChooser {
  @Test
  public void testPriorityChooser() {
    MockPriorityChooser chooser = new MockPriorityChooser();
    IncomingMessageEnvelope envelope1 = new IncomingMessageEnvelope(null, null, null, 1);
    IncomingMessageEnvelope envelope2 = new IncomingMessageEnvelope(null, null, null, 2);
    IncomingMessageEnvelope envelope3 = new IncomingMessageEnvelope(null, null, null, 3);

    assertEquals(null, chooser.choose());

    // Test one message.
    chooser.setPriority(1);
    chooser.update(envelope1);
    assertEquals(envelope1, chooser.choose());
    assertEquals(null, chooser.choose());

    // Test multiple and duplicate out of order messages.
    chooser.setPriority(1);
    chooser.update(envelope1);
    chooser.setPriority(0);
    chooser.update(envelope3);
    chooser.setPriority(-1);
    chooser.update(envelope2);
    chooser.setPriority(0);
    chooser.update(envelope3);

    assertEquals(envelope1, chooser.choose());
    assertEquals(envelope3, chooser.choose());
    assertEquals(envelope3, chooser.choose());
    assertEquals(envelope2, chooser.choose());
    assertEquals(null, chooser.choose());
  }

  public static class MockPriorityChooser extends PriorityChooser {
    private int priority;

    public MockPriorityChooser() {
      this.priority = 0;
    }

    public void setPriority(int priority) {
      this.priority = priority;
    }

    @Override
    protected double prioritize(IncomingMessageEnvelope envelope) {
      return priority;
    }
  }
}

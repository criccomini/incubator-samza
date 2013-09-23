package org.apache.samza.system;

/**
 * An abstract MessageChooser that implements start/stop/register for choosers
 * that don't use them.
 */
abstract public class BaseMessageChooser implements MessageChooser {
  public void start() {
  }

  public void stop() {
  }

  public void register(SystemStreamPartition systemStreamPartition, String lastReadOffset) {
  }
}

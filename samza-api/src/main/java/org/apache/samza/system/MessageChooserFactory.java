package org.apache.samza.system;

import org.apache.samza.config.Config;

public interface MessageChooserFactory {
  MessageChooser getChooser(Config config);
}

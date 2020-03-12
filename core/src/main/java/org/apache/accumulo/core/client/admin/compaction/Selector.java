package org.apache.accumulo.core.client.admin.compaction;

import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;

/**
 * Compaction file selector
 */
public interface Selector {

  public interface InitParamaters {
    Map<String,String> getOptions();

    PluginEnvironment getEnvironment();
  }

  void init(InitParamaters iparams);

  public interface SelectionParameters {
    PluginEnvironment getEnvironment();
  }

  Selection select(SelectionParameters sparams);
}

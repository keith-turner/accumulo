package org.apache.accumulo.core.client.admin.compaction;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;

/**
 * Enables dynamically overriding of configuration used to create the output file for a compaction.
 * 
 * @since 2.1.0
 */
public interface CompactionConfigurer {
  /**
   * @since 2.1.0
   */
  public interface InitParamaters {
    Map<String,String> getOptions();

    PluginEnvironment getEnvironment();
  }

  void init(InitParamaters iparams);

  /**
   * @since 2.1.0
   */
  public interface InputParameters {
    public Collection<TabletFileInfo> getInputFiles();

    PluginEnvironment getEnvironment();
  }

  /**
   * Specifies how the output file should be created for a compaction.
   * 
   * @since 2.1.0
   */
  public class Overrides {
    // TODO builder
    public Map<String,String> tabletPropertyOverrides;
  }

  Overrides override(InputParameters params);
}

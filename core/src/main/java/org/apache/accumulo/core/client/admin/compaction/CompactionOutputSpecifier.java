package org.apache.accumulo.core.client.admin.compaction;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;

/**
 * Compaction file output specification factory.
 * 
 * @since 2.1.0
 */
public interface CompactionOutputSpecifier {
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
  public class Specifications {
    // TODO builder
    public Map<String,String> tabletPropertyOverrides;
  }

  Specifications specify(InputParameters params);
}

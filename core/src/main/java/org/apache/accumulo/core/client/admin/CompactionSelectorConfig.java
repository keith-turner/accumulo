package org.apache.accumulo.core.client.admin;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 2.1.0
 */
public class CompactionSelectorConfig {
  private String className;
  private Map<String,String> options = Collections.emptyMap();

  /**
   * @param className
   *          The name of a class that implements
   *          org.apache.accumulo.tserver.compaction.CompactionStrategy. This class must be exist on
   *          tservers.
   */
  public CompactionSelectorConfig(String className) {
    requireNonNull(className);
    this.className = className;
  }

  /**
   * @return the class name passed to the constructor.
   */
  public String getClassName() {
    return className;
  }

  /**
   * @param opts
   *          The options that will be passed to the init() method of the compaction strategy when
   *          its instantiated on a tserver. This method will copy the map. The default is an empty
   *          map.
   * @return this
   */
  public CompactionSelectorConfig setOptions(Map<String,String> opts) {
    requireNonNull(opts);
    this.options = new HashMap<>(opts);
    return this;
  }

  /**
   * @return The previously set options. Returns an unmodifiable map. The default is an empty map.
   */
  public Map<String,String> getOptions() {
    return Collections.unmodifiableMap(options);
  }

  @Override
  public int hashCode() {
    return className.hashCode() + options.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CompactionSelectorConfig) {
      CompactionSelectorConfig ocsc = (CompactionSelectorConfig) o;
      return className.equals(ocsc.className) && options.equals(ocsc.options);
    }

    return false;
  }
}

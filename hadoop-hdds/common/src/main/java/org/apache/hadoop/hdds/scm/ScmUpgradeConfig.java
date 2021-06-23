package org.apache.hadoop.hdds.scm;


import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

import java.time.Duration;

/**
 * Class to track upgrade related SCM configs.
 */
@ConfigGroup(prefix = "hdds.scm")
public class ScmUpgradeConfig {

  @Config(key = "upgrade.finalization.ratis.based.timeout",
      defaultValue = "30s",
      type = ConfigType.TIME,
      tags = {ConfigTag.SCM, ConfigTag.UPGRADE},
      description = "Maximum time to wait for a slow follower to be finalized" +
          " through a Ratis snapshot. This is an advanced config, and needs " +
          "to be changed only under a special circumstance when the leader " +
          "SCM has purged the finalize request from its logs, and a follower" +
          "SCM was down during upgrade finalization. Default is 30s."
  )
  private long ratisBasedFinalizationTimeout =
      Duration.ofSeconds(30).getSeconds();

  @Config(key = "init.default.layout.version",
      defaultValue = "-1",
      type = ConfigType.INT,
      tags = {ConfigTag.SCM, ConfigTag.UPGRADE},
      description = "Default Layout Version to init the SCM with. Intended to "
          + "be used in tests to finalize from an older version of SCM to the "
          + "latest. By default, SCM init uses the highest layout version."
  )
  private int defaultLayoutVersionOnInit = -1;

  /**
   * Config key class.
   */
  public static class ConfigStrings {
    public static final String HDDS_SCM_INIT_DEFAULT_LAYOUT_VERSION =
        "hdds.scm.init.default.layout.version";

    public static final String
        HDDS_SCM_UPGRADE_FINALIZATION_RATIS_BASED_TIMEOUT =
        "hdds.scm.upgrade.finalization.ratis.based.timeout";
  }

  public long getRatisBasedFinalizationTimeout() {
    return ratisBasedFinalizationTimeout;
  }

  public void setRatisBasedFinalizationTimeout(long timeout) {
    this.ratisBasedFinalizationTimeout = timeout;
  }

  public int getDefaultLayoutVersionOnInit() {
    return defaultLayoutVersionOnInit;
  }

  public void setDefaultLayoutVersionOnInit(int defaultLayoutVersionOnInit) {
    this.defaultLayoutVersionOnInit = defaultLayoutVersionOnInit;
  }


}

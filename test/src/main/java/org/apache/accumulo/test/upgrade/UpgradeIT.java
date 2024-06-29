package org.apache.accumulo.test.upgrade;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.harness.WithTestNames;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;

import static org.apache.accumulo.harness.AccumuloITBase.createTestDir;

public class UpgradeIT extends WithTestNames {
    @Test
    public void testCleanUpgrade() throws Exception {
        // TODO this test is doing a bunch of odd sh*t, so comment it well
        File baseDir = createTestDir(this.getClass().getName() + "_" + this.testName());

        FileUtils.deleteQuietly(baseDir);
        File prevMac = new File(baseDir, "prev-mac");
        FileUtils.copyDirectory(UpgradeGenerateIT.MAC_DIR_CLEAN, prevMac);


        File csFile = new File(prevMac, "conf/hdfs-site.xml");
        Configuration hadoopSite = new Configuration();
        hadoopSite.set("fs.defaultFS", "file:///");
        try(OutputStream out = new BufferedOutputStream(new FileOutputStream(csFile.getAbsolutePath()))) {
            hadoopSite.writeXml(out);
        }

        MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(new File(baseDir, "mac"), UpgradeGenerateIT.ROOT_PASSWORD);
        config.useExistingInstance(new File(prevMac, "conf/accumulo.properties"), new File(prevMac,"conf"));

        var cluster = new MiniAccumuloClusterImpl(config);

        cluster._exec(cluster.getConfig().getServerClass(ServerType.ZOOKEEPER), ServerType.ZOOKEEPER, Map.of(), new File(prevMac, "conf/zoo.cfg").getAbsolutePath());

        // TODO started processes continue to run
        cluster.start();

        try(var client = Accumulo.newClient().from(cluster.getClientProperties()).build()){
            try(var scanner = client.createScanner("test")) {
                scanner.forEach(System.out::println);
            }
        }
    }
}

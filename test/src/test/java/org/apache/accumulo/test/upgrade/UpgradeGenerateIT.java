package org.apache.accumulo.test.upgrade;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Objects;

import static org.apache.accumulo.core.conf.Property.GENERAL_PROCESS_BIND_ADDRESS;

public class UpgradeGenerateIT {

    public static final String BASE_DIR = "/tmp/7a0c897a-57ed-4114-8ea8-2a6b1d1c9df9";
    public static final File MAC_DIR_CLEAN = new File(BASE_DIR+"/mac_clean");
    public static final String ROOT_PASSWORD = "979d55ae-98fd-4d22-9b1c-2d5723546a5e";

    @Test
    public void generateAccumuloZip() throws Exception {
        FileUtils.deleteQuietly(MAC_DIR_CLEAN);

        MiniAccumuloConfigImpl config = new MiniAccumuloConfigImpl(MAC_DIR_CLEAN, ROOT_PASSWORD);
        config.setProperty(GENERAL_PROCESS_BIND_ADDRESS, "localhost");
        MiniAccumuloClusterImpl cluster = new MiniAccumuloClusterImpl(config);
        Configuration haddopConfig = new Configuration(false);
        haddopConfig.set("fs.file.impl", RawLocalFileSystem.class.getName());
        File csFile = new File(Objects.requireNonNull(config.getConfDir()), "core-site.xml");
        try(OutputStream out = new BufferedOutputStream(new FileOutputStream(csFile.getAbsolutePath()))) {
            haddopConfig.writeXml(out);
        }

        cluster.start();

        try (AccumuloClient c = Accumulo.newClient().from(cluster.getClientProperties()).build()) {
            c.tableOperations().create("test");
            try(var writer = c.createBatchWriter("test")) {
                var mutation = new Mutation("0");
                mutation.put("f","q","v");
                writer.addMutation(mutation);
            }

           cluster.getClusterControl().adminStopAll();

            var dir = cluster.getConfig().getDir();
            System.out.println(dir);

            Files.walk(dir.toPath()).forEach(path->System.out.println(path));
        }finally {
            cluster.stop();
        }
    }
}

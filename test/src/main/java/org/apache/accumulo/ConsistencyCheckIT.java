package org.apache.accumulo;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ConsistencyCheckIT extends ConfigurableMacBase {

    @Override
    public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
        cfg.setProperty(Property.TSERV_HEALTH_CHECK_FREQ, "5s");
    }

    private static final Logger log = LoggerFactory.getLogger(ConsistencyCheckIT.class);

    @Test
    public void testConsistencyCheck() throws Exception {
        final String table = this.getUniqueNames(1)[0];

        try(var client = Accumulo.newClient().from(getClientProperties()).build()) {
            NewTableConfiguration ntc = new NewTableConfiguration();
            ntc.setProperties(Map.of(Property.TABLE_OPERATION_LOG_MAX_SIZE.getKey(), "100"));
            client.tableOperations().create(table, ntc);

            for(int i = 0; i< 11;i++) {
                try(var writer = client.createBatchWriter(table)) {
                    Mutation m = new Mutation("r"+i);
                    m.put("data","time", ""+System.currentTimeMillis());
                    writer.addMutation(m);
                }

                client.tableOperations().flush(table,null,null,true);

            }

            client.securityOperations().grantTablePermission("root", "accumulo.metadata", TablePermission.WRITE);

            var tableId = TableId.of(client.tableOperations().tableIdMap().get(table));
            var extent = new KeyExtent(tableId, null,null);

            var tabletMetadata = getServerContext().getAmple().readTablet(extent);

            var someFile = tabletMetadata.getFiles().iterator().next();

            var mutator = getServerContext().getAmple().mutateTablet(extent);
            mutator.deleteFile(someFile);
            mutator.mutate();

            // give the consistency check a chance to run
            UtilWaitThread.sleep(30000);

        }
    }
}

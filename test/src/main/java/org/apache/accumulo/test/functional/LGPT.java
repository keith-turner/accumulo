package org.apache.accumulo.test.functional;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.jupiter.api.Test;

public class LGPT extends AccumuloClusterHarness {
    @Test
    public void testLgp() throws Exception {
        try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
            client.tableOperations().create("lgpt");

            try(var writer = client.createBatchWriter("lgpt")){
                for(int i = 0; i< 1_000_000; i++) {
                    Mutation m = new Mutation(String.format("%06x", i));
                    m.put("fam1", );
                }
            }


        }
    }
}

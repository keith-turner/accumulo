package org.apache.accumulo.manager.tableOps.split;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;

import org.apache.hadoop.io.Text;

public class AddNewTablet extends ManagerRepo {

    private final KeyExtent expectedExtent;
    private final Text split;
    private final String dirName;

    public AddNewTablet(KeyExtent expectedExtent, Text split, String dirName) {
        this.expectedExtent = expectedExtent;
        this.split = split;
        this.dirName = dirName;
    }

    @Override
    public Repo<Manager> call(long tid, Manager manager) throws Exception {
        TabletMetadata tabletMetadata = manager.getContext().getAmple().readTablet(expectedExtent);

        var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);
        Preconditions.checkState(tabletMetadata != null && tabletMetadata.getOperationId().equals(opid));

        KeyExtent newExtent = new KeyExtent(expectedExtent.tableId(), split, expectedExtent.prevEndRow());

        try(var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()){

            var mutator = tabletsMutator.mutateTablet(newExtent).requireAbsentTablet();

            mutator.putOperation(opid);
            mutator.putDirName(dirName);
            mutator.putTime(tabletMetadata.getTime());
            tabletMetadata.getFlushId().ifPresent(mutator::putFlushId);
            mutator.putPrevEndRow(newExtent.prevEndRow());
            tabletMetadata.getCompactId().ifPresent(mutator::putCompactionId);
            mutator.putHostingGoal(tabletMetadata.getHostingGoal());

            //TODO suspend??

            tabletMetadata.getLoaded().forEach(mutator::putBulkFile);
            tabletMetadata.getLogs().forEach(mutator::putWal);

            // TODO determine which files go to where
            tabletMetadata.getFilesMap().forEach((f,v)->{
                // TODO use floats?
                mutator.putFile(f, new DataFileValue(v.getSize()/2, v.getNumEntries()/2, v.getTime()));
            });

            mutator.submit(afterMeta->afterMeta != null && afterMeta.getOperationId() != null && afterMeta.getOperationId().equals(opid));

            // not expecting this to fail
            // TODO message with context if it does
            Preconditions.checkState(tabletsMutator.process().get(expectedExtent).getStatus() == ConditionalWriter.Status.ACCEPTED);
        }


        return null;
    }
}

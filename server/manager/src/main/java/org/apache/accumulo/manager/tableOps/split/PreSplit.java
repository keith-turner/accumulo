package org.apache.accumulo.manager.tableOps.split;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

public class PreSplit extends ManagerRepo {

    private static final Logger log = LoggerFactory.getLogger(PreSplit.class);

    private final KeyExtent expectedExtent;
    private final Text split;

    PreSplit(KeyExtent expectedExtent, Text split) {
        this.expectedExtent = Objects.requireNonNull(expectedExtent);
        this.split = Objects.requireNonNull(split);
        Preconditions.checkArgument(expectedExtent.contains(split));
    }

    @Override
    public long isReady(long tid, Manager manager) throws Exception {

        TabletMetadata tabletMetadata = manager.getContext().getAmple().readTablet(expectedExtent, ColumnType.PREV_ROW, ColumnType.LOCATION, ColumnType.OPID);

        if(tabletMetadata == null) {
            // TODO tablet does not exists, how to best handle?
            throw new NullPointerException();
        }

        var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);

        if(tabletMetadata.getLocation() != null) {
            //TODO ensure this removed
            log.debug("{} waiting for unassignment {}", FateTxId.formatTid(tid), tabletMetadata.getLocation());
            manager.requestUnassignment(expectedExtent, tid);
            return 1000;
        }

        if(tabletMetadata.getOperationId() != null){
            if(tabletMetadata.getOperationId().equals(opid)) {
                log.trace("{} already set operation id", FateTxId.formatTid(tid));
                return 0;
            } else {
                log.debug("{} can not split, another operation is active {}", FateTxId.formatTid(tid), tabletMetadata.getOperationId());
                return 1000;
            }
        } else {
            try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {
                tabletsMutator.mutateTablet(expectedExtent).requireAbsentOperation().requireAbsentLocation().requirePrevEndRow(expectedExtent.prevEndRow()).putOperation(opid).submit(tmeta-> tmeta.getOperationId() != null && tmeta.getOperationId().equals(opid));

                Map<KeyExtent, Ample.ConditionalResult> results = tabletsMutator.process();

                if(results.get(expectedExtent).getStatus() == ConditionalWriter.Status.ACCEPTED) {
                    // TODO change to trace
                    log.debug("{} reserved {} for split", FateTxId.formatTid(tid), expectedExtent);
                    return 0;
                } else {
                    tabletMetadata = results.get(expectedExtent).readMetadata();

                    log.debug("{} Failed to set operation id. extent:{} location:{} opid:{}", FateTxId.formatTid(tid), expectedExtent, tabletMetadata.getLocation(), tabletMetadata.getOperationId());

                    // TODO write IT that spins up 100 threads that all try to add a diff split to the same tablet

                    return 1000;
                }
            }
        }
    }

    @Override
    public Repo<Manager> call(long tid, Manager manager) throws Exception {
        // ELASTICITY_TODO need to make manager ignore tablet with an operation id for assignment purposes
        manager.cancelUnassignmentRequest(expectedExtent, tid);

        // Create the dir name here for the next step. If the next step fails it will always have the same dir name each time it runs again making it idempotent.
        String dirName = UniqueNameAllocator.createTabletDirectoryName(manager.getContext(), split);

        return new AddNewTablet(expectedExtent, split, dirName);
    }

    @Override
    public void undo(long tid, Manager manager) throws Exception {
        //TODO is this called if isReady fails?
        manager.cancelUnassignmentRequest(expectedExtent, tid);
    }
}

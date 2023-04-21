package org.apache.accumulo.manager.tableOps.bulkVer2;

import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;

public class RefreshTablets extends ManagerRepo {

    private final BulkInfo bulkInfo;

    public RefreshTablets(BulkInfo bulkInfo) {
        this.bulkInfo = bulkInfo;
    }


    public long isReady(long tid, Manager manager) throws Exception {
        //TODO check consistency?
        //TODO limit tablets scanned to range of bulk import extents
        var tablets = manager.getContext().getAmple().readTablets().forTable(bulkInfo.tableId).fetch(ColumnType.LOCATION, ColumnType.PREV_ROW).build();

        int refreshRequestSent = 0;


        for (TabletMetadata tablet : tablets) {
            if(tablet.getRefreshId().contains(tid)) {
                //TODO make refresh call
                refreshRequestSent++;
            }
        }

        if(refreshRequestSent > 0) {
            return 1000;
        } else {
            return 0;
        }
    }

    @Override
    public Repo<Manager> call(long tid, Manager environment) throws Exception {
        return new CompleteBulkImport(bulkInfo);
    }
}

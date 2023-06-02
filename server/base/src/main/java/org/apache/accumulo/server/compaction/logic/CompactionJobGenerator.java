package org.apache.accumulo.server.compaction.logic;

import com.beust.jcommander.converters.IParameterSplitter;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class CompactionJobGenerator {

    Map<CompactionServiceId, List<CompactionJob>> generateJobs(CompactionKind kind, TabletMetadata tablet) {

        CompactionServiceId serviceId = dispatch(kind, tablet);

        Collection<CompactionJob> jobs =  planCompactions(serviceId, kind, tablet);

        return null;
    }



    private CompactionServiceId dispatch(CompactionKind kind, TabletMetadata tablet) {
        return null;
    }

    private Collection<CompactionJob> planCompactions(CompactionServiceId serviceId, CompactionKind kind, TabletMetadata tablet) {

        CompactionPlanner planner = null;

        // selecting indicator
        // selected files


        CompactionPlanner.PlanningParameters params = new CompactionPlanner.PlanningParameters() {
            @Override
            public TableId getTableId() {
                return tablet.getTableId();
            }

            @Override
            public ServiceEnvironment getServiceEnvironment() {
                return null;
            }

            @Override
            public CompactionKind getKind() {
                return kind;
            }

            @Override
            public double getRatio() {
                return 0;
            }

            @Override
            public Collection<CompactableFile> getAll() {
                return null;
            }

            @Override
            public Collection<CompactableFile> getCandidates() {
                Set<StoredTabletFile> allTabletFiles = null;
                // files selected for a user compaction
                Set<StoredTabletFile> selectedFiles = null;
                //files
                Set<StoredTabletFile> compactingFiles = null;

                return null;
            }

            @Override
            public Collection<CompactionJob> getRunningCompactions() {

                return null;
            }

            @Override
            public Map<String, String> getExecutionHints() {
                return null;
            }

            @Override
            public CompactionPlan.Builder createPlanBuilder() {
                return null;
            }
        };

        planner.makePlan(params);

        return null;
    }
}

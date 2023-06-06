package org.apache.accumulo.server.compaction.queue;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkState;

public class CompactionJobQueue {

    private class Entry implements Comparable<Entry> {
        private final CompactionJob job;

        private final long seq;
        private final KeyExtent extent;

        Entry(CompactionJob job, KeyExtent extent) {
            this.job = job;
            this.seq = nextSeq++;
            this.extent = extent;
        }

        @Override
        public int compareTo(Entry oe) {
            // extent is intentionally not part of comparison.  The extent is used to find the entry in the tabletJobs map.
            int cmp = CompactionJobPrioritizer.JOB_COMPARATOR.compare(this.job, oe.job);
            if(cmp == 0) {
                cmp = Long.compare(seq, oe.seq);
            }
            return cmp;
        }
    }

    private final TreeSet<Entry> jobQueue;
    private final int maxSize;

    private final Map<KeyExtent, List<Entry>> tabletJobs;

    private long nextSeq;

    public CompactionJobQueue(int maxSize) {
        this.jobQueue = new TreeSet<>();
        this.maxSize = maxSize;
        this.tabletJobs = new HashMap<>();
    }

    public synchronized void add(KeyExtent extent, Collection<CompactionJob> jobs) {
        // this is important for reasoning about set operations
        Preconditions.checkArgument(jobs.stream().allMatch(job-> job instanceof CompactionJobImpl));

        jobs = reconcileWithPrevSubmissions(extent, jobs);

        List<Entry> newEntries = new ArrayList<>(jobs.size());

        for(CompactionJob job : jobs) {
           Entry entry = addJobToQueue(extent, job);
           if(entry != null) {
               newEntries.add(entry);
           }
        }

        if(!newEntries.isEmpty()) {
            checkState(tabletJobs.put(extent, newEntries) == null);
        }

    }

    public synchronized CompactionJob poll(){
        Entry first = jobQueue.pollFirst();

        if(first != null) {
            List<Entry> jobs = tabletJobs.get(first.extent);
            checkState(jobs.remove(first));
            if(jobs.isEmpty()) {
                tabletJobs.remove(first.extent);
            }
        }

        return first == null ? null : first.job;
    }



    private Collection<CompactionJob> reconcileWithPrevSubmissions(KeyExtent extent, Collection<CompactionJob> jobs) {
        List<Entry> prevJobs = tabletJobs.get(extent);
        if(prevJobs != null) {
            // TODO instead of removing everything, attempt to detect if old and new jobs are the same
            prevJobs.forEach(jobQueue::remove);
            tabletJobs.remove(extent);
        }
        return jobs;
    }

    private Entry addJobToQueue(KeyExtent extent, CompactionJob job) {
        if(jobQueue.size() >= maxSize) {
            var lastEntry = jobQueue.last();
            if(job.getPriority() <= lastEntry.job.getPriority()) {
                // the queue is full and this job has a lower or same priority than the lowest job in the queue, so do not add it
                return null;
            } else {
                // the new job has a higher priority than the lowest job in the queue, so remove the lowest
                jobQueue.pollLast();
            }

        }

        var entry = new Entry(job, extent);
        jobQueue.add(entry);
        return entry;
    }
}

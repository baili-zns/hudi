package org.apache.hudi.sink.compact;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.client.WriteStatus;

import java.util.List;

/**
 * @program: hudi
 * @description:
 * @author: zns
 * @create: 2022-01-24 17:00
 */
public class CompactionCommitEventEx extends CompactionCommitEvent {
    private Configuration configuration;

    public CompactionCommitEventEx() {
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public CompactionCommitEventEx(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * An event with NULL write statuses that represents a failed compaction.
     *
     * @param instant
     * @param fileId
     * @param taskID
     */
    public CompactionCommitEventEx(String instant, String fileId, int taskID, Configuration configuration) {
        super(instant, fileId, taskID);
        this.configuration = configuration;
    }

    public CompactionCommitEventEx(String instant, String fileId, List<WriteStatus> writeStatuses, int taskID, Configuration configuration) {
        super(instant, fileId, writeStatuses, taskID);
        this.configuration = configuration;
    }
}
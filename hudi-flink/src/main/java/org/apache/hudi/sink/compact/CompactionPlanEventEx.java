package org.apache.hudi.sink.compact;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.common.model.CompactionOperation;

/**
 * @program: hudi
 * @description:
 * @author: zns
 * @create: 2022-01-24 16:59
 */
public class CompactionPlanEventEx extends CompactionPlanEvent{
    private Configuration configuration;

    public CompactionPlanEventEx() {
    }

    public CompactionPlanEventEx(Configuration configuration) {
        this.configuration = configuration;
    }

    public CompactionPlanEventEx(String instantTime, CompactionOperation operation, Configuration configuration) {
        super(instantTime, operation);
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}
package org.apache.hudi.sink.event;

import cn.hutool.core.util.ReflectUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.ValidationUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @program: econ-analysis
 * @description:
 * @author: zns
 * @create: 2022-01-20 15:10
 */
public class WriteMetadataEventEx extends WriteMetadataEvent {
    private RowType rowType;
    private List<String> primaryKeyColumnNames;


    public WriteMetadataEventEx(
            int taskID,
            String instantTime,
            List<WriteStatus> writeStatuses,
            boolean lastBatch,
            boolean endInput,
            boolean bootstrap) {
        ReflectUtil.setFieldValue(this, "taskID", taskID);
        ReflectUtil.setFieldValue(this, "instantTime", instantTime);
        ReflectUtil.setFieldValue(this, "writeStatuses", writeStatuses);
        ReflectUtil.setFieldValue(this, "lastBatch", lastBatch);
        ReflectUtil.setFieldValue(this, "endInput", endInput);
        ReflectUtil.setFieldValue(this, "bootstrap", bootstrap);
    }

    public WriteMetadataEventEx(
            int taskID,
            String instantTime,
            List<WriteStatus> writeStatuses,
            boolean lastBatch,
            boolean endInput,
            boolean bootstrap,
            RowType rowType,
            List<String> primaryKeyColumnNames) {
        this(taskID, instantTime, writeStatuses, lastBatch, endInput, bootstrap);
        this.rowType = rowType;
        this.primaryKeyColumnNames = primaryKeyColumnNames;
    }

    // default constructor for efficient serialization
    public WriteMetadataEventEx() {
    }


    public RowType getRowType() {
        return rowType;
    }

    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    public List<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }

    public void setPrimaryKeyColumnNames(List<String> primaryKeyColumnNames) {
        this.primaryKeyColumnNames = primaryKeyColumnNames;
    }

    /**
     * Merges this event with given {@link WriteMetadataEventEx} {@code other}.
     *
     * @param other The event to be merged
     */
    public void mergeWith(WriteMetadataEventEx other) {
        ValidationUtils.checkArgument(this.getTaskID() == other.getTaskID());
        // the instant time could be monotonically increasing
        this.setInstantTime(other.getInstantTime());
        this.setLastBatch(this.isLastBatch() | other.isLastBatch()); // true if one of the event lastBatch is true
        List<WriteStatus> statusList = new ArrayList<>();
        statusList.addAll(this.getWriteStatuses());
        statusList.addAll(other.getWriteStatuses());
        this.setWriteStatuses(statusList);
    }


    // -------------------------------------------------------------------------
    //  Utilities
    // -------------------------------------------------------------------------

    /**
     * Creates empty bootstrap event for task {@code taskId}.
     *
     * <p>The event indicates that the new instant can start directly,
     * there is no old instant write statuses to recover.
     */
    public static WriteMetadataEventEx emptyBootstrap(int taskId) {
        return new WriteMetadataEventEx(taskId, BOOTSTRAP_INSTANT, Collections.emptyList(), false, false, true);
    }

}

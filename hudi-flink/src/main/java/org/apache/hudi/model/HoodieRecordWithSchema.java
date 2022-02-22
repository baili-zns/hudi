package org.apache.hudi.model;

import com.hito.econ.flink.common.model.RowDataWithSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.*;

import java.util.List;

/**
 * @program: econ-analysis
 * @description:
 * @author: zns
 * @create: 2022-01-19 16:09
 */
public class HoodieRecordWithSchema<T extends HoodieRecordPayload> extends HoodieAvroRecord {
    private RowType rowType;
    private List<String> primaryKeyColumnNames;


    public HoodieRecordWithSchema() {
    }

    public HoodieRecordWithSchema(HoodieKey key, T data, HoodieOperation operation, RowDataWithSchema rowDataWithSchema) {
        super(key, data, operation);
        this.rowType = rowDataWithSchema.getRowType();
        this.primaryKeyColumnNames = rowDataWithSchema.getPrimaryKeyColumnNames();
    }

    public HoodieRecordWithSchema(HoodieKey key, T data, HoodieOperation operation, RowType rowType) {
        super(key, data, operation);
        this.rowType = rowType;
    }

    public HoodieRecordWithSchema(HoodieKey key, HoodieRecordPayload data, RowType rowType, List<String> primaryKeyColumnNames) {
        super(key, data);
        this.rowType = rowType;
        this.primaryKeyColumnNames = primaryKeyColumnNames;
    }

    public HoodieRecordWithSchema(HoodieKey key, HoodieRecordPayload data, HoodieOperation operation, RowType rowType, List<String> primaryKeyColumnNames) {
        super(key, data, operation);
        this.rowType = rowType;
        this.primaryKeyColumnNames = primaryKeyColumnNames;
    }

    public HoodieRecordWithSchema(HoodieRecord record, RowType rowType, List<String> primaryKeyColumnNames) {
        super(record);
        this.rowType = rowType;
        this.primaryKeyColumnNames = primaryKeyColumnNames;
    }

    public HoodieRecordWithSchema(RowType rowType, List<String> primaryKeyColumnNames) {
        this.rowType = rowType;
        this.primaryKeyColumnNames = primaryKeyColumnNames;
    }

    public HoodieRecordWithSchema(HoodieRecord<HoodieRecordPayload> hoodieRecord, RowType rowType) {
        super(hoodieRecord);
        this.rowType = rowType;
    }

    public HoodieRecordWithSchema(HoodieRecord<HoodieRecordPayload> hoodieRecord) {
        super(hoodieRecord);
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

}
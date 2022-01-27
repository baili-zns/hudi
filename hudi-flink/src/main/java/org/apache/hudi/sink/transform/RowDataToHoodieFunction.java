/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.transform;

import cn.hutool.core.util.StrUtil;
import com.hito.econ.flink.common.model.RowDataWithSchema;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.model.HoodieRecordWithSchema;
import org.apache.hudi.sink.utils.PayloadCreation;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.util.StreamerUtil.flinkConf2TypedProperties;

/**
 * Function that transforms RowData to HoodieRecord.
 */
public class RowDataToHoodieFunction<I extends RowData, O extends HoodieRecord>
        extends RichMapFunction<I, O> {
    /**
     * Row type of the input.
     */
    private RowType rowType;
    private List<String> primaryKeyColumnNames;

    /**
     * Avro schema of the input.
     */
    private transient Schema avroSchema;

    /**
     * RowData to Avro record converter.
     */
    private transient RowDataToAvroConverters.RowDataToAvroConverter converter;

    /**
     * HoodieKey generator.
     */
    private transient KeyGenerator keyGenerator;

    /**
     * Utilities to create hoodie pay load instance.
     */
    private transient PayloadCreation payloadCreation;

    /**
     * Config options.
     */
    private final Configuration config;

    public RowDataToHoodieFunction(RowType rowType, Configuration config) {
        this.rowType = rowType;
        this.config = config;
    }

    private static final Logger LOG = LoggerFactory.getLogger(RowDataToHoodieFunction.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            this.avroSchema = StreamerUtil.getSourceSchema(this.config);
            this.converter = RowDataToAvroConverters.createConverter(this.rowType);
            this.keyGenerator =
                    HoodieAvroKeyGeneratorFactory
                            .createKeyGenerator(flinkConf2TypedProperties(FlinkOptions.flatOptions(this.config)));
            this.payloadCreation = PayloadCreation.instance(config);
        } catch (HoodieException e) {
            LOG.info("初始化writeClient失败,稍后根据数据进行初始化");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public O map(I i) throws Exception {
        return (O) toHoodieRecord(i);
    }

    /**
     * Converts the give record to a {@link HoodieRecord}.
     *
     * @param record The input record
     * @return HoodieRecord based on the configuration
     * @throws IOException if error occurs
     */
    @SuppressWarnings("rawtypes")
    private HoodieRecord toHoodieRecord(I record) throws Exception {
        HoodieOperation operation = HoodieOperation.fromValue(record.getRowKind().toByteValue());
        if (record instanceof RowDataWithSchema) {
            RowDataWithSchema rowDataWithSchema = (RowDataWithSchema) record;
            RowType rowType1 = rowDataWithSchema.getRowType();
            List primaryKeyColumnNames1 = rowDataWithSchema.getPrimaryKeyColumnNames();
            if (!rowType1.equals(this.rowType) || !primaryKeyColumnNames1.equals(this.primaryKeyColumnNames)) {
                this.rowType = rowDataWithSchema.getRowType();
                this.avroSchema = AvroSchemaConverter.convertToSchema(this.rowType);
                this.converter = RowDataToAvroConverters.createConverter(this.rowType);
                this.config.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, this.avroSchema.toString());
                this.config.setString(FlinkOptions.RECORD_KEY_FIELD, StrUtil.join(",", primaryKeyColumnNames1));
                this.keyGenerator =
                        HoodieAvroKeyGeneratorFactory
                                .createKeyGenerator(flinkConf2TypedProperties(FlinkOptions.flatOptions(this.config)));
                this.payloadCreation = PayloadCreation.instance(this.config);
                GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);
                final HoodieKey hoodieKey = keyGenerator.getKey(gr);
                HoodieRecordPayload payload = payloadCreation.createPayload(gr);
                return new HoodieRecordWithSchema(hoodieKey, payload, operation, (RowDataWithSchema) record);
            }
        }
        GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);
        HoodieRecordPayload payload = payloadCreation.createPayload(gr);
        final HoodieKey hoodieKey = keyGenerator.getKey(gr);
        return new HoodieRecord<>(hoodieKey, payload, operation);
    }
}

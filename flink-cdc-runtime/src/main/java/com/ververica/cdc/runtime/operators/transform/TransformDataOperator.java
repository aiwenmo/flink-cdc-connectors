/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.operators.transform;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.schema.Selectors;
import com.ververica.cdc.common.utils.SchemaUtils;
import com.ververica.cdc.common.utils.StringUtils;
import com.ververica.cdc.runtime.parser.FlinkSqlParser;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** A data process function that applies user-defined transform logics. */
public class TransformDataOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {

    private final List<Tuple3<String, String, String>> transformRules;
    private transient List<Tuple4<Selectors, Optional<Projector>, Optional<RowFilter>, Boolean>>
            transforms;

    /** keep the relationship of TableId and table information. */
    private final Map<TableId, TableInfo> tableInfoMap;

    public static TransformDataOperator.Builder newBuilder() {
        return new TransformDataOperator.Builder();
    }

    /** Builder of {@link TransformDataOperator}. */
    public static class Builder {
        private final List<Tuple3<String, String, String>> transformRules = new ArrayList<>();

        public TransformDataOperator.Builder addTransform(
                String tableInclusions, @Nullable String projection, @Nullable String filter) {
            transformRules.add(Tuple3.of(tableInclusions, projection, filter));
            return this;
        }

        public TransformDataOperator build() {
            return new TransformDataOperator(transformRules);
        }
    }

    private TransformDataOperator(List<Tuple3<String, String, String>> transformRules) {
        this.transformRules = transformRules;
        this.tableInfoMap = new ConcurrentHashMap<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        transforms =
                transformRules.stream()
                        .map(
                                tuple3 -> {
                                    String tableInclusions = tuple3.f0;
                                    String projection = tuple3.f1;
                                    String filterExpression = tuple3.f2;

                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple4<>(
                                            selectors,
                                            Projector.generateProjector(projection),
                                            RowFilter.generateRowFilter(filterExpression),
                                            containFilteredComputedColumn(
                                                    projection, filterExpression));
                                })
                        .collect(Collectors.toList());
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof FlushEvent) {
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof SchemaChangeEvent) {
            event = cacheSchema((SchemaChangeEvent) event);
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof DataChangeEvent) {
            Optional<DataChangeEvent> dataChangeEventOptional =
                    applyDataChangeEvent(((DataChangeEvent) event));
            if (dataChangeEventOptional.isPresent()) {
                output.collect(new StreamRecord<>(dataChangeEventOptional.get()));
            }
        }
    }

    private SchemaChangeEvent cacheSchema(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        Schema newSchema;
        if (event instanceof CreateTableEvent) {
            newSchema = ((CreateTableEvent) event).getSchema();
        } else {
            TableInfo tableInfo = tableInfoMap.get(tableId);
            if (tableInfo == null) {
                throw new RuntimeException("Schema of " + tableId + " is not existed.");
            }
            newSchema = SchemaUtils.applySchemaChangeEvent(tableInfo.getSchema(), event);
        }
        transformSchema(tableId, newSchema);
        tableInfoMap.put(tableId, TableInfo.of(tableId, newSchema));
        return event;
    }

    private void transformSchema(TableId tableId, Schema schema) {
        for (Tuple4<Selectors, Optional<Projector>, Optional<RowFilter>, Boolean> transform :
                transforms) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId) && transform.f1.isPresent()) {
                Projector projector = transform.f1.get();
                // update the columns of projection and add the column of projection into Schema
                projector.applySchemaChangeEvent(schema);
            }
        }
    }

    private Optional<DataChangeEvent> applyDataChangeEvent(DataChangeEvent dataChangeEvent) {
        Optional<DataChangeEvent> dataChangeEventOptional = Optional.of(dataChangeEvent);
        TableId tableId = dataChangeEvent.tableId();

        for (Tuple4<Selectors, Optional<Projector>, Optional<RowFilter>, Boolean> transform :
                transforms) {
            Selectors selectors = transform.f0;
            Boolean isPreProjection = transform.f3;
            if (selectors.isMatch(tableId)) {
                Optional<Projector> projectorOptional = transform.f1;
                if (isPreProjection
                        && projectorOptional.isPresent()
                        && projectorOptional.get().isValid()) {
                    dataChangeEventOptional =
                            applyProjection(projectorOptional.get(), dataChangeEventOptional.get());
                }
                Optional<RowFilter> rowFilterOptional = transform.f2;
                if (rowFilterOptional.isPresent() && rowFilterOptional.get().isValid()) {
                    dataChangeEventOptional =
                            applyFilter(rowFilterOptional.get(), dataChangeEventOptional.get());
                }
                if (!isPreProjection
                        && dataChangeEventOptional.isPresent()
                        && projectorOptional.isPresent()
                        && projectorOptional.get().isValid()) {
                    dataChangeEventOptional =
                            applyProjection(projectorOptional.get(), dataChangeEventOptional.get());
                }
            }
        }
        return dataChangeEventOptional;
    }

    private Optional<DataChangeEvent> applyFilter(
            RowFilter rowFilter, DataChangeEvent dataChangeEvent) {
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        // insert and update event only apply afterData, delete only apply beforeData
        if (after != null) {
            if (rowFilter.run(after, tableInfoMap.get(dataChangeEvent.tableId()))) {
                return Optional.of(dataChangeEvent);
            } else {
                return Optional.empty();
            }
        } else if (before != null) {
            if (rowFilter.run(before, tableInfoMap.get(dataChangeEvent.tableId()))) {
                return Optional.of(dataChangeEvent);
            } else {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private Optional<DataChangeEvent> applyProjection(
            Projector projector, DataChangeEvent dataChangeEvent) {
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if (before != null) {
            BinaryRecordData data =
                    projector.recordData(before, tableInfoMap.get(dataChangeEvent.tableId()));
            dataChangeEvent = DataChangeEvent.resetBefore(dataChangeEvent, data);
        }
        if (after != null) {
            BinaryRecordData data =
                    projector.recordData(after, tableInfoMap.get(dataChangeEvent.tableId()));
            dataChangeEvent = DataChangeEvent.resetAfter(dataChangeEvent, data);
        }
        return Optional.of(dataChangeEvent);
    }

    private boolean containFilteredComputedColumn(String projection, String filter) {
        boolean contain = false;
        if (StringUtils.isNullOrWhitespaceOnly(projection)
                || StringUtils.isNullOrWhitespaceOnly(filter)) {
            return contain;
        }
        List<String> computedColumnNames = FlinkSqlParser.parseComputedColumnNames(projection);
        List<String> filteredColumnNames = FlinkSqlParser.parseFilterColumnNameList(filter);
        for (String computedColumnName : computedColumnNames) {
            if (filteredColumnNames.contains(computedColumnName)) {
                return true;
            }
        }
        return contain;
    }
}

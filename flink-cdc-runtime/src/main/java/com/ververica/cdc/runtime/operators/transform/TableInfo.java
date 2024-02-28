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

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.utils.SchemaUtils;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import com.ververica.cdc.runtime.typeutils.DataTypeConverter;

import java.util.List;

/** The TableInfo applies to cache schema and fieldGetters. */
public class TableInfo {
    private TableId tableId;
    private Schema schema;
    private RecordData.FieldGetter[] fieldGetters;
    private BinaryRecordDataGenerator recordDataGenerator;

    public TableInfo(
            TableId tableId,
            Schema schema,
            RecordData.FieldGetter[] fieldGetters,
            BinaryRecordDataGenerator recordDataGenerator) {
        this.tableId = tableId;
        this.schema = schema;
        this.fieldGetters = fieldGetters;
        this.recordDataGenerator = recordDataGenerator;
    }

    public String getName() {
        return tableId.identifier();
    }

    public String getTableName() {
        return tableId.getTableName();
    }

    public String getSchemaName() {
        return tableId.getSchemaName();
    }

    public String getNamespace() {
        return tableId.getNamespace();
    }

    public TableId getTableId() {
        return tableId;
    }

    public Schema getSchema() {
        return schema;
    }

    public RecordData.FieldGetter[] getFieldGetters() {
        return fieldGetters;
    }

    public BinaryRecordDataGenerator getRecordDataGenerator() {
        return recordDataGenerator;
    }

    public static TableInfo of(TableId tableId, Schema schema) {
        List<RecordData.FieldGetter> fieldGetters =
                SchemaUtils.createFieldGetters(schema.getColumns());
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(DataTypeConverter.toRowType(schema.getColumns()));
        return new TableInfo(
                tableId,
                schema,
                fieldGetters.toArray(new RecordData.FieldGetter[0]),
                recordDataGenerator);
    }
}

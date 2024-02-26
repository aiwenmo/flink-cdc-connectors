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
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.utils.StringUtils;
import com.ververica.cdc.runtime.parser.TransformParser;
import com.ververica.cdc.runtime.typeutils.DataTypeConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** The Projector applies to describe the projection of filtering tables. */
public class Projector {
    private String projection;
    private List<ColumnTransform> columnTransformList;

    public Projector(String projection, List<ColumnTransform> columnTransformList) {
        this.projection = projection;
        this.columnTransformList = columnTransformList;
    }

    public boolean isValid() {
        return !StringUtils.isNullOrWhitespaceOnly(projection);
    }

    private static Projector of(String projection, List<ColumnTransform> columnTransformList) {
        return new Projector(projection, columnTransformList);
    }

    public static Optional<Projector> generateProjector(String projection) {
        if (StringUtils.isNullOrWhitespaceOnly(projection)) {
            return Optional.empty();
        }
        return Optional.of(of(projection, new ArrayList<>()));
    }

    private List<Column> getAllColumnList() {
        return columnTransformList.stream()
                .map(ColumnTransform::getColumn)
                .collect(Collectors.toList());
    }

    public CreateTableEvent applyCreateTableEvent(CreateTableEvent createTableEvent) {
        columnTransformList =
                TransformParser.generateColumnTransforms(
                        projection, createTableEvent.getSchema().getColumns());
        List<Column> allColumnList = getAllColumnList();
        // add the column of projection into Schema
        Schema schema = createTableEvent.getSchema().copy(allColumnList);
        return new CreateTableEvent(createTableEvent.tableId(), schema);
    }

    public void applySchemaChangeEvent(Schema schema) {
        columnTransformList =
                TransformParser.generateColumnTransforms(projection, schema.getColumns());
    }

    public BinaryRecordData recordFillDataField(
            BinaryRecordData data, TableChangeInfo tableChangeInfo) {
        List<Object> valueList = new ArrayList<>();
        for (Column column : tableChangeInfo.getTransformedSchema().getColumns()) {
            boolean isColumnTransform = false;
            for (ColumnTransform columnTransform : columnTransformList) {
                if (column.getName().equals(columnTransform.getColumnName())
                        && columnTransform.isValidProjection()) {
                    valueList.add(null);
                    isColumnTransform = true;
                    break;
                }
            }
            if (!isColumnTransform) {
                valueList.add(
                        getValueFromBinaryRecordData(column.getName(), data, tableChangeInfo));
            }
        }
        return tableChangeInfo
                .getRecordDataGenerator()
                .generate(valueList.toArray(new Object[valueList.size()]));
    }

    public BinaryRecordData recordData(BinaryRecordData after, TableInfo tableInfo) {
        List<Object> valueList = new ArrayList<>();
        for (Column column : tableInfo.getSchema().getColumns()) {
            boolean isColumnTransform = false;
            for (ColumnTransform columnTransform : columnTransformList) {
                if (column.getName().equals(columnTransform.getColumnName())
                        && columnTransform.isValidProjection()) {
                    valueList.add(
                            DataTypeConverter.convert(
                                    columnTransform.evaluate(after, tableInfo),
                                    columnTransform.getDataType()));
                    isColumnTransform = true;
                    break;
                }
            }
            if (!isColumnTransform) {
                valueList.add(getValueFromBinaryRecordData(column.getName(), after, tableInfo));
            }
        }
        return tableInfo
                .getRecordDataGenerator()
                .generate(valueList.toArray(new Object[valueList.size()]));
    }

    private Object getValueFromBinaryRecordData(
            String columnName, BinaryRecordData binaryRecordData, TableChangeInfo tableChangeInfo) {
        List<Column> columns = tableChangeInfo.getOriginalSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columnName.equals(columns.get(i).getName())) {
                RecordData.FieldGetter[] fieldGetters = tableChangeInfo.getFieldGetters();
                return DataTypeConverter.convert(
                        fieldGetters[i].getFieldOrNull(binaryRecordData), columns.get(i).getType());
            }
        }
        return null;
    }

    private Object getValueFromBinaryRecordData(
            String columnName, BinaryRecordData binaryRecordData, TableInfo tableInfo) {
        List<Column> columns = tableInfo.getSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columnName.equals(columns.get(i).getName())) {
                RecordData.FieldGetter[] fieldGetters = tableInfo.getFieldGetters();
                return DataTypeConverter.convert(
                        fieldGetters[i].getFieldOrNull(binaryRecordData), columns.get(i).getType());
            }
        }
        return null;
    }
}

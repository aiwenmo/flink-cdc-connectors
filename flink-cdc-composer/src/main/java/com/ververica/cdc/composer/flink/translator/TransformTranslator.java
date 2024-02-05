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

package com.ververica.cdc.composer.flink.translator;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.composer.definition.TransformDef;
import com.ververica.cdc.runtime.operators.transform.TransformDataOperator;
import com.ververica.cdc.runtime.operators.transform.TransformSchemaOperator;
import com.ververica.cdc.runtime.typeutils.EventTypeInfo;

import java.util.List;

/** Translator for transform schema. */
public class TransformTranslator {

    public DataStream<Event> translateSchema(
            DataStream<Event> input, List<TransformDef> transforms) {
        if (transforms.isEmpty()) {
            return input;
        }

        TransformSchemaOperator.Builder transformSchemaFunctionBuilder =
                TransformSchemaOperator.newBuilder();
        for (TransformDef transform : transforms) {
            if (transform.isValidProjection()) {
                transformSchemaFunctionBuilder.addTransform(
                        transform.getSourceTable(), transform.getProjection().get());
            }
        }
        return input.transform(
                "Transform:Schema", new EventTypeInfo(), transformSchemaFunctionBuilder.build());
    }

    public DataStream<Event> translateData(
            DataStream<Event> input, List<TransformDef> transforms, OperatorID schemaOperatorID) {
        if (transforms.isEmpty()) {
            return input;
        }

        TransformDataOperator.Builder transformDataFunctionBuilder =
                TransformDataOperator.newBuilder();
        for (TransformDef transform : transforms) {
            if (transform.isValidProjection() || transform.isValidFilter()) {
                transformDataFunctionBuilder.addTransform(
                        transform.getSourceTable(),
                        transform.isValidProjection() ? transform.getProjection().get() : null,
                        transform.isValidFilter() ? transform.getFilter().get() : null);
            }
        }
        transformDataFunctionBuilder.addSchemaOperatorID(schemaOperatorID);
        return input.transform(
                "Transform:Data", new EventTypeInfo(), transformDataFunctionBuilder.build());
    }
}

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

package com.ververica.cdc.composer.definition;

import com.ververica.cdc.common.utils.StringUtils;

import java.util.Objects;
import java.util.Optional;

/**
 * Definition of a transformation.
 *
 * <p>A transformation definition contains:
 *
 * <ul>
 *   <li>sourceTable: a regex pattern for matching input table IDs. Required for the definition.
 *   <li>projection: a string for projecting the row of matched table as output. Optional for the
 *       definition.
 *   <li>filter: a string for filtering the row of matched table as output. Optional for the
 *       definition.
 *   <li>description: description for the transformation. Optional for the definition.
 * </ul>
 */
public class TransformDef {
    private final String sourceTable;
    private final String projection;
    private final String filter;
    private final String description;

    public TransformDef(String sourceTable, String projection, String filter, String description) {
        this.sourceTable = sourceTable;
        this.projection = projection;
        this.filter = filter;
        this.description = description;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public Optional<String> getProjection() {
        return Optional.ofNullable(projection);
    }

    public boolean isValidProjection() {
        return !StringUtils.isNullOrWhitespaceOnly(projection);
    }

    public Optional<String> getFilter() {
        return Optional.ofNullable(filter);
    }

    public boolean isValidFilter() {
        return !StringUtils.isNullOrWhitespaceOnly(filter);
    }

    public Optional<String> getDescription() {
        return Optional.ofNullable(description);
    }

    @Override
    public String toString() {
        return "TransformDef{"
                + "sourceTable='"
                + sourceTable
                + '\''
                + ", projection='"
                + projection
                + '\''
                + ", filter='"
                + filter
                + '\''
                + ", description='"
                + description
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransformDef that = (TransformDef) o;
        return Objects.equals(sourceTable, that.sourceTable)
                && Objects.equals(projection, that.projection)
                && Objects.equals(filter, that.filter)
                && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceTable, projection, filter, description);
    }
}

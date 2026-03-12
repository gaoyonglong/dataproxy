/*
 * Copyright 2024 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.core.visitor;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * @author yuexie
 * @date 2024/11/1 16:48
 **/
public interface ValueVisitor<T> {

    default T visit(@Nonnull Integer value) {
        throw new UnsupportedOperationException("Integer not supported");
    }

    default T visit(@Nonnull Short value) {
        throw new UnsupportedOperationException("Short not supported");
    }

    default T visit(@Nonnull Long value) {
        throw new UnsupportedOperationException("Long not supported");
    }

    default T visit(@Nonnull Double value) {
        throw new UnsupportedOperationException("Double not supported");
    }

    default T visit(@Nonnull Float value) {
        throw new UnsupportedOperationException("Float not supported");
    }

    default T visit(boolean value) {
        throw new UnsupportedOperationException("Boolean not supported");
    }

    default T visit(@Nonnull Date value) {
        throw new UnsupportedOperationException("Date not supported");
    }

    default T visit(@Nonnull String value) {
        throw new UnsupportedOperationException("String not supported");
    }

    default T visit(@Nonnull byte[] value) {
        throw new UnsupportedOperationException("byte[] not supported");
    }

    default T visit(@Nonnull BigDecimal value) {
        throw new UnsupportedOperationException("BigDecimal not supported");
    }

    default T visit(@Nonnull Object value) {
        throw new UnsupportedOperationException("Object not supported");
    }

    default T visit(@Nonnull ZonedDateTime value) {
        throw new UnsupportedOperationException("Object not supported");
    }

    default T visit(@Nonnull LocalDateTime value) {
        throw new UnsupportedOperationException("Object not supported");
    }

    default T visit(@Nonnull LocalDate value) {
        throw new UnsupportedOperationException("Object not supported");
    }

    default T visit(@Nonnull Instant value) {
        throw new UnsupportedOperationException("Object not supported");
    }

}

interface BigDecimalValueVisitor extends ValueVisitor<BigDecimal> {

    @Override
    default BigDecimal visit(@Nonnull Integer value) {
        return BigDecimal.valueOf(value);
    }

    @Override
    default BigDecimal visit(@Nonnull Long value) {
        return BigDecimal.valueOf(value);
    }

    @Override
    default BigDecimal visit(@Nonnull Double value) {
        return BigDecimal.valueOf(value);
    }

    @Override
    default BigDecimal visit(@Nonnull Float value) {
        return BigDecimal.valueOf(value);
    }

    @Override
    default BigDecimal visit(String value) {
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot convert string to BigDecimal: " + value, e);
        }
    }

    @Override
    default BigDecimal visit(@Nonnull BigDecimal value) {
        return value;
    }

    @Override
    default BigDecimal visit(@Nonnull Object value) {
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        if (value instanceof String) {
            return visit(value);
        }
        throw new IllegalArgumentException("BigDecimalValueVisitor unsupported type: " + value.getClass().getName());
    }
}

interface ObjectValueVisitor extends ValueVisitor<Object> {

    @Override
    default Object visit(@Nonnull Integer value) {
        return value;
    }

    @Override
    default Object visit(@Nonnull Long value) {
        return value;
    }

    @Override
    default Object visit(@Nonnull Double value) {
        return value;
    }

    @Override
    default Object visit(@Nonnull Float value) {
        return value;
    }

    @Override
    default Object visit(boolean value) {
        return value;
    }

    @Override
    default Object visit(String value) {
        return value;
    }

    @Override
    default Object visit(@Nonnull byte[] value) {
        return value;
    }

    @Override
    default Object visit(@Nonnull BigDecimal value) {
        return value;
    }

    @Override
    default Object visit(@Nonnull Object value) {
        return value;
    }
}

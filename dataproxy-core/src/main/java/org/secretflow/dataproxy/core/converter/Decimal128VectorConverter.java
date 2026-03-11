/*
 * Copyright 2025 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.core.converter;

import org.apache.arrow.vector.ValueVector;
import org.secretflow.dataproxy.core.visitor.ValueVisitor;

import java.math.BigDecimal;

/**
 * Converter for Decimal128 Arrow
 * Note: Uses generic NumberVector for compatibility across Arrow versions
 *
 * @author chenmingliang
 * @date 2026/2/6
 */
public class Decimal128VectorConverter extends AbstractValueConverter<BigDecimal> {

    public Decimal128VectorConverter(ValueVisitor<BigDecimal> visitor) {
        super(visitor);
    }

    @Override
    public void convertAndSet(ValueVector vector, int index, Object value) {
        // Try to handle various decimal vector types
        try {
            // Use reflection to check for setSafe methods to support different Arrow versions
            java.lang.reflect.Method setSafeMethod = vector.getClass().getMethod("setSafe", int.class, BigDecimal.class);
            setSafeMethod.invoke(vector, index, this.visit(value));
        } catch (NoSuchMethodException e) {
            // Fallback: try using Object parameter
            try {
                java.lang.reflect.Method setSafeMethod = vector.getClass().getMethod("setSafe", int.class, Object.class);
                setSafeMethod.invoke(vector, index, this.visit(value));
            } catch (Exception ex) {
                throw new IllegalArgumentException("Decimal128VectorConverter unsupported vector type: " + vector.getClass().getName(), ex);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Decimal128VectorConverter error: " + vector.getClass().getName(), e);
        }
    }
}
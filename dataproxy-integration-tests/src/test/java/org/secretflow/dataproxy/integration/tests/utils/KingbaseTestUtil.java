/*
 * Copyright 2026 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.integration.tests.utils;

import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @author chenmingliang
 * @date 2025/12/11
 */
public class KingbaseTestUtil {

    private static final Properties properties = new Properties();

    static {
        try (InputStream is = KingbaseTestUtil.class.getResourceAsStream("/test-kingbase.conf")) {
            properties.load(is);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getKingbaseEndpoint() {
        return properties.getProperty("test.kingbase.endpoint");
    }

    public static String getkingbaseUsername() {
        return properties.getProperty("test.kingbase.username");
    }

    public static String getKingbasePassword() {
        return properties.getProperty("test.kingbase.password");
    }

    public static String getKingbaseDatabase() {
        return properties.getProperty("test.kingbase.database");
    }
}

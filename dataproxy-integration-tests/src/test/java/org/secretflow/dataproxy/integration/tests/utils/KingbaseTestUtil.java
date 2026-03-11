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

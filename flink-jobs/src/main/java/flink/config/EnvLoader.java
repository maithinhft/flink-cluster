package flink.config;

import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvLoader {
    private static final Logger LOG = LoggerFactory.getLogger(EnvLoader.class);
    private static Dotenv dotenv;

    static {
        try {
            // Flink job chạy có thể từ thư mục flink-jobs hoặc thư mục cha
            dotenv = Dotenv.configure()
                    .directory("../") // Trỏ về thư mục gốc của project (nơi chứa .env)
                    .ignoreIfMalformed()
                    .ignoreIfMissing()
                    .load();
            LOG.info("Loaded .env from ../");
        } catch (Exception e) {
            try {
                // Fallback nếu thư mục hiện tại là thư mục gốc
                dotenv = Dotenv.configure()
                        .directory("./")
                        .ignoreIfMalformed()
                        .ignoreIfMissing()
                        .load();
                LOG.info("Loaded .env from ./");
            } catch (Exception ex) {
                LOG.warn("Could not load .env file. Falling back to system environment variables.");
            }
        }
    }

    public static String get(String key, String defaultValue) {
        if (dotenv != null) {
            String val = dotenv.get(key);
            if (val != null && !val.trim().isEmpty()) {
                return val;
            }
        }
        String sysVal = System.getenv(key);
        return (sysVal != null && !sysVal.trim().isEmpty()) ? sysVal : defaultValue;
    }

    public static String get(String key) {
        return get(key, null);
    }
}

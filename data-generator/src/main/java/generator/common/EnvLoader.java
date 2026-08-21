package generator.common;

import io.github.cdimascio.dotenv.Dotenv;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class EnvLoader {

    private static Dotenv dotenv;

    static {
        try {
            // Find .env file by looking up the directory tree
            Path currentPath = Paths.get("").toAbsolutePath();
            String envDir = null;
            
            for (int i = 0; i < 5; i++) {
                if (Files.exists(currentPath.resolve(".env"))) {
                    envDir = currentPath.toString();
                    break;
                }
                currentPath = currentPath.getParent();
                if (currentPath == null) break;
            }

            if (envDir != null) {
                dotenv = Dotenv.configure().directory(envDir).load();
                System.out.println("Loaded .env from: " + envDir);
            } else {
                System.out.println("No .env file found. Falling back to system environment variables.");
                dotenv = Dotenv.configure().ignoreIfMissing().load();
            }
        } catch (Exception e) {
            System.err.println("Warning: Failed to load .env file. " + e.getMessage());
        }
    }

    public static String get(String key, String defaultValue) {
        if (dotenv != null) {
            String value = dotenv.get(key);
            if (value != null && !value.trim().isEmpty()) {
                return value;
            }
        }
        String sysVal = System.getenv(key);
        return (sysVal != null && !sysVal.trim().isEmpty()) ? sysVal : defaultValue;
    }
}

package generator.rules;

import java.sql.Connection;
import java.sql.DriverManager;

public class RuleGeneratorApp {

    public static void main(String[] args) throws Exception {
        String dbUrl = RuleConfig.DEFAULT_DB_URL;
        String dbUser = RuleConfig.DEFAULT_DB_USER;
        String dbPassword = RuleConfig.DEFAULT_DB_PASSWORD;
        int numRules = RuleConfig.DEFAULT_NUM_RULES;
        int batchSize = RuleConfig.DEFAULT_BATCH_SIZE;
        boolean continuous = false;
        String schemaPath = "./schema";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--num-rules":
                    numRules = Integer.parseInt(args[++i]);
                    break;
                case "--batch-size":
                    batchSize = Integer.parseInt(args[++i]);
                    break;
                case "--db-url":
                    dbUrl = args[++i];
                    break;
                case "--db-user":
                    dbUser = args[++i];
                    break;
                case "--db-password":
                    dbPassword = args[++i];
                    break;
                case "--continuous":
                    continuous = true;
                    break;
                case "--and-root-ratio":
                    RuleFactory.andRootRatio = Double.parseDouble(args[++i]);
                    break;
                case "--linear-ratio":
                    RuleFactory.linearRatio = Double.parseDouble(args[++i]);
                    break;
                case "--rule-metric-mode":
                    RuleFactory.metricMode = args[++i];
                    break;
                case "--schema-path":
                    schemaPath = args[++i];
                    break;
                case "--seed":
                    generator.common.RandomUtils.RANDOM.setSeed(Long.parseLong(args[++i]));
                    break;
            }
        }

        System.out.println("======================================================");
        System.out.println("PostgreSQL Rule Definition Generator (Concrete Fields)");
        System.out.println("======================================================");
        System.out.println("======================================================");
        System.out.printf("Rules        : %s%n", continuous ? "Continuous" : String.format("%,d", numRules));
        System.out.printf("AND root     : %.2f, LINEAR leaves: %.2f, metric mode: %s%n",
                RuleFactory.andRootRatio, RuleFactory.linearRatio, RuleFactory.metricMode);
        if ("reference".equalsIgnoreCase(RuleFactory.metricMode)) {
            RuleFactory.loadMetricAliases(java.nio.file.Paths.get(schemaPath));
            System.out.printf("Metric aliases: %,d loaded from %s%n", RuleFactory.metricAliases.size(), schemaPath);
        }

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            connection.setAutoCommit(false);
            long start = System.nanoTime();

            RuleDao.generateAndInsertRules(connection, numRules, batchSize, continuous);

            connection.commit();
            double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;

            System.out.println("======================================================");
            System.out.println("Generation completed");
            System.out.printf("Elapsed      : %.2f s%n", elapsed);
            System.out.printf("Throughput   : %,.0f rules/s%n", numRules / elapsed);
        }
    }
}

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
            }
        }

        System.out.println("======================================================");
        System.out.println("PostgreSQL Rule Definition Generator (Concrete Fields)");
        System.out.println("======================================================");
        System.out.printf("Rules        : %,d%n", numRules);

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            connection.setAutoCommit(false);
            long start = System.nanoTime();

            RuleDao.generateAndInsertRules(connection, numRules, batchSize);

            connection.commit();
            double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;

            System.out.println("======================================================");
            System.out.println("Generation completed");
            System.out.printf("Elapsed      : %.2f s%n", elapsed);
            System.out.printf("Throughput   : %,.0f rules/s%n", numRules / elapsed);
        }
    }
}

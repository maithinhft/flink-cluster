package flink;

import flink.evaluators.FilterEvaluatorTest;
import flink.models.RingBufferTest;

import java.lang.reflect.Method;

public class TestRunner {

    public static void main(String[] args) {
        System.out.println("=================================================");
        System.out.println("   Running Flink Aggregation Unit Tests");
        System.out.println("=================================================");

        int total = 0;
        int passed = 0;
        int failed = 0;

        Class<?>[] testClasses = new Class<?>[] {
                RingBufferTest.class,
                FilterEvaluatorTest.class,
                flink.evaluators.RuleEvaluatorTest.class
        };

        for (Class<?> testClass : testClasses) {
            System.out.println("\nExecuting: " + testClass.getSimpleName());
            for (Method m : testClass.getDeclaredMethods()) {
                if (m.isAnnotationPresent(org.junit.jupiter.api.Test.class)) {
                    total++;
                    try {
                        Object instance = testClass.getDeclaredConstructor().newInstance();
                        // Run setUp if present
                        for (Method setupMethod : testClass.getDeclaredMethods()) {
                            if (setupMethod.isAnnotationPresent(org.junit.jupiter.api.BeforeEach.class)) {
                                setupMethod.invoke(instance);
                            }
                        }
                        m.invoke(instance);
                        passed++;
                        System.out.println("  [PASS] " + m.getName());
                    } catch (Throwable t) {
                        failed++;
                        Throwable cause = t.getCause() != null ? t.getCause() : t;
                        System.err.println("  [FAIL] " + m.getName() + ": " + cause.getMessage());
                        cause.printStackTrace();
                    }
                }
            }
        }

        System.out.println("\n=================================================");
        System.out.println("Test Summary: Total=" + total + ", Passed=" + passed + ", Failed=" + failed);
        System.out.println("=================================================");

        if (failed > 0) {
            System.exit(1);
        }
    }
}


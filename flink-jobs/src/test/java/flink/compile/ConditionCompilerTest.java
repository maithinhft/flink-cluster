package flink.compile;

import com.fasterxml.jackson.databind.ObjectMapper;
import flink.model.ValidatedEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ConditionCompilerTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static boolean test(String condition, ValidatedEvent ev) throws Exception {
        Node n = ConditionCompiler.compileFilter(M.readTree(condition));
        return n.eval(new EvalContext() {
            @Override
            public ValidatedEvent event() {
                return ev;
            }

            @Override
            public boolean testAggregate(AggPredNode node) {
                return false;
            }

            @Override
            public Double aggregateValue(AggRef ref) {
                return null;
            }

            @Override
            public void record(String path, Double value) {
            }
        });
    }

    private static ValidatedEvent ev() {
        return ValidatedEvent.builder()
                .entity("u1").eventType("purchase")
                .str("source_system", "ecommerce")
                .str("product_category", "electronics")
                .num("total_amount", 1500.0)
                .num("quantity", 5)
                .bool("is_3ds_verified", true)
                .str("user_email", "a@example.com")
                .str("registration_date", "2026-01-15T10:00:00Z")
                .str("amount_text", "42.5")
                .build();
    }

    @Test
    void nullOrEmptyFilterIsTrue() throws Exception {
        assertTrue(test("null", ev()));
        assertTrue(test("{}", ev()));
    }

    @Test
    void equalityAcrossStringNumberAndBoolean() throws Exception {
        assertTrue(test("{\"type\":\"RAW_FIELD\",\"field\":\"product_category\",\"operator\":\"EQ\",\"value\":\"electronics\"}", ev()));
        assertFalse(test("{\"field\":\"product_category\",\"operator\":\"=\",\"value\":\"books\"}", ev()));
        assertTrue(test("{\"field\":\"quantity\",\"operator\":\"EQ\",\"value\":5}", ev()));
        assertTrue(test("{\"field\":\"quantity\",\"operator\":\"EQ\",\"value\":\"5\"}", ev()));
        assertTrue(test("{\"field\":\"is_3ds_verified\",\"operator\":\"EQ\",\"value\":true}", ev()));
        assertTrue(test("{\"field\":\"is_3ds_verified\",\"operator\":\"!=\",\"value\":false}", ev()));
    }

    @Test
    void numericComparisonsAndBetween() throws Exception {
        assertTrue(test("{\"field\":\"total_amount\",\"operator\":\"GT\",\"value\":1000}", ev()));
        assertTrue(test("{\"field\":\"total_amount\",\"operator\":\">=\",\"value\":1500}", ev()));
        assertFalse(test("{\"field\":\"total_amount\",\"operator\":\"LT\",\"value\":1500}", ev()));
        assertTrue(test("{\"field\":\"total_amount\",\"operator\":\"LTE\",\"value\":1500.0}", ev()));
        assertTrue(test("{\"field\":\"total_amount\",\"operator\":\"BETWEEN\",\"value\":[1000,2000]}", ev()));
        assertTrue(test("{\"field\":\"total_amount\",\"operator\":\"BETWEEN\",\"value\":[2000,1000]}", ev()),
                "reversed bounds are normalised");
        assertTrue(test("{\"field\":\"amount_text\",\"operator\":\"GT\",\"value\":40}", ev()),
                "numeric strings compare numerically");
    }

    @Test
    void timestampComparison() throws Exception {
        assertTrue(test("{\"field\":\"registration_date\",\"operator\":\"GT\",\"value\":\"2026-01-01T00:00:00Z\"}", ev()));
        assertFalse(test("{\"field\":\"registration_date\",\"operator\":\"LT\",\"value\":\"2025-12-31\"}", ev()));
    }

    @Test
    void setOperatorsAndContains() throws Exception {
        assertTrue(test("{\"field\":\"product_category\",\"operator\":\"IN\",\"value\":[\"books\",\"electronics\"]}", ev()));
        assertFalse(test("{\"field\":\"product_category\",\"operator\":\"NOT IN\",\"value\":[\"books\",\"electronics\"]}", ev()));
        assertTrue(test("{\"field\":\"product_category\",\"operator\":\"NOT_IN\",\"value\":[\"food\"]}", ev()));
        assertFalse(test("{\"field\":\"product_category\",\"operator\":\"IN\",\"value\":[]}", ev()));
        assertTrue(test("{\"field\":\"user_email\",\"operator\":\"CONTAINS\",\"value\":\"example\"}", ev()));
    }

    @Test
    void missingFieldOnlySatisfiesNegativeOperators() throws Exception {
        assertFalse(test("{\"field\":\"nope\",\"operator\":\"EQ\",\"value\":\"x\"}", ev()));
        assertFalse(test("{\"field\":\"nope\",\"operator\":\"GT\",\"value\":1}", ev()));
        assertTrue(test("{\"field\":\"nope\",\"operator\":\"NEQ\",\"value\":\"x\"}", ev()));
        assertTrue(test("{\"field\":\"nope\",\"operator\":\"NOT IN\",\"value\":[\"x\"]}", ev()));
    }

    @Test
    void logicalAndOrNotAndReportDialect() throws Exception {
        String and = "{\"operator\":\"AND\",\"children\":["
                + "{\"field\":\"source_system\",\"operator\":\"EQ\",\"value\":\"ecommerce\"},"
                + "{\"operator\":\"OR\",\"children\":["
                + "{\"field\":\"product_category\",\"operator\":\"EQ\",\"value\":\"books\"},"
                + "{\"field\":\"total_amount\",\"operator\":\"GT\",\"value\":1000}]}]}";
        assertTrue(test(and, ev()));
        assertFalse(test("{\"operator\":\"NOT\",\"children\":[" + and + "]}", ev()));
        String report = "{\"type\":\"LOGIC\",\"operator\":\"AND\",\"children\":["
                + "{\"type\":\"LEAF\",\"field_type\":\"raw\",\"field_name\":\"product_category\",\"operator\":\"=\",\"value\":\"electronics\"}]}";
        assertTrue(test(report, ev()));
    }

    @Test
    void notOfNotAndFlatteningAreSimplified() throws Exception {
        Node n = ConditionCompiler.compileFilter(M.readTree(
                "{\"operator\":\"NOT\",\"children\":[{\"operator\":\"NOT\",\"children\":[{\"field\":\"a\",\"value\":1}]}]}"));
        assertInstanceOf(RawPredNode.class, n);
        Node and = ConditionCompiler.compileFilter(M.readTree(
                "{\"operator\":\"AND\",\"children\":[{\"field\":\"a\",\"value\":1},"
                        + "{\"operator\":\"AND\",\"children\":[{\"field\":\"b\",\"value\":2},{\"field\":\"c\",\"value\":3}]}]}"));
        assertEquals(3, ((Node.And) and).children().length);
    }

    @Test
    void aggregationsAreRejectedInsideFiltersAndUnknownOperatorsFail() {
        assertThrows(CompileException.class, () -> ConditionCompiler.compileFilter(M.readTree(
                "{\"type\":\"AGGREGATION\",\"field\":\"x\",\"function\":\"SUM\",\"operator\":\"GT\",\"value\":1}")));
        assertThrows(CompileException.class, () -> ConditionCompiler.compileFilter(M.readTree(
                "{\"field\":\"x\",\"operator\":\"LIKE\",\"value\":1}")));
        assertThrows(CompileException.class, () -> ConditionCompiler.compileCondition(M.readTree(
                "{\"type\":\"AGGREGATION\",\"field\":\"x\",\"function\":\"SUM\",\"operator\":\"GT\",\"value\":\"abc\"}")));
    }

    @Test
    void costOrderingPutsRawChecksBeforeAggregates() throws Exception {
        Node n = ConditionCompiler.compileCondition(M.readTree("{\"operator\":\"AND\",\"children\":["
                + "{\"type\":\"AGGREGATION\",\"field\":\"total_amount\",\"function\":\"SUM\",\"window\":{\"lookback_seconds\":86400},\"operator\":\"GT\",\"value\":1},"
                + "{\"field\":\"source_system\",\"operator\":\"EQ\",\"value\":\"crm\"}]}"));
        Node[] c = ((Node.And) n).children();
        assertInstanceOf(RawPredNode.class, c[0]);
        assertInstanceOf(AggPredNode.class, c[1]);
    }
}

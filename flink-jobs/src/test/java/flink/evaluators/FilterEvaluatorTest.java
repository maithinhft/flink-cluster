package flink.evaluators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FilterEvaluatorTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testNullOrEmptyFilter() {
        JsonNode event = mapper.createObjectNode().put("source_system", "ecommerce");
        assertTrue(FilterEvaluator.evaluate(null, event));
        assertTrue(FilterEvaluator.evaluate(mapper.nullNode(), event));
        assertTrue(FilterEvaluator.evaluate(mapper.missingNode(), event));
    }

    @Test
    public void testRawFieldEquality() throws Exception {
        String eventJson = "{\"source_system\": \"ecommerce\", \"event_type\": \"purchase\", \"total_amount\": 150000.0}";
        JsonNode event = mapper.readTree(eventJson);

        String matchingFilter = "{\"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"EQ\", \"value\": \"ecommerce\"}";
        assertTrue(FilterEvaluator.evaluate(mapper.readTree(matchingFilter), event));

        String nonMatchingFilter = "{\"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"EQ\", \"value\": \"crm\"}";
        assertFalse(FilterEvaluator.evaluate(mapper.readTree(nonMatchingFilter), event));

        String neqFilter = "{\"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"NEQ\", \"value\": \"crm\"}";
        assertTrue(FilterEvaluator.evaluate(mapper.readTree(neqFilter), event));
    }

    @Test
    public void testNumericComparisons() throws Exception {
        String eventJson = "{\"unit_price\": 50.0, \"quantity\": 3, \"loyalty_points\": 120}";
        JsonNode event = mapper.readTree(eventJson);

        assertTrue(FilterEvaluator.evaluate(mapper.readTree("{\"type\": \"RAW_FIELD\", \"field\": \"unit_price\", \"operator\": \"GT\", \"value\": 40.0}"), event));
        assertFalse(FilterEvaluator.evaluate(mapper.readTree("{\"type\": \"RAW_FIELD\", \"field\": \"unit_price\", \"operator\": \"GT\", \"value\": 50.0}"), event));
        assertTrue(FilterEvaluator.evaluate(mapper.readTree("{\"type\": \"RAW_FIELD\", \"field\": \"unit_price\", \"operator\": \"GTE\", \"value\": 50.0}"), event));

        assertTrue(FilterEvaluator.evaluate(mapper.readTree("{\"type\": \"RAW_FIELD\", \"field\": \"quantity\", \"operator\": \"LT\", \"value\": 5}"), event));
        assertTrue(FilterEvaluator.evaluate(mapper.readTree("{\"type\": \"RAW_FIELD\", \"field\": \"quantity\", \"operator\": \"LTE\", \"value\": 3}"), event));

        // BETWEEN
        assertTrue(FilterEvaluator.evaluate(mapper.readTree("{\"type\": \"RAW_FIELD\", \"field\": \"loyalty_points\", \"operator\": \"BETWEEN\", \"value\": [100, 200]}"), event));
        assertFalse(FilterEvaluator.evaluate(mapper.readTree("{\"type\": \"RAW_FIELD\", \"field\": \"loyalty_points\", \"operator\": \"BETWEEN\", \"value\": [130, 200]}"), event));
    }

    @Test
    public void testSetOperationsInAndNotIn() throws Exception {
        String eventJson = "{\"product_category\": \"electronics\"}";
        JsonNode event = mapper.readTree(eventJson);

        String inFilter = "{\"type\": \"RAW_FIELD\", \"field\": \"product_category\", \"operator\": \"IN\", \"value\": [\"fashion\", \"electronics\", \"books\"]}";
        assertTrue(FilterEvaluator.evaluate(mapper.readTree(inFilter), event));

        String notInFilter = "{\"type\": \"RAW_FIELD\", \"field\": \"product_category\", \"operator\": \"NOT IN\", \"value\": [\"fashion\", \"books\"]}";
        assertTrue(FilterEvaluator.evaluate(mapper.readTree(notInFilter), event));

        String inFailFilter = "{\"type\": \"RAW_FIELD\", \"field\": \"product_category\", \"operator\": \"IN\", \"value\": [\"fashion\", \"books\"]}";
        assertFalse(FilterEvaluator.evaluate(mapper.readTree(inFailFilter), event));
    }

    @Test
    public void testLogicalAndOrNot() throws Exception {
        String eventJson = "{\"source_system\": \"ecommerce\", \"event_type\": \"purchase\", \"product_category\": \"electronics\"}";
        JsonNode event = mapper.readTree(eventJson);

        // AND
        String andFilter = "{\n" +
                "  \"operator\": \"AND\",\n" +
                "  \"children\": [\n" +
                "    {\"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"EQ\", \"value\": \"ecommerce\"},\n" +
                "    {\"type\": \"RAW_FIELD\", \"field\": \"event_type\", \"operator\": \"EQ\", \"value\": \"purchase\"}\n" +
                "  ]\n" +
                "}";
        assertTrue(FilterEvaluator.evaluate(mapper.readTree(andFilter), event));

        // OR
        String orFilter = "{\n" +
                "  \"operator\": \"OR\",\n" +
                "  \"children\": [\n" +
                "    {\"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"EQ\", \"value\": \"crm\"},\n" +
                "    {\"type\": \"RAW_FIELD\", \"field\": \"product_category\", \"operator\": \"EQ\", \"value\": \"electronics\"}\n" +
                "  ]\n" +
                "}";
        assertTrue(FilterEvaluator.evaluate(mapper.readTree(orFilter), event));

        // NOT
        String notFilter = "{\n" +
                "  \"operator\": \"NOT\",\n" +
                "  \"children\": [\n" +
                "    {\"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"EQ\", \"value\": \"crm\"}\n" +
                "  ]\n" +
                "}";
        assertTrue(FilterEvaluator.evaluate(mapper.readTree(notFilter), event));
    }
}


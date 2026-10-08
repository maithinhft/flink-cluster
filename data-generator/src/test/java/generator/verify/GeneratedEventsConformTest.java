package generator.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import generator.events.EventConfig;
import generator.events.EventFactory;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** With --dirty-rate 0 every generated event must pass its schema, otherwise clean data silently lands in the DLQ. */
class GeneratedEventsConformTest {

    @Test
    void cleanEventsConformToTheirSchemas() throws Exception {
        ObjectMapper m = new ObjectMapper();
        Map<String, JsonNode> schemas = new HashMap<>();
        for (String dir : new String[]{"crm", "ecommerce", "payment"}) {
            try (Stream<Path> s = Files.list(Paths.get("schema", dir))) {
                for (Path p : (Iterable<Path>) s::iterator) {
                    JsonNode n = m.readTree(p.toFile());
                    schemas.put(n.get("source_name").asText() + "_" + n.get("action").asText(), n);
                }
            }
        }
        EventConfig cfg = EventConfig.parse(new String[]{"--dirty-rate", "0"});
        Random r = new Random(7);
        Map<String, Integer> failures = new TreeMap<>();
        for (int i = 0; i < 20_000; i++) {
            JsonNode ev = m.valueToTree(EventFactory.generateEvent(i, "customer-1", r, cfg));
            String type = ev.get("source_system").asText() + "_" + ev.get("event_type").asText();
            if (!BatchSemantics.conforms(ev, schemas.get(type))) {
                failures.merge(type, 1, Integer::sum);
            }
        }
        assertTrue(failures.isEmpty(), "non-conforming clean events: " + failures);
    }
}

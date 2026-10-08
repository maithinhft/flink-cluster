package generator.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the data-driven metric definitions published to schema_definitions:
 * a metric named "..._where_&lt;field&gt;_&lt;value&gt;" must really filter on that condition, filters may only
 * reference declared fields, and a metric_id must be unique across schemas (it is a global alias).
 */
class SchemaMetricsConsistencyTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static List<Path> schemaFiles() throws IOException {
        List<Path> out = new ArrayList<>();
        for (String dir : new String[]{"crm", "ecommerce", "payment"}) {
            try (Stream<Path> s = Files.list(Paths.get("schema", dir))) {
                s.filter(p -> p.toString().endsWith(".json")).sorted().forEach(out::add);
            }
        }
        return out;
    }

    private static void leaves(JsonNode f, Set<String> conds, Set<String> fields) {
        if (f == null || f.isNull()) {
            return;
        }
        if (f.has("children")) {
            for (JsonNode c : f.get("children")) {
                leaves(c, conds, fields);
            }
        } else if (f.has("field")) {
            fields.add(f.get("field").asText());
            conds.add(f.get("field").asText() + "_" + f.path("value").asText());
        }
    }

    @Test
    void metricNamesMatchFiltersAndIdsAreUnique() throws Exception {
        List<String> problems = new ArrayList<>();
        Map<String, Path> owners = new HashMap<>();
        for (Path p : schemaFiles()) {
            JsonNode schema = M.readTree(p.toFile());
            Set<String> declared = new HashSet<>();
            schema.get("fields").fieldNames().forEachRemaining(declared::add);
            for (JsonNode m : schema.path("metrics")) {
                String id = m.get("metric_id").asText();
                Path prev = owners.put(id, p);
                if (prev != null) {
                    problems.add("duplicate metric_id " + id + " in " + prev + " and " + p);
                }
                Set<String> conds = new HashSet<>();
                Set<String> fields = new HashSet<>();
                leaves(m.get("filter"), conds, fields);
                int where = id.indexOf("_where_");
                if (where >= 0) {
                    String cond = id.substring(where + 7);
                    if (conds.stream().noneMatch(c -> c.equalsIgnoreCase(cond))) {
                        problems.add(p.getFileName() + ": " + id + " does not filter on " + cond);
                    }
                }
                for (String f : fields) {
                    if (!declared.contains(f)) {
                        problems.add(p.getFileName() + ": " + id + " filters on undeclared field " + f);
                    }
                }
                String source = m.path("source_field").asText("event_id");
                if (!declared.contains(source)) {
                    problems.add(p.getFileName() + ": " + id + " uses undeclared source_field " + source);
                }
            }
        }
        assertTrue(problems.isEmpty(), String.join("\n", problems.subList(0, Math.min(20, problems.size()))));
    }
}

package generator.events;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class EventConfigTest {

    @Test
    public void testDefaultConfigIsDualCluster() {
        EventConfig config = EventConfig.parse(new String[]{"--num-events", "100"});
        config.validate();

        assertEquals("dual", config.cluster);
        assertEquals(100L, config.numEvents);
        assertNotNull(config.plainBootstrapServers);
        assertNotNull(config.gssapiBootstrapServers);
        assertEquals("events", config.topic);
    }

    @Test
    public void testEventFactoryGeneratesValidSources() {
        EventConfig config = EventConfig.parse(new String[]{"--num-events", "100"});
        EntityPool entityPool = new EntityPool(10, 0.5);
        Random random = new Random(42);

        boolean seenCrm = false;
        boolean seenEcom = false;
        boolean seenPayment = false;

        for (int i = 0; i < 100; i++) {
            String entityId = entityPool.next(random);
            Map<String, Object> event = EventFactory.generateEvent(i, entityId, random, config);

            assertNotNull(event.get("event_id"));
            assertNotNull(event.get("entity_id"));
            String source = (String) event.get("source_system");
            assertNotNull(source);

            if ("crm".equals(source)) seenCrm = true;
            if ("ecommerce".equals(source)) seenEcom = true;
            if ("payment".equals(source)) seenPayment = true;
        }

        assertTrue(seenCrm, "Should generate crm events");
        assertTrue(seenEcom, "Should generate ecommerce events");
        assertTrue(seenPayment, "Should generate payment events");
    }

    @Test
    public void testProducerPropertiesGeneration() {
        EventConfig config = EventConfig.parse(new String[]{"--num-events", "100"});
        var plainProps = config.getPlainProducerProperties();
        assertEquals("SASL_PLAINTEXT", plainProps.getProperty("security.protocol"));
        assertEquals("PLAIN", plainProps.getProperty("sasl.mechanism"));
        assertNotNull(plainProps.getProperty("sasl.jaas.config"));

        var gssapiProps = config.getGssapiProducerProperties();
        assertEquals("SASL_PLAINTEXT", gssapiProps.getProperty("security.protocol"));
        assertEquals("GSSAPI", gssapiProps.getProperty("sasl.mechanism"));
        assertNotNull(gssapiProps.getProperty("sasl.jaas.config"));
    }
}


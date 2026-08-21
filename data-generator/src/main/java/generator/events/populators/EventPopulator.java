package generator.events.populators;

import java.util.Map;
import java.util.Random;

public interface EventPopulator {
    void populate(Map<String, Object> event, String eventType, Random random, boolean isDirty, int dirtyType);
}

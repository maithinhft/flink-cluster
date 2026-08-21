package generator.events;

import java.util.Random;

public class EntityPool {
    private final String[] entities;
    private final int hotEntityCount;
    private final double skew;

    public EntityPool(int numEntities, double skew) {
        entities = new String[numEntities];
        for (int i = 0; i < numEntities; i++)
            entities[i] = "customer-" + i;
        hotEntityCount = Math.max(1, (int) (numEntities * 0.01));
        this.skew = skew;
    }

    public String next(Random random) {
        if (skew > 0 && random.nextDouble() < skew)
            return entities[random.nextInt(hotEntityCount)];
        return entities[random.nextInt(entities.length)];
    }
}

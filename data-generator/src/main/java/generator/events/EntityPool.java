package generator.events;

import java.util.Arrays;
import java.util.Random;

/**
 * Entity selection with two skew models:
 * <ul>
 *   <li>{@code zipfS > 0}: Zipf distribution, P(rank k) ~ 1/k^s (s = 0.8, 1.2 are typical benchmark points).</li>
 *   <li>otherwise {@code skew}: probability of picking one of the 1% "hot" entities.</li>
 * </ul>
 */
public class EntityPool {
    private final String[] entities;
    private final int hotEntityCount;
    private final double skew;
    private final double[] zipfCdf;

    public EntityPool(int numEntities, double skew) {
        this(numEntities, skew, 0.0);
    }

    public EntityPool(int numEntities, double skew, double zipfS) {
        entities = new String[numEntities];
        for (int i = 0; i < numEntities; i++)
            entities[i] = "customer-" + i;
        hotEntityCount = Math.max(1, (int) (numEntities * 0.01));
        this.skew = skew;
        if (zipfS > 0) {
            zipfCdf = new double[numEntities];
            double sum = 0;
            for (int k = 0; k < numEntities; k++) {
                sum += 1.0 / Math.pow(k + 1, zipfS);
                zipfCdf[k] = sum;
            }
            for (int k = 0; k < numEntities; k++) {
                zipfCdf[k] /= sum;
            }
        } else {
            zipfCdf = null;
        }
    }

    public String next(Random random) {
        if (zipfCdf != null) {
            int idx = Arrays.binarySearch(zipfCdf, random.nextDouble());
            if (idx < 0) idx = -idx - 1;
            return entities[Math.min(idx, entities.length - 1)];
        }
        if (skew > 0 && random.nextDouble() < skew)
            return entities[random.nextInt(hotEntityCount)];
        return entities[random.nextInt(entities.length)];
    }
}

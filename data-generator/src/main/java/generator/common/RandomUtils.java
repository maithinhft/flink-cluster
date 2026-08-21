package generator.common;

import java.util.Random;

public class RandomUtils {

    public static final Random RANDOM = new Random(12345L);

    @SafeVarargs
    public static <T> T randomElement(T... elements) {
        if (elements == null || elements.length == 0) return null;
        return elements[RANDOM.nextInt(elements.length)];
    }

    public static int randomInt(int... elements) {
        if (elements == null || elements.length == 0) return 0;
        return elements[RANDOM.nextInt(elements.length)];
    }

    public static long randomLong(long... elements) {
        if (elements == null || elements.length == 0) return 0L;
        return elements[RANDOM.nextInt(elements.length)];
    }
}

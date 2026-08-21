package generator.events.populators;

import generator.common.RandomUtils;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Random;

public class CrmPopulator implements EventPopulator {

    private static final String[] COUNTRIES = { "VN", "US", "JP", "KR", "SG" };

    @Override
    public void populate(Map<String, Object> event, Random random, boolean isDirty, int dirtyType) {
        event.put("user_first_name", "User" + random.nextInt(1000));
        event.put("user_last_name", "Name" + random.nextInt(1000));

        if (dirtyType == 3 && random.nextBoolean()) {
            event.put("user_email", "invalid-email-format");
        } else {
            event.put("user_email", "user" + random.nextInt(10000) + "@example.com");
        }

        event.put("user_phone", "+8498" + random.nextInt(10000000));
        event.put("user_gender", RandomUtils.randomElement("M", "F", "O"));
        event.put("user_date_of_birth", "19" + (50 + random.nextInt(50)) + "-01-01");
        event.put("user_nationality", RandomUtils.randomElement(COUNTRIES));

        event.put("account_id", "acc-" + random.nextInt(50000));
        event.put("account_status", RandomUtils.randomElement("active", "suspended", "closed"));
        event.put("registration_date", Instant.now().minus(random.nextInt(365), ChronoUnit.DAYS).toString());
        event.put("last_login_date", Instant.now().minus(random.nextInt(10), ChronoUnit.DAYS).toString());

        event.put("subscription_tier", RandomUtils.randomElement("free", "basic", "premium"));

        if (dirtyType == 2 && random.nextBoolean()) {
            event.put("loyalty_points", -100);
        } else {
            event.put("loyalty_points", random.nextInt(5000));
        }

        event.put("opt_in_email", random.nextBoolean());
        event.put("opt_in_sms", random.nextBoolean());
        event.put("opt_in_push", random.nextBoolean());

        event.put("support_ticket_id", "tck-" + random.nextInt(10000));
        event.put("support_ticket_status", RandomUtils.randomElement("open", "in_progress", "resolved"));
        event.put("satisfaction_score", 1 + random.nextInt(5));
    }
}

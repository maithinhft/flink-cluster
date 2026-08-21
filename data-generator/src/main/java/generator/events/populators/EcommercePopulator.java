package generator.events.populators;

import generator.common.RandomUtils;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Random;

public class EcommercePopulator implements EventPopulator {

    private static final String[] COUNTRIES = { "VN", "US", "JP", "KR", "SG" };

    @Override
    public void populate(Map<String, Object> event, Random random, boolean isDirty, int dirtyType) {
        event.put("product_id", "prod-" + random.nextInt(10000));
        event.put("product_name", "Product " + random.nextInt(1000));
        event.put("product_category", RandomUtils.randomElement("electronics", "fashion", "food", "books"));
        event.put("product_subcategory", "subcat-" + random.nextInt(10));
        event.put("product_sku", "SKU-" + random.nextInt(10000));
        event.put("product_brand", "Brand-" + random.nextInt(20));

        if (dirtyType == 1 && random.nextBoolean()) {
            event.put("unit_price", "N/A"); // Type mismatch
        } else if (dirtyType == 2 && random.nextBoolean()) {
            event.put("unit_price", -50.0); // Out of bounds
        } else {
            event.put("unit_price", 10.0 + random.nextDouble() * 500.0);
        }

        event.put("quantity", 1 + random.nextInt(5));
        event.put("discount_amount", random.nextDouble() * 10.0);
        event.put("discount_code", "SUMMER" + random.nextInt(100));
        event.put("tax_amount", 5.0 + random.nextDouble() * 20.0);

        double uPrice = event.get("unit_price") instanceof Double ? (Double) event.get("unit_price") : 100.0;
        int qty = (Integer) event.get("quantity");
        event.put("total_amount", uPrice * qty);

        event.put("currency", RandomUtils.randomElement("USD", "VND"));
        event.put("cart_id", "cart-" + random.nextInt(100000));
        event.put("order_id", "ord-" + random.nextInt(100000));
        event.put("order_status", RandomUtils.randomElement("created", "paid", "shipped", "completed", "cancelled"));
        event.put("shipping_method", RandomUtils.randomElement("standard", "express"));
        event.put("shipping_cost", random.nextDouble() * 15.0);

        event.put("delivery_address_line1", random.nextInt(9999) + " Main St");
        event.put("delivery_city", "City-" + random.nextInt(50));
        event.put("delivery_country", RandomUtils.randomElement(COUNTRIES));
        event.put("estimated_delivery_date",
                Instant.now().plus(random.nextInt(7), ChronoUnit.DAYS).toString().substring(0, 10));

        event.put("billing_address_line1", event.get("delivery_address_line1"));
        event.put("billing_city", event.get("delivery_city"));
        event.put("billing_country", event.get("delivery_country"));

        event.put("merchant_id", "merch-" + random.nextInt(100));
        event.put("store_id", "store-" + random.nextInt(10));
        event.put("warranty_period_months", RandomUtils.randomInt(0, 12, 24));
    }
}

package generator.events.populators;

import generator.common.RandomUtils;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Random;

public class EcommercePopulator implements EventPopulator {

    private static final String[] COUNTRIES = { "VN", "US", "JP", "KR", "SG" };

    @Override
    public void populate(Map<String, Object> event, String eventType, Random random, boolean isDirty, int dirtyType) {
        
        switch (eventType) {
            case "add_to_cart":
            case "remove_from_cart":
                event.put("cart_id", "cart-" + random.nextInt(100000));
                populateProductDetails(event, random, isDirty, dirtyType);
                break;

            case "order_created":
            case "purchase":
                event.put("order_id", "ord-" + random.nextInt(100000));
                event.put("cart_id", "cart-" + random.nextInt(100000));
                if ("order_created".equals(eventType)) {
                    event.put("order_status", "created");
                } else {
                    event.put("order_status", "paid");
                    event.put("discount_amount", random.nextDouble() * 10.0);
                    event.put("discount_code", "SUMMER" + random.nextInt(100));
                    event.put("tax_amount", 5.0 + random.nextDouble() * 20.0);
                }
                
                populateProductDetails(event, random, isDirty, dirtyType);
                
                double uPrice = event.get("unit_price") instanceof Double ? (Double) event.get("unit_price") : 100.0;
                int qty = (Integer) event.get("quantity");
                event.put("total_amount", uPrice * qty);
                event.put("currency", RandomUtils.randomElement("USD", "VND"));
                
                populateAddressDetails(event, random);
                event.put("merchant_id", "merch-" + random.nextInt(100));
                event.put("store_id", "store-" + random.nextInt(10));
                break;

            case "order_shipped":
            case "order_completed":
            case "order_cancelled":
                event.put("order_id", "ord-" + random.nextInt(100000));
                if ("order_shipped".equals(eventType)) {
                    event.put("order_status", "shipped");
                    event.put("shipping_method", RandomUtils.randomElement("standard", "express"));
                    event.put("shipping_cost", random.nextDouble() * 15.0);
                    event.put("estimated_delivery_date",
                            Instant.now().plus(random.nextInt(7), ChronoUnit.DAYS).toString().substring(0, 10));
                } else if ("order_completed".equals(eventType)) {
                    event.put("order_status", "completed");
                } else {
                    event.put("order_status", "cancelled");
                }
                break;
        }
    }

    private void populateProductDetails(Map<String, Object> event, Random random, boolean isDirty, int dirtyType) {
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
        event.put("warranty_period_months", RandomUtils.randomInt(0, 12, 24));
    }

    private void populateAddressDetails(Map<String, Object> event, Random random) {
        event.put("delivery_address_line1", random.nextInt(9999) + " Main St");
        event.put("delivery_city", "City-" + random.nextInt(50));
        event.put("delivery_country", RandomUtils.randomElement(COUNTRIES));
        event.put("billing_address_line1", event.get("delivery_address_line1"));
        event.put("billing_city", event.get("delivery_city"));
        event.put("billing_country", event.get("delivery_country"));
    }
}

package generator.events.populators;

import generator.common.RandomUtils;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class PaymentPopulator implements EventPopulator {

    private static final String[] PAYMENT_GATEWAYS = { "stripe", "paypal", "vnpay", "momo" };
    private static final String[] PAYMENT_METHODS = { "credit_card", "wallet", "bank_transfer" };
    private static final String[] CARD_NETWORKS = { "visa", "mastercard", "amex", "napas" };

    @Override
    public void populate(Map<String, Object> event, String eventType, Random random, boolean isDirty, int dirtyType) {
        event.put("transaction_id", "txn-" + UUID.randomUUID().toString().substring(0, 8));
        
        switch (eventType) {
            case "payment_initiated":
                event.put("payment_gateway", RandomUtils.randomElement(PAYMENT_GATEWAYS));
                event.put("payment_method", RandomUtils.randomElement(PAYMENT_METHODS));
                event.put("card_network", RandomUtils.randomElement(CARD_NETWORKS));
                event.put("bank_name", "Bank-" + random.nextInt(10));
                event.put("account_number_hash", "hash-" + random.nextInt(999999));
                event.put("transaction_status", "pending");
                break;

            case "payment_success":
            case "payment_failed":
                event.put("payment_gateway", RandomUtils.randomElement(PAYMENT_GATEWAYS));
                event.put("payment_method", RandomUtils.randomElement(PAYMENT_METHODS));
                if ("payment_success".equals(eventType)) {
                    event.put("transaction_status", "success");
                } else {
                    event.put("transaction_status", "failed");
                    event.put("payment_error_message", random.nextDouble() < 0.1 ? "Insufficient funds" : "Timeout");
                }
                event.put("is_3ds_verified", random.nextBoolean());
                event.put("billing_zip_match", random.nextDouble() < 0.9);
                break;

            case "refund":
            case "chargeback":
                event.put("payment_gateway", RandomUtils.randomElement(PAYMENT_GATEWAYS));
                event.put("transaction_status", "payment_success".equals(eventType) ? "refunded" : "chargeback");
                break;
        }
    }
}

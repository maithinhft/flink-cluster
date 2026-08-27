package generator.rules;

import generator.common.EnvLoader;

public class RuleConfig {

    // ============================================================
    // PostgreSQL configuration
    // ============================================================
    private static final String IP = EnvLoader.get("SERVER_IP", "127.0.0.1");
    private static final String PORT = EnvLoader.get("POSTGRES_PORT", "5433");
    private static final String DB = EnvLoader.get("POSTGRES_DB", "realtime_core");
    
    public static final String DEFAULT_DB_URL = "jdbc:postgresql://" + IP + ":" + PORT + "/" + DB;
    public static final String DEFAULT_DB_USER = EnvLoader.get("POSTGRES_USER", "postgres");
    public static final String DEFAULT_DB_PASSWORD = EnvLoader.get("POSTGRES_PASSWORD", "postgres");
    public static final int DEFAULT_NUM_RULES = 100_000;
    public static final int DEFAULT_BATCH_SIZE = 5_000;

    // ============================================================
    // Dictionaries matching EventGenerator concrete schema
    // ============================================================
    public static final String[] TAG_FIELDS = {
            "product_category", "payment_method", "order_status",
            "account_status", "subscription_tier", "support_ticket_status", "transaction_status"
    };

    public static final String[][] TAG_VALUES = {
            { "electronics", "fashion", "food", "books" },
            { "credit_card", "wallet", "bank_transfer" },
            { "created", "paid", "shipped", "completed", "cancelled" },
            { "active", "suspended", "closed" },
            { "free", "basic", "premium" },
            { "open", "in_progress", "resolved" },
            { "pending", "success", "failed" }
    };

    public static final String[] NUM_FIELDS = {
            "total_amount", "loyalty_points", "quantity",
            "satisfaction_score", "unit_price", "discount_amount", "tax_amount"
    };

    public static final String[] NUMERIC_OPS = { "GT", "GTE", "LT", "LTE" };

    public static final String[] EVENT_TYPES = {
            "add_to_cart", "remove_from_cart", "order_created", "order_completed", "order_cancelled", "order_shipped", "purchase",
            "account_created", "account_status_change", "profile_update", "login", "logout", "subscription_change", "support_ticket_created", "support_ticket_resolved",
            "payment_initiated", "payment_success", "payment_failed", "refund", "chargeback"
    };

    public static final String[] RULE_PREFIXES = {
            "high_value_segment", "active_user_filter", "churn_risk_detect",
            "vip_upgrade_check", "engagement_score_rule", "purchase_frequency",
            "retention_campaign", "cross_sell_trigger", "loyalty_tier_eval",
            "dormant_reactivation", "geo_target_segment", "device_pref_rule"
    };
}

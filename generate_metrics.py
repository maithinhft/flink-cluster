import json, glob, os

# Definitions from RuleConfig.java
FUNCTIONS = ["SUM", "COUNT", "AVG", "MAX", "MIN"]
NUM_FIELDS = ["total_amount", "loyalty_points", "quantity", "satisfaction_score", "unit_price", "discount_amount", "tax_amount"]

TAG_FIELDS = ["product_category", "payment_method", "order_status", "account_status", "subscription_tier", "support_ticket_status", "transaction_status", "payment_gateway"]
TAG_VALUES = {
    "product_category": ["electronics", "fashion", "food", "books"],
    "payment_method": ["credit_card", "wallet", "bank_transfer"],
    "order_status": ["created", "paid", "shipped", "completed", "cancelled"],
    "account_status": ["active", "suspended", "closed"],
    "subscription_tier": ["free", "basic", "premium"],
    "support_ticket_status": ["open", "in_progress", "resolved"],
    "transaction_status": ["pending", "success", "failed"],
    "payment_gateway": ["stripe", "paypal", "vnpay", "momo"]
}

files = glob.glob("data-generator/schema/**/*.json", recursive=True)

count = 0
for filepath in files:
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
            
        # Do not process rule_schema.json
        if "rule_schema" in filepath:
            continue
            
        if "fields" not in data:
            continue
            
        schema_fields = data["fields"]
        metrics = []
        
        # 1. Add event counting metrics (for event_type and source_system aggregation)
        if "event_id" in schema_fields:
            metrics.append({
                "metric_id": "count_events",
                "source_field": "event_id",
                "aggregation": "COUNT",
                "filter": None
            })
            
            for tag_field in TAG_FIELDS:
                if tag_field not in schema_fields:
                    continue
                    
                schema_allowed = schema_fields[tag_field].get("allowed_values", [])
                if not schema_allowed:
                    continue
                    
                config_allowed = TAG_VALUES[tag_field]
                valid_values = set(schema_allowed).intersection(set(config_allowed))
                
                for val in valid_values:
                    filter_obj = {
                        "type": "RAW_FIELD",
                        "field": tag_field,
                        "operator": "EQ",
                        "value": val
                    }
                    
                    metric_id = f"count_events_where_{tag_field}_{val}"
                    metrics.append({
                        "metric_id": metric_id,
                        "source_field": "event_id",
                        "aggregation": "COUNT",
                        "filter": filter_obj
                    })
        
        # 2. Add numeric field metrics
        for num_field in NUM_FIELDS:
            if num_field not in schema_fields:
                continue
                
            for func in FUNCTIONS:
                # 1. Add unfiltered metric
                metrics.append({
                    "metric_id": f"{func.lower()}_{num_field}",
                    "source_field": num_field,
                    "aggregation": func,
                    "filter": None
                })
                
                # 2. Add filtered metrics for all tag fields present in this schema
                for tag_field in TAG_FIELDS:
                    if tag_field not in schema_fields:
                        continue
                        
                    schema_allowed = schema_fields[tag_field].get("allowed_values", [])
                    if not schema_allowed:
                        continue
                        
                    config_allowed = TAG_VALUES[tag_field]
                    
                    # Intersect allowed values in config with allowed values in schema
                    valid_values = set(schema_allowed).intersection(set(config_allowed))
                    
                    for val in valid_values:
                        filter_obj = {
                            "type": "RAW_FIELD",
                            "field": tag_field,
                            "operator": "EQ",
                            "value": val
                        }
                        
                        metric_id = f"{func.lower()}_{num_field}_where_{tag_field}_{val}"
                        metrics.append({
                            "metric_id": metric_id,
                            "source_field": num_field,
                            "aggregation": func,
                            "filter": filter_obj
                        })
                        
        data["metrics"] = metrics
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        count += 1
        print(f"Updated {filepath} with {len(metrics)} metrics")
        
    except Exception as e:
        print(f"Error processing {filepath}: {e}")

print(f"\nSuccessfully generated metrics for {count} files.")

import json

with open("data-generator/schema/rule_schema.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# 1. Update rule_json_structure
agg_node = data["rule_json_structure"]["Node"]["LeafNode_Aggregation"]
if "source_system" in agg_node:
    del agg_node["source_system"]
if "event_type" in agg_node:
    del agg_node["event_type"]

if "filter" in agg_node:
    agg_node["filter"]["description"] = "Điều kiện lọc event trước khi tổng hợp (tùy chọn). Cấu trúc giống Node (có thể là LogicalNode hoặc LeafNode_RawField)."
    agg_node["filter"]["type"] = "Node"

# 2. Update example_rules
def transform_node(node):
    if not isinstance(node, dict):
        return

    if "children" in node:
        for child in node["children"]:
            transform_node(child)
            
    if node.get("type") == "AGGREGATION":
        filters = []
        if "source_system" in node:
            filters.append({
                "type": "RAW_FIELD",
                "field": "source_system",
                "operator": "EQ",
                "value": node.pop("source_system")
            })
        if "event_type" in node:
            filters.append({
                "type": "RAW_FIELD",
                "field": "event_type",
                "operator": "EQ",
                "value": node.pop("event_type")
            })
            
        if "filter" in node:
            existing_filter = node.pop("filter")
            filters.append(existing_filter)
            
        if len(filters) == 1:
            node["filter"] = filters[0]
        elif len(filters) > 1:
            node["filter"] = {
                "operator": "AND",
                "children": filters
            }

for rule_name, rule_body in data["example_rules"].items():
    transform_node(rule_body)

with open("data-generator/schema/rule_schema.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4, ensure_ascii=False)

print("Schema updated successfully!")

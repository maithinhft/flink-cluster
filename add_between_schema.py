import json

with open("data-generator/schema/rule_schema.json", "r", encoding="utf-8") as f:
    data = json.load(f)

agg_node = data["rule_json_structure"]["Node"]["LeafNode_Aggregation"]
agg_node["operator"]["allowed_values"].append("BETWEEN")
agg_node["value"] = {
    "type": "any",
    "description": "Ngưỡng giá trị để so sánh với kết quả tổng hợp. Có thể là một số (number) hoặc mảng 2 phần tử [min, max] nếu dùng toán tử BETWEEN."
}

with open("data-generator/schema/rule_schema.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4, ensure_ascii=False)

print("Schema updated successfully!")

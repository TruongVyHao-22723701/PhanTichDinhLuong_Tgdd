import json
from pymongo import MongoClient

# Kết nối MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["Tgdd_DB1"]
collection = db["Tgdd_Crawler1"]

# Đọc file JSON
with open(r'C:/Users/T&T/thegioididong/thegioididong/spiders/DHDT_INFO.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Chèn dữ liệu vào MongoDB
collection.insert_many(data)

print("Dữ liệu JSON đã được chèn vào MongoDB thành công!")

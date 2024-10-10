# %%
import pandas as pd
import re
import pymongo

# %%
# Kết nối tới MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")  # Thay thế bằng địa chỉ MongoDB của bạn
db = client["TGDD_CRAWLER"]  # Tên database của bạn
collection = db["DHDT_Collection"]  # Tên collection chứa dữ liệu

# %%
# Trích xuất dữ liệu từ MongoDB và bỏ qua trường '_id'
cursor = collection.find({}, {'_id': 0})

# Chuyển dữ liệu từ MongoDB sang DataFrame của Pandas
df = pd.DataFrame(list(cursor))

# Hiển thị 5 dòng đầu tiên
df.head()

# %%
def process_name(name):
    # Loại bỏ từ "Mẫu mới" nếu có
    name = name.replace("Mẫu mới", "").strip()
    
    # Loại bỏ các chuỗi kích thước như 'xx mm', 'xx x yy mm', 'xx.x mm', hoặc 'xx × yy mm' và các thông tin giới tính
    clean_name = re.sub(r'\d+(\.\d+)?( x \d+(\.\d+)?| × \d+(\.\d+)?)*\s*mm|Nam|Nữ|Trẻ em|Unisex', '', name).strip()

    # Loại bỏ dấu gạch nối hoặc khoảng trắng không cần thiết
    clean_name = re.sub(r'\s*-\s*', '', clean_name).strip()

    # Loại bỏ từ "Eco-Drive" nếu có ở cuối chuỗi
    clean_name = re.sub(r'Eco-Drive$', '', clean_name).strip()

    # Tìm mã sản phẩm dựa trên định dạng mã (các chữ và số cuối)
    product_code = re.search(r'[A-Z0-9\/\-\.]+$', clean_name)
    
    # Tách tên sản phẩm và mã sản phẩm
    if product_code:
        name_only = clean_name.replace(product_code.group(), '').strip()
        code = product_code.group()
    else:
        name_only = clean_name
        code = "Unknown"
    
    return name_only, code

# Áp dụng hàm xử lý cho cột 'Name'
df['Tên sản phẩm'], df['Mã sản phẩm'] = zip(*df['Name'].apply(process_name))

# Đổi vị trí các cột nếu cần thiết (thêm vào đầu hoặc cuối DataFrame)
df = df[['Tên sản phẩm', 'Mã sản phẩm'] + [col for col in df.columns if col not in ['Tên sản phẩm', 'Mã sản phẩm']]]

# %%
# Xóa cột 'Name' khỏi dataframe
df.drop(columns=['Name'], inplace=True)

# %%
df.head(5)

# %%
# Hàm xử lý cột 'Promotion'
def process_promotion(promotion):
    # Tìm mã giảm giá
    discount_code = re.search(r'mã (\w+)', promotion)
    discount_code = discount_code.group(1) if discount_code else None

    # Tìm mức giảm tối thiểu và tối đa
    min_discount = re.search(r'giảm từ (\d{1,3}(?:,\d{3})*|\d+)', promotion)
    max_discount = re.search(r'đến (\d{1,3}(?:,\d{3})*|\d+)', promotion)

    # Chuyển đổi thành số nguyên
    min_discount = int(min_discount.group(1).replace(',', '')) if min_discount else None
    max_discount = int(max_discount.group(1).replace(',', '')) if max_discount else None

    # Tìm phương thức thanh toán khuyến mãi
    payment_method = re.search(r'thanh toán qua (\w+-\w+)', promotion)
    payment_method = payment_method.group(1) if payment_method else None
    
    return discount_code, min_discount, max_discount, payment_method

# Áp dụng hàm xử lý cho cột 'Promotion' để tạo các cột mới
df['Mã giảm giá'], df['Mức giảm tối thiểu'], df['Mức giảm tối đa'], df['Phương thức thanh toán nhận khuyến mãi'] = zip(*df['Promotion'].apply(process_promotion))



# %%
df.drop(columns="Promotion", inplace=True)

# %%
# Đưa các cột mới vào vị trí trước cột 'Old Price'
cols = df.columns.tolist()
new_order = cols[:2] + cols[-4:] + cols[2:-4]  # Đưa các cột mới vào vị trí trước cột 'Old Price'
df = df[new_order]

# %%
df.head(3)

# %%
# Hàm xử lý thời gian sử dụng pin
def process_battery_life(battery_life):
    if pd.isna(battery_life) or battery_life == '':
        return float('nan')  # Trả về NaN cho giá trị rỗng hoặc None
    
    months = 0  # Khởi tạo biến lưu số tháng
    if 'tháng' in battery_life:
        months_match = re.search(r'(\d+(\.\d+)?)', battery_life)  # Tìm số tháng
        if months_match:
            months += float(months_match.group(1))  # Cộng thêm số tháng (có thể là số thập phân)
    elif 'năm' in battery_life:
        years_match = re.search(r'(\d+(\.\d+)?)', battery_life)  # Tìm số năm
        if years_match:
            months += float(years_match.group(1)) * 12  # Chuyển đổi năm thành tháng
    
    return months  # Trả về số tháng dưới dạng float

# Xử lý cột 'Thời gian sử dụng pin'
df['Thời gian sử dụng pin:'] = df['Thời gian sử dụng pin:'].apply(process_battery_life)

# %%
# Đổi tên cột thành "Thời gian sử dụng pin (năm)"
df.rename(columns={'Thời gian sử dụng pin:': 'Thời gian sử dụng pin (tháng)'}, inplace=True)

# %%
df.head()

# %%


# %%
df.head()

# %%
# Hàm xử lý cột 'Kháng nước:'
def process_water_resistance(resistance):
    if pd.isna(resistance):  # Kiểm tra giá trị NaN
        return None
    
    match = re.match(r'(\d+)\s*ATM\s*-\s*(.*)', resistance)
    if match:
        level = int(match.group(1))  # Chuyển đổi mức kháng nước thành số
        return level
    return None

# Áp dụng hàm xử lý cho cột 'Kháng nước:'
df['Mức kháng nước']= df['Kháng nước:'].apply(process_water_resistance)

# Đổi tên cột 'Mức kháng nước' để thêm đơn vị 'ATM'
df.rename(columns={'Mức kháng nước': 'Mức kháng nước (ATM)'}, inplace=True)

# Xóa cột 'Kháng nước:' khỏi DataFrame
df.drop(columns=['Kháng nước:'], inplace=True)

# %%
df.head()

# %%
# Hàm xử lý cho cột 'Đường kính mặt:'
def process_diameter(diameter):
    if pd.isna(diameter) or diameter == 'N/A':
        return None  # Trả về None cho giá trị NaN hoặc 'N/A'
    if isinstance(diameter, str):  # Kiểm tra xem có phải là chuỗi không
        # Tìm tất cả các số và mm
        match = re.match(r'(\d+(\.\d+)?)\s*mm', diameter)
        if match:
            return float(match.group(1))  # Trả về giá trị đầu tiên
        # Nếu có định dạng như "xx x yy mm", ta có thể lấy giá trị đầu tiên
        match_dimension = re.search(r'(\d+(\.\d+)?)\s*x\s*(\d+(\.\d+)?)\s*mm', diameter)
        if match_dimension:
            return float(match_dimension.group(1))  # Trả về giá trị đầu tiên
    return None  # Trả về None nếu không tìm thấy

# Hàm xử lý cho cột 'Độ dày mặt:'
def process_thickness(thickness):
    if pd.isna(thickness) or thickness == 'N/A':
        return None  # Trả về None cho giá trị NaN hoặc 'N/A'
    if isinstance(thickness, str):  # Kiểm tra xem có phải là chuỗi không
        match = re.match(r'(\d+(\.\d+)?)\s*mm', thickness)
        if match:
            return float(match.group(1))  # Chuyển đổi thành số thực
    return None  # Trả về None cho các giá trị không hợp lệ

# Ghi đè lên cột cũ với giá trị đã xử lý
df['Đường kính mặt:'] = df['Đường kính mặt:'].apply(process_diameter)
df['Độ dày mặt:'] = df['Độ dày mặt:'].apply(process_thickness)

# %%
# Đổi tên các cột với đơn vị trong ngoặc
df.rename(columns={
    'Đường kính mặt:': 'Đường kính mặt (mm)',
    'Độ dày mặt:': 'Độ dày mặt (mm)'
}, inplace=True)

# %%
df.head()

# %%
# Thay thế các bình luận rỗng hoặc không ghi gì bằng "N/A"
df['Bình luận'] = df['Bình luận'].replace('', 'N/A')  # Thay thế chuỗi rỗng
df['Bình luận'] = df['Bình luận'].fillna('N/A')       # Thay thế giá trị None
df.head(20)

# %%
# Thứ tự cột mong muốn
new_order = [
    'Tên sản phẩm', 'Mã sản phẩm', 'Mã giảm giá', 
    'Mức giảm tối thiểu', 'Mức giảm tối đa', 
    'Phương thức thanh toán nhận khuyến mãi', 
    'Price', 'Old Price', 'Thời gian sử dụng pin (tháng)', 
    'Đối tượng sử dụng:', 'Đường kính mặt (mm)', 
    'Độ dày mặt (mm)', 'Nguồn năng lượng:', 
    'Mức kháng nước (ATM)', 
    'Chất liệu mặt kính:', 'Thương hiệu của:', 
    'Bình luận'
]

# Sắp xếp lại cột
df = df[new_order]
df.head()

# %%
# Xử lý tất cả các cột: thay thế các giá trị rỗng hoặc None bằng "N/A"
df.fillna('N/A', inplace=True)  # Thay thế giá trị None
df.replace('', 'N/A', inplace=True)  # Thay thế chuỗi rỗng

# %%
# Lưu DataFrame vào file Excel mới
df.to_excel('processed_data.xlsx', index=False)

print("Dữ liệu đã được lưu vào file 'processed_data.xlsx'.")

# %%
import pandas as pd
import json

# Giả sử bạn đã có DataFrame 'df' đã được xử lý

# Lưu dữ liệu vào file JSON
with open('DHDT_json_Processed.json', 'w', encoding='utf-8') as json_file:
    json.dump(df.to_dict(orient='records'), json_file, ensure_ascii=False, indent=4)

# Lưu DataFrame ra định dạng CSV
df.to_csv('DHDT_csv_Processed.csv', index=False, encoding='utf-8')

print("Dữ liệu đã được lưu vào file JSON và CSV.")




import json
import psycopg2

# Kết nối đến PostgreSQL
conn = psycopg2.connect(
    dbname="DongHoDeoTay_TGDD",
    user="postgres",
    password="123",
    host="localhost",
    port="5432"
)

# Tạo một cursor
cur = conn.cursor()

# Đọc dữ liệu từ file JSON chứa dữ liệu đã qua xử lý 
with open('C:/Users/T&T/thegioididong/Xulydulieu_TGDD/DHDT_json_Processed.json', 'r', encoding='utf-8') as file:
    data = json.load(file)


    
# Lưu dữ liệu vào bảng Product_Discount
for product in data:
    discount_code = product['Mã giảm giá']
    min_reduce = product['Mức giảm tối thiểu']
    max_reduce = product['Mức giảm tối đa']
    pay_method = product['Phương thức thanh toán nhận khuyến mãi']

    cur.execute("""
        INSERT INTO Product_Discount (DiscountCode, Min_Reduce, Max_Reduce, Pay_Method)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (DiscountCode) DO NOTHING;
    """, (discount_code, min_reduce, max_reduce, pay_method))






# Lưu dữ liệu vào bảng Brand (Thương hiệu)
brands = {product['Thương hiệu của:'] for product in data}  # Tạo tập hợp các thương hiệu duy nhất

for brand in brands:
    cur.execute("""
        INSERT INTO Brand (BrandName)
        VALUES (%s)
        ON CONFLICT (BrandName) DO NOTHING;
    """, (brand,))





# Lưu dữ liệu vào bảng Product
for product in data:
    # Lấy BrandID dựa trên thương hiệu
    cur.execute("""
        SELECT BrandID FROM Brand WHERE BrandName = %s LIMIT 1
    """, (product['Thương hiệu của:'],))
    brand_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO Product (ProductName, ProductCode, Price, OldPrice, Object_toUse, BrandID, DiscountID)
        VALUES (%s, %s, %s, %s, %s, %s, (SELECT DiscountID FROM Product_Discount WHERE DiscountCode = %s LIMIT 1))
        ON CONFLICT (ProductCode) DO NOTHING;
    """, (
        product['Tên sản phẩm'],
        product['Mã sản phẩm'],
        product['Price'],
        product['Old Price'],
        product['Đối tượng sử dụng:'],
        brand_id,
        product['Mã giảm giá']
    ))





    # Kiểm tra và chuyển đổi giá trị "N/A" thành NULL hoặc 0
    battery_usetime = product['Thời gian sử dụng pin (tháng)'] if product['Thời gian sử dụng pin (tháng)'] != "N/A" else None
    face_diameter = product['Đường kính mặt (mm)'] if product['Đường kính mặt (mm)'] != "N/A" else None
    face_thickness = product['Độ dày mặt (mm)'] if product['Độ dày mặt (mm)'] != "N/A" else None
    water_resistance_level = product['Mức kháng nước (ATM)'] if product['Mức kháng nước (ATM)'] != "N/A" else None

    cur.execute(""" 
        INSERT INTO Product_Details (ProductCode, Battery_usetime, Face_diameter, Face_thickness, Energy_source, Water_resistance_level, GlassFace_material)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ProductCode) DO NOTHING;
    """, (
        product['Mã sản phẩm'],
        battery_usetime,
        face_diameter,
        face_thickness,
        product['Nguồn năng lượng:'],
        water_resistance_level,
        product['Chất liệu mặt kính:']
    ))





    # Chèn dữ liệu vào bảng Review
    cur.execute("""
        INSERT INTO Review (ProductCode, Comment)
        VALUES (%s, %s)
    """, (
        product['Mã sản phẩm'],
        product['Bình luận']
    ))



# Commit các thay đổi
conn.commit()




# Đóng cursor và kết nối
cur.close()
conn.close()
#
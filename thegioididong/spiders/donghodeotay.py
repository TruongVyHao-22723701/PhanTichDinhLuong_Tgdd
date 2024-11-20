import scrapy
import json
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
from kafka import KafkaProducer

class DongHoDeoTaySpider(scrapy.Spider):
    name = "dongho_spider"
    start_urls = [f"https://www.thegioididong.com/Category/FilterProductBox?c=7264&pi=0"]
    base_url = "https://www.thegioididong.com"
    
    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US, en;q=0.9',
        'connection': 'keep-alive',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'x-requested-with': 'XMLHttpRequest',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
    }

    data = {
        "IsParentCate": "False",  # Chuyển đổi thành chuỗi
        "IsShowCompare": "False",  # Chuyển đổi thành chuỗi
        "prevent": "False"         # Chuyển đổi thành chuỗi
    }

    products = []
    # Khởi tạo Kafka Producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))


    def start_requests(self):
        for i in range(50):  # Lặp qua các trang
            url = f"https://www.thegioididong.com/Category/FilterProductBox?c=7264&pi={i}"
            yield scrapy.FormRequest(url, method='POST', headers=self.headers, formdata=self.data, callback=self.parse)

    def parse(self, response):
        json_response = json.loads(response.text)
        listproducts_html = json_response.get('listproducts', '')

        # Dùng BeautifulSoup để phân tích cú pháp HTML
        soup = BeautifulSoup(listproducts_html, 'html.parser')
        product_items = soup.find_all('li', class_='item __cate_7264')
        for item in product_items:
            product_name = item.find('h3', class_="fashionWatch-name").text.strip() if item.find('h3', class_="fashionWatch-name") else "Unknown"
            price = item.find('strong', class_="price").text.strip().replace('₫', '').replace('đ', '').replace('.', '').strip() if item.find('strong', class_="price") else "Unknown"
            old_price = item.find('p', class_="price-old black").text.strip().replace('₫', '').replace('đ', '').replace('.', '').strip() if item.find('p', class_="price-old black") else "Unknown"
            #float
            price = float(price) if price.isnumeric() else 0.0
            
            old_price = float(old_price) if old_price.isnumeric() else 0.0
            product_url = self.base_url + item.find('a').get('href', '') if item.find('a') else None
            # Lấy product_id từ thuộc tính data-id của thẻ <li>
            product_id = item.get('data-id', 'Unknown')  # Lấy 'data-id' từ <li>

            # Gửi yêu cầu đến URL sản phẩm để lấy thông tin chi tiết
            if product_url:
                yield scrapy.Request(product_url, callback=self.parse_product_detail, meta={'product_id': product_id,'name': product_name, 'price': price, 'old_price': old_price})

    def parse_product_detail(self, response):
        # Thông tin chi tiết sản phẩm
        product_id = response.meta['product_id']
        product_name = response.meta['name']
        price = response.meta['price']
        old_price = response.meta['old_price']

        # Lấy URL chi tiết sản phẩm
        detail_url = response.url
        
        # Gửi yêu cầu đến detail_url để lấy thông tin chi tiết
        res = requests.get(detail_url, headers=self.headers)
        soup = BeautifulSoup(res.text, 'lxml')        
        # Lấy các thông tin khác
        thongtin = soup.find(class_="text-specifi active")
        if thongtin:
            thongtin = thongtin.find_all('li')
            
            # Tạo dictionary để lưu thông tin chi tiết
            product_info = {
                'Product ID':product_id,
                'Name': product_name,
                'Price': price,
                'Old Price': old_price
            }
            
            # Các thuộc tính cần truy xuất cụ thể
            target_attributes = {
                'Thời gian sử dụng pin:': "N/A",
                'Đối tượng sử dụng:': "N/A",
                'Đường kính mặt:': "N/A",
                'Độ dày mặt:': "N/A",
                'Kháng nước:': "N/A",
                'Nguồn năng lượng:': "N/A",
                'Chất liệu mặt kính:': "N/A",
                'Thương hiệu của:': "N/A",
                'Hãng:':"N/A"
            }

            for item in thongtin:
                strong = item.find('strong')
                
                # Tìm các thẻ <span> hoặc <a>
                span_elements = item.find_all(['span', 'a'])
                
                if strong and span_elements:
                    key = strong.text.strip()
                    
                    # Nếu strong.text là một trong những thuộc tính cần tìm
                    if key in target_attributes:
                        # Lấy nội dung text từ <span>, bỏ qua các thẻ có class "parameter__manu"
                        values = [
                            element.text.strip()
                            for element in span_elements
                            if "parameter__manu" not in element.get("class", [])
                        ]
                        # Xóa cụm từ "Xem thông tin hãng" nếu xuất hiện
                        cleaned_values = [value.replace("Xem thông tin hãng", "").strip() for value in values]
                        value = ', '.join(cleaned_values)  # Kết hợp các giá trị thành một chuỗi                      
                        # Lưu giá trị vào target_attributes
                        target_attributes[key] = value
            # Cập nhật product_info với thông tin chi tiết
            product_info.update(target_attributes)
            # **Lấy số lượng sản phẩm đã bán**
            quantity_sale = soup.find('span', class_='quantity-sale')
            if quantity_sale:
                product_info['Quantity Sale'] = quantity_sale.text.strip().replace('Đã bán', '').strip()
            else:
                product_info['Quantity Sale'] = '0'
            
            # **Lấy thông tin giảm giá (Discount)**
            discount = soup.find('p', class_='box-price-percent')
            if discount:
                product_info['Discount'] = discount.text.strip()
            else:
                product_info['Discount'] = 'N/A'
            # **Lấy tỉ lệ đánh giá và số lượng đánh giá**
            rating = soup.find('div', class_='box-star')
            if rating:
                # Tỉ lệ đánh giá
                rating_value = rating.find('p').text.strip()  # Lấy giá trị số sao
                product_info['Rating'] = rating_value
            else:
                product_info['Rating']='None'

            # Số lượng đánh giá
            rating = soup.find('div', class_='box-star')
            if rating:
                review_count = rating.find('a', class_='total-cmtrt')
                if review_count:
                    product_info['Review Count'] = review_count.text.strip()
                else:
                    product_info['Review Count'] = '0'  # Nếu không tìm thấy, gán mặc định là 0
            else:
                product_info['Review Count'] = '0'  # Nếu không tìm thấy phần tử rating, gán mặc định là 0
                
            # Lấy bình luận từ trang sản phẩm
            comments = soup.find_all('li', class_='par')
            if comments:  # Nếu có bình luận
                longest_comment = ''
                for comment in comments:
                    comment_text = comment.find('p', class_='cmt-txt').text.strip()
                    
                    # Loại bỏ ký tự đặc biệt nhưng giữ lại dấu câu
                    comment_text = re.sub(r'[^\w\s.,!?-]', '', comment_text) 

                    if len(comment_text) > len(longest_comment):
                        longest_comment = comment_text

                # Lưu bình luận dài nhất vào product_info
                product_info['Bình luận'] = longest_comment
            else:  # Nếu không có bình luận
                product_info['Bình luận'] = "N/A"
            
            self.products.append(product_info)
            
            # **Gửi dữ liệu vào Kafka**
            self.producer.send('product-topic', product_info)
            
            yield product_info


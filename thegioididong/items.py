# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class ThegioididongItem(scrapy.Item):
    # Define the fields for your scraped item here
    name = scrapy.Field()  # Tên sản phẩm
    price = scrapy.Field()  # Giá hiện tại
    old_price = scrapy.Field()  # Giá cũ
    promotion = scrapy.Field()  # Thông tin khuyến mãi
    battery_life = scrapy.Field()  # Thời gian sử dụng pin
    target_user = scrapy.Field()  # Đối tượng sử dụng
    dial_diameter = scrapy.Field()  # Đường kính mặt
    dial_thickness = scrapy.Field()  # Độ dày mặt
    water_resistance = scrapy.Field()  # Khả năng kháng nước
    power_source = scrapy.Field()  # Nguồn năng lượng
    features = scrapy.Field()  # Tiện ích
    glass_material = scrapy.Field()  # Chất liệu mặt kính
    brand_origin = scrapy.Field()  # Thương hiệu của
    comments = scrapy.Field()  # Bình luận

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
from scrapy.exceptions import DropItem
import os

class MongoDBTgddPipeline:
    def __init__(self):
        # Connection String
        econnect = str(os.environ.get('Mongo_HOST', 'localhost'))
        #self.client = pymongo.MongoClient('mongodb://mymongodb:27017')
        self.client = pymongo.MongoClient('mongodb://'+econnect+':27017')
        self.db = self.client['TGDD_CRAWLER'] #Create Database      
        pass
    
    def process_item(self, item, spider):
        
        collection =self.db['DHDT_Collection'] #Create Collection or Table
        try:
            collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item: {e}")       
        pass
class ThegioididongPipeline:
    def process_item(self, item, spider):
        return item

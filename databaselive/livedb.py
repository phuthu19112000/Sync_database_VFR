from pymongo import MongoClient
from bson.objectid import ObjectId
from pydantic import BaseModel
from databaselive.utils import norm_dict
from typing import Optional
import os

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(self):
        yield self.validate
    
    @classmethod
    def validate(self,v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid objectid")
        return ObjectId(v)
    
    @classmethod
    def __modify_schema__(self,field_schema):
        field_schema.update(type="string")

class Singleton(type):
    __instance = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.__instance:
            cls.__instance[cls] = super(Singleton, cls).__call__(*args,**kwargs)
        return cls.__instance[cls]

class UserDB():

    __metaclass__ = Singleton

    def __init__(self):
        self.load_env()
        self.database = MongoClient(self.URL)[self.DB_NAME]
        self.collection = self.database["users"]
    
    @staticmethod
    def reload():
        UserDB()

    def load_env(self):
        self.URL = os.getenv("URL_MONGO")
        self.DB_NAME = os.getenv("NAME_DB")

    def add_user(self, data: dict):
        user = self.collection.find_one({"id": data["userId"]})
        if user:
            return None
        new_user = self.collection.insert_one(data)
        new_user = self.collection.find_one({"_id": new_user.inserted_id})
        return norm_dict(new_user)
    
    def get_user_info(self, uid:int) -> dict:
        
        user = self.collection.find_one({"id":uid})
        if user:
            return norm_dict(user)
        return None
    
    def update_user(self, uid:int, data:dict):
        user = self.collection.find_one({"id":uid})
        if user:
            is_update = self.collection.update_one(
                {"_id":user["_id"]}, {"$set":data}
            )
            if is_update:
                return True
            else:
                return False
        return False
    
    def del_user(self, uid:int):
        user = self.collection.find_one({"id":uid})
        if user:
            self.collection.delete_one({"_id":user["_id"]})
            return True
        return False


class ItemDBLIVE():

    __metaclass__ = Singleton

    def __init__(self):
        self.load_env()
        # self.database = MongoClient(url,tlsCAFile="/root/hieu/fastapi/lib/python3.7/site-packages/certifi/cacert.pem")[db_name]
        self.database = MongoClient(self.URL)[self.DB_NAME]
        self.collection = self.database["items"]

    @staticmethod
    def reload():
        ItemDBLIVE()

    def load_env(self):
        self.URL = os.getenv("URL_MONGO")
        self.DB_NAME = os.getenv("NAME_DB")

    def add_item(self,data:dict):
        item = self.collection.find_one({"entity_id":data["entity_id"]})
        if item:
            return None
        new_item = self.collection.insert_one(data)
        new_item = self.collection.find_one({"_id": new_item.inserted_id})
        return norm_dict(new_item)

    def get_item_info(self, iid: int) -> dict:
        item = self.collection.find_one({"entity_id": iid})
        if item:
            return norm_dict(item)
        return None
    
    def update_item(self, iid: int, data: dict):
        item = self.collection.find_one({"entity_id": iid})
        if item:
            is_update = self.collection.update_one(
                {"_id": item["_id"]}, {"$set": data}
            )
            if is_update:
                return True
            else:
                return False
        return False

    def update_field_item(self, iid: int, attribute_name, value):
        item = self.collection.find_one({"entity_id": iid})
        if item:
            is_update = self.collection.update_one(
                {"_id": item["_id"]},
                {"$addToSet": {attribute_name:value}}
            )
            if is_update:
                return True
            else:
                return False
        return False

    def del_item(self, iid: int):
        item = self.collection.find_one({"entity_id": iid})
        if item:
            self.collection.delete_one({"_id":item["_id"]})
            return True
        return False 
    
    def del_field_item(self, iid: int, attribute_name):
        item = self.collection.find_one({"entity_id": iid})
        if item:
            self.collection.update_one({"_id": item["_id"]}, {"$unset":{attribute_name:""}})
            return True
        return False


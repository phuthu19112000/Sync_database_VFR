#!/usr/bin/env python3
from mysql.connector.errors import custom_error_exception
import os
from mysql import connector
from kafka import KafkaConsumer, consumer
import json
from databaselive.livedb import ItemDBLIVE
from kafka import TopicPartition
from utils.consumers import KafkaInit
from utils.mysql_init_db import MysqlInit
from utils.key_feature import VFR_image, attribute_product
from logger.logger import callback_log, write_logs

def load_env():
    global mydb
    global instance_db_mysql
    global col

    col = ItemDBLIVE()
    name_server_mysql = os.getenv("NAME_SERVERLIVE")
    name_table_mysql = "catalog_product_entity_int"
    bootstrap_server = "kafka:9092"
    var_kafka = (name_server_mysql,name_table_mysql,bootstrap_server)

    host_mysql_db=os.getenv("HOST_MYSQL_LIVE")
    user_mysql=os.getenv("USER_MYSQL_LIVE")
    password_user=os.getenv("PASSWORD_MYSQL_LIVE")
    database_name="magento2"
    mydb = MysqlInit(host_mysql_db,user_mysql,password_user,database_name)
    instance_db_mysql = mydb.init_db()

    return var_kafka


def sync_to_mongo(payload: dict) -> dict:

    cursor1 = mydb.get_cursor(instance_db_mysql)
    
    if payload["op"] == "r":
        return None

    if payload["op"] == "u" or payload["op"] == "c":
        data = payload["after"]
        entity_id = data["entity_id"]

        cursor1.execute("select type_id from catalog_product_entity where entity_id={}".format(entity_id))
        fetch = cursor1.fetchall()
        for i in fetch:
            if i[0] == "simple":
                return None

        item = col.get_item_info(entity_id)
        if item:
            # update
            if data["attribute_id"] in list(attribute_product.keys()) and data["value"] != None:
                attribute_name = attribute_product[data["attribute_id"]]
                cursor1.execute("select value from eav_attribute_option_value \
                    where option_id={} and store_id={}".format(
                    data["value"], 0))
                result1 = cursor1.fetchall()
                for i in result1:
                    value_name = i[0]
                    result = col.update_item(entity_id,
                                             {attribute_name: value_name})
                if result:
                    msg = {"status": True, "op": "u"}
                    return msg
                else:
                    msg = {"status": False, "op": "u"}
                    return msg

            if data["attribute_id"] in list(VFR_image.keys()):
                attribute_name = VFR_image[data["attribute_id"]]
                if data["value"] != None:
                    cursor1.execute("select value from eav_attribute_option_value where option_id={} and store_id={}".format(data["value"],2))
                    result2 = cursor1.fetchall()
                    result = col.update_item(entity_id, {attribute_name: result2[0][0]})
                    if result:
                        msg = {"status": True, "op": "u"}
                        return msg
                    else:
                        msg = {"status": False, "op": "u"}
                        return msg

        elif item is None:
            msg = {"status": False, "op": "u"}
            return msg

    if payload["op"] == "d":
        data = payload["before"]
        entity_id = data["entity_id"]

        cursor1.execute("select type_id from catalog_product_entity where entity_id={}".format(entity_id))
        fetch = cursor1.fetchall()
        for i in fetch:
            if i[0] == "simple":
                return None

        if data["attribute_id"] in list(attribute_product.keys()):
            attribute_name = attribute_product[data["attribute_id"]]
            result = col.del_field_item(entity_id, attribute_name)
            if result:
                msg = {"status": True, "op": "d"}
                return msg
            else:
                msg = {"status": False, "op": "d"}
                return msg

def start_consumer(consumer, topic):
    count = 0
    for msg in consumer:
        if msg.value != None:
            msg = msg.value.decode("utf-8")
            msg = json.loads(msg)
            payload = msg["payload"]
            success = sync_to_mongo(payload=payload)

            if success is None:
                consumer.commit()
                continue

            if success["status"] is True:
                count = 0
                consumer.commit()
                offset = consumer.committed(partition=TopicPartition(
                    topic, 0))
                write_logs(offset,payload)

            elif success["status"] is False:
                offset = consumer.committed(partition=TopicPartition(
                    topic, 0))
                if count < 5:
                    consumer.seek(partition=TopicPartition(
                        topic, 0),
                                  offset=offset)
                    count += 1
                else:
                    callback_log(offset, payload)
                    consumer.commit()
                    
        elif msg.value == None:
            continue


if __name__ == "__main__":
    var_kafka = load_env()
    k_consumer, topic = KafkaInit(var_kafka[0], var_kafka[1], var_kafka[2])
    start_consumer(k_consumer, topic)
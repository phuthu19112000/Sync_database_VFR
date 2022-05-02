import json
import os
from kafka import TopicPartition
from databaselive.livedb import ItemDBLIVE
from utils.consumers import KafkaInit
from utils.mysql_init_db import MysqlInit
from utils.key_feature import attribute_product, VFR_image, list_category
from logger.logger import callback_log, write_logs

def load_env():
    global mydb
    global instance_db_mysql
    global col

    col = ItemDBLIVE()
    name_server_mysql = os.getenv("NAME_SERVERLIVE")
    name_table_mysql = "catalog_product_entity"
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

    cursor = mydb.get_cursor(instance_db_mysql)
    if payload["op"] == "r" or payload["op"] == "c":

        data = payload["after"]

        if data["type_id"] == "simple":
            return None

        entity_id = data["entity_id"]

        # attribute product
        cursor.execute(
            "select attribute_id,value from catalog_product_entity_int where entity_id={}"
            .format(entity_id))
        result3 = cursor.fetchall()
        if result3 != []:
            for i in result3:
                attribute_id = i[0]
                if attribute_id in list(attribute_product.keys()):
                    attribute_name = attribute_product[attribute_id]
                    if i[1] is not None:
                        cursor.execute(
                            "select value from eav_attribute_option_value where option_id={} \
                            and store_id={}".format(i[1], 0))
                        result4 = cursor.fetchall()
                        for j in result4:
                            value_name = j[0]
                            data.update({attribute_name: value_name})

        # fitting room type
        cursor.execute(
            "select attribute_id,value from catalog_product_entity_int where entity_id={}"
            .format(entity_id))
        result5 = cursor.fetchall()
        if result5 != []:
            for i in result5:
                attribute_id = i[0]
                if attribute_id in list(VFR_image.keys()):
                    if i[1] != None:
                        attribute_name = VFR_image[attribute_id]
                        cursor.execute("select value from eav_attribute_option_value where option_id={} and store_id={}".format(i[1],2))
                        result8 = cursor.fetchall()
                        data.update({attribute_name: result8[0][0]})

        # image path VFR manequine, VFR base
        cursor.execute(
            "select attribute_id,value from catalog_product_entity_varchar \
            where entity_id={}".format(entity_id))
        result1 = cursor.fetchall()
        if result1 != []:
            for i in result1:
                attribute_id = i[0]
                if attribute_id in list(VFR_image.keys()):
                    attribute_name = VFR_image[attribute_id]
                    data.update({attribute_name: i[1]})

        # Category products
        cursor.execute(
            "select category_id from catalog_category_product where product_id={}"
            .format(entity_id))
        result6 = cursor.fetchall()
        if result6 != []:
            for i in result6:
                category_id = i[0]
                if category_id in list_category:
                    cursor.execute(
                        "select value from catalog_category_entity_varchar where entity_id={} and store_id={} and attribute_id={}"
                        .format(category_id, 0, 120))
                    result7 = cursor.fetchall()
                    if result7 != []:
                        for i in result7:
                            data.update({"category": i[0]})
                else:
                    continue

        result = col.add_item(data)
        if result:
            msg = {"status": True, "op": "c"}
            return msg
        elif result is None:
            msg = {"status": False, "op": "c"}
            return msg

    if payload["op"] == "u":
        data = payload["after"]

        if data["type_id"] == "simple":
            return None

        ID = data["entity_id"]
        result = col.update_item(ID, data)
        if result:
            msg = {"status": True, "op": "u"}
            return msg
        else:
            msg = {"status": False, "op": "u"}
            return msg

    if payload["op"] == "d":
        data = payload["before"]
        
        if data["type_id"] == "simple":
            return None

        ID = data["entity_id"]
        result = col.del_item(ID)
        if result:
            msg = {"status": True, "op": "d"}
            return msg
        else:
            msg = {"status": False, "op": "d"}
            return msg

def start_consumer(consumer, topic):

    count = 0
    for msg in consumer:
        if msg.value is not None:
            msg = msg.value.decode("utf=8")
            msg = json.loads(msg)
            payload = msg["payload"]
            success = sync_to_mongo(payload)

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

        elif msg.value is None:
            print("value is None")
            continue


if __name__ == "__main__":

    var_kafka = load_env()
    k_consumer, topic = KafkaInit(var_kafka[0], var_kafka[1], var_kafka[2])
    start_consumer(k_consumer, topic)

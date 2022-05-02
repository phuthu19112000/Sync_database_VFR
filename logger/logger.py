
import datetime

def callback_log(offset, payload):
    with open("../logs/log_failed_message.log",
              "a") as f:
        parser = datetime.datetime.now()
        parser = parser.strftime("%d-%m-%Y %H:%M:%S")
        f.write(
            parser +
            "\t | \tcatalog_product_entity\t | \tMESSAGE FAILED AT OFFSET {}\n{}\n\n"
            .format(offset, payload))

def write_logs(offset, payload):
    with open("../logs/catalog_product_entity_main.log","a") as f:
        parser = datetime.datetime.now()
        parser = parser.strftime("%d-%m-%Y %H:%M:%S")
        f.write(
            parser +
            "\t | \tcatalog_product_entity\t | \tMESSAGE SUCCESS AT OFFSET {}\n{}\n\n"
            .format(offset, payload))

from kafka import KafkaConsumer, consumer

class KafkaInit(object):
    def __init__(self, name_server, name_table, bootstrap_server) -> None:
        self.topic = "{}.magento2.{}".format(name_server, name_table)
        self.bootstrap_server = bootstrap_server
        self.InitConsumer()

    def InitConsumer(self):
        consumer = KafkaConsumer(self.topic,
                         bootstrap_servers=self.bootstrap_server,
                         auto_offset_reset="earliest",
                         enable_auto_commit=False,
                         auto_commit_interval_ms=5000,
                         reconnect_backoff_ms=50,
                         heartbeat_interval_ms=3000,
                         max_poll_interval_ms=500000,
                         group_id="hieu",
                         security_protocol="PLAINTEXT")
        
        return consumer, self.topic

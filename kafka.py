# -*- coding:utf-8 -*-

from pykafka import KafkaClient
import codecs
import logging
logging.basicConfig(level=logging.INFO)


class kafka(object):
    def __init__(self, ip, port, topic):
        """
        :param ip: kafka的IP
        :param port: kafka的port
        :param topic: kafka的topic
        """
        self.client = KafkaClient(hosts="%s:%s" % (ip, port))
        self.topic = self.client.topics[topic]

    def produce_kafka_data(self, message=""):
        """
        :param message: 指定produce的数据
        :return:
        """
        with self.topic.get_sync_producer() as producer:
            producer.produce(message)

    def produce_kafka_file(self, filename):
        """
        :param filename: 指定文件名
        :return:
        """
        with self.topic.get_producer() as producer:
            with codecs.open(filename, "r") as rf:
                for line in rf.readlines():
                    line = line.strip()
                    if not line:
                        continue
                    producer.produce(str(line))

    def consume_simple_kafka(self, timeout):
        consumer = self.topic.get_simple_consumer(consumer_timeout_ms = timeout)
        for message in consumer:
            if message is not None:
                print message.offset, message.value

    # def consume_kafka(self, zkhost):
    #     balanced_consumer = self.topic.get_balanced_consumer(
    #         consumer_group="testgroup",
    #         auto_commit_enable=False,
    #         zookeeper_connect=zkhost,
    #         #zookeeper=zkhost,
    #         zookeeper_connection_timeout_ms=6000,
    #          consumer_timeout_ms=10000,
    #     )
    #     for message in balanced_consumer:
    #         if message is not None:
    #             print message.offset, message.value
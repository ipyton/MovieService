# -*- coding: utf-8 -*-
import json
import logging
import time
import traceback


import threading

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from utils import movieEncodingUtil, FileManipulator

from confluent_kafka import Consumer, KafkaException, Producer
import json


cluster = Cluster(['127.0.0.1'],port=9042,  # Ensure correct port
    auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))

KAFKA_BROKER = "localhost:9092"
KAFKA_GROUP_ID = "flask-consumer-group"

instance = cluster.connect()

set_upload_status = instance.prepare("update files.file_upload_status set status_code = ? where resource_id = ? and resource_type = ?")
check_upload_status = instance.prepare("select status_code from files.file_upload_status where resource_id = ? and resource_type = ?;")
add_playlist = instance.prepare("insert into movie.playable (resource_id, type, quality, bucket, path) values(?, ?, ?, ?, ?)")

producer = Producer({
    'bootstrap.servers': KAFKA_BROKER
})
def delivery_report(err, msg):
    """回调函数，用于处理消息发送结果"""
    if err is not None:
        print(f"failed sending message: {err}",flush=True)
    else:
        print(f"successfully sent message: {msg.topic()} [{msg.partition()}] @ {msg.offset()}",flush = True)



def send_message(key, value):
    producer.produce("fileUploadStage2", key = key, value = value, callback = delivery_report)
    producer.flush()  # 确保消息发送


def upload_to_minio():
    '''
    stage2 upload to minio
    :return:
    '''

    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # 关闭自动提交
    }

    consumer = Consumer(conf)
    consumer.subscribe(["fileUploadStage2"])

    while True:
        try:
            msg = consumer.poll(timeout=5.0)  # Poll messages with timeout

            if msg is None:
                continue
            if msg.error():
                print(msg.error(),flush=True)
                traceback.print_exc()
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print("EOF", flush=True)
                    continue
                else:
                    traceback.print_exc()
                    logging.error(f"Consumer error: {msg.error()}")
                    continue
            print(f"Message received {msg.value()}", flush=True)
            data = json.loads(msg.value())
            print(data, flush=True)
            result = instance.execute(check_upload_status, (data["resourceId"], data["type"]))
            result = list(result)
            if len(result) == 0:
                print(f"No Movie In DB: {msg.key()}")
                consumer.commit(message=msg)
                continue
            if len(result) != 0 and result[0][0] > 4:
                print(f"No Movie In DB: {msg.key()}")
                consumer.commit(message=msg)
                continue


            upload_result = FileManipulator.upload_files(data["inputPath"], data["bucket"], data["outputPath"])
            if upload_result:
                instance.execute(set_upload_status, (5, result["resourceId"], result["type"]))
            else:
                logging.error("Uploading error!!!")

            FileManipulator.delete_files(data["inputPath"])
            quality = data.get("quality")
            if data.get("quality") is None:
                quality = 3
            instance.execute(add_playlist, (data["resourceId"], data["type"], quality, data["bucket"], data["outputPath"]))

            # 处理完后手动提交偏移量
            consumer.commit(message=msg)


        except Exception as e:
            traceback.print_stack()
            logging.error(f"Error: {e}")

    consumer.close()



def kafka_consumer():
    '''
        stage 1: encoding.
    '''
    logging.info("kafka consumer 1 start")

    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # 关闭自动提交
    }

    consumer = Consumer(conf)
    consumer.subscribe(["fileUploadStage1"])


    while True:
        try:
            msg = consumer.poll(timeout=5.0)  # Poll messages with timeout
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue

            logging.info(f"Message received {msg.value()}")

            data = json.loads(msg.value())
            if data is None:
                print(f"Consumer error: {msg.key()}")
                continue
            print(data, flush=True)
            # Check the status.
            result = instance.execute(check_upload_status, (data["resourceId"], data["type"]))
            result = list(result)
            if len(result) == 0:
                print(f"No Movie In DB: {msg.key()}")
                consumer.commit(message=msg)
            if  result[0][0] > 3:
                consumer.commit(message=msg)
                continue


            # Encoding process
            encoding_result = movieEncodingUtil.encodeHls(data["inputPath"], data["outputPath"], data["inputSource"],
                                                 data["outputSource"])
            if encoding_result:
                send_message(data["resourceId"] + "_" + data["type"],json.dumps({
                    "resourceId": data["resourceId"],
                    "inputPath": data["outputPath"],
                    "type": data["type"],
                    "bucket":"longvideos",
                    "outputPath": "/" + data["type"] + "_" +data["resourceId"],
                }))
            else:
                print("Encoding error!!!", flush=True)

            # 处理完后手动提交偏移量
            instance.execute(set_upload_status, (4, data["resourceId"], data["type"]))
            FileManipulator.delete_files(data["inputPath"])
        except Exception as e:
            traceback.print_stack()
            print(f"Error: {e}", flush=True)

if __name__ =="__main__":
    logging.info("Starting Kafka consumers")

    t1 = threading.Thread(target=kafka_consumer, daemon=True, name="KafkaConsumer1")
    t2 = threading.Thread(target=upload_to_minio, daemon=True, name="MinioUploader")

    t1.start()
    t2.start()
    t1.join()
    t2.join()
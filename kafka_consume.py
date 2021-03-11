from kafka import KafkaConsumer
from elasticsearch_class import MyElasticSearch
import json
consumer=KafkaConsumer("message",bootstrap_servers=["localhost:9092"])
es=MyElasticSearch("bookstores","test-type")
for mess in consumer:
    mess_str=mess.value.decode()
    mess_str=json.loads(mess_str)
    print(type(mess_str))
    flag=mess_str["action"]
    if flag=="insert":
        print(mess_str["data"])
        es.insert_one(mess_str["data"])
    elif flag=="delete":
        es.delete_one(mess_str["data"])
    elif flag=="update":
        es.update_one(mess_str["data"])
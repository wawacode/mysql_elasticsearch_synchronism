from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
consumer=KafkaConsumer("message",bootstrap_servers=["localhost:9092"])
es=Elasticsearch()
for mess in consumer:
    #传来的数据需要进行json转换
    result=json.loads(mess.value.decode("utf8"))
    event=result["event"]
    if event=="insert":
        result_values=result["values"]
        es.index(index="westjourney",doc_type="test-type",id=result_values["id"],body=result_values)
        print("添加成功")
    elif event=="update":
        #注意更新操作, body内容要加入一个doc键,指示的内容就是要修改的内容
        result_values=result["after_values"]
        es.update(index="westjourney",doc_type="test-type",id=result_values["id"],body={"doc":result_values})
        print("更新数据成功")
    elif event=="delete":
        result_id=result["values"]["id"]
        es.delete(index="westjourney",doc_type="test-type",id=result_id)
        print("删除数据成功")


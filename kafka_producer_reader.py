from kafka import KafkaProducer
#实例化生产者
import json
producer=KafkaProducer(bootstrap_servers=["localhost:9092"])
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)
MYSQL_SETTINGS={
    "host":"localhost",
    "user":"root",
    "password":"admin"
}
stream=BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                          server_id=4,
                          blocking=True,
                          only_schemas="readerbinlog",
                          only_events=[WriteRowsEvent,UpdateRowsEvent,DeleteRowsEvent])
for binlogstream in stream:
   for row in binlogstream.rows:
        if isinstance(binlogstream,WriteRowsEvent):
            row["event"]="insert"
        elif isinstance(binlogstream,UpdateRowsEvent):
            row["event"]="update"
        elif isinstance(binlogstream,DeleteRowsEvent):
            row["event"]="delete"
        row_json=json.dumps(row,ensure_ascii=False)
        producer.send("message",row_json.encode())
producer.close()

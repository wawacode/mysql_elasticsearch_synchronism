from kafka import KafkaProducer
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
MYSQL_SETTINGS={
    "host":"localhost",
    "user":"root",
    "password":"admin"
}
import json
import sys
producer=KafkaProducer(bootstrap_servers=["localhost:9092"])
stream=BinLogStreamReader(connection_settings=MYSQL_SETTINGS,server_id=3,blocking=True,only_schemas=["booksme"],only_events=[DeleteRowsEvent,WriteRowsEvent,UpdateRowsEvent])
for binlogevent in stream:
    for row in binlogevent.rows:
        event={"schema":binlogevent.schema,"table":binlogevent.table}
        if isinstance(binlogevent,DeleteRowsEvent):
            event["action"]="delete"
            event["data"]=row["values"]
        elif isinstance(binlogevent,WriteRowsEvent):
            event["action"]="insert"
            event["data"]=row["values"]
        elif isinstance(binlogevent,UpdateRowsEvent):
            event["action"]="update"
            event["data"]=row["values"]
        json_response=json.dumps(event,ensure_ascii=False).encode()
        print(json_response)
        producer.send("message",json_response)
        sys.stdout.flush()

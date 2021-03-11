from elasticsearch import Elasticsearch
class MyElasticSearch():
    def __init__(self,index_name,index_type):
        self.es=Elasticsearch();
        self.index_name=index_name
        self.index_type=index_type
    def insert_one(self,doc):
        self.es.index(index=self.index_name,doc_type=self.index_type,body=doc)
    def insert_array(self,docs):
        for doc in docs:
            self.es.index(index=self.index_name,doc_type=self.index_type,body=doc)
    def update_one(self,doc,uid):
        self.es.update(index=self.index_name,doc_type=self.index_type,id=uid,body=doc)
    def delete_one(self,uid):
        self.es.delete(index=self.index_name,doc_type=self.index_type,id=uid)
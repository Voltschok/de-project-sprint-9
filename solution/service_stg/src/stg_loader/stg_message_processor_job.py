import time
from datetime import datetime
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer 
import json
from typing import Dict, Optional
 
from lib.redis.redis_client import  RedisClient
from stg_loader.repository.stg_repository import StgRepository


import json

class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis:RedisClient,
                 stg_repository:StgRepository,
                 batch_size: int, 
                 logger: Logger
                 ) -> None:
        
        self._consumer=consumer
        self._producer=producer
        self._redis=redis
        self._stg_repository=stg_repository
        self._batch_size=batch_size
        self._logger = logger
     
    
    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")
        
        i=0
        while i< self._batch_size:
            self._logger.info(f"{datetime.utcnow()}: {self._consumer}")
            self._logger.info(f"{datetime.utcnow()}: {type(self._stg_repository)}")
            message=self._consumer.consume()
            self._logger.info(f"{datetime.utcnow()}: {message}")

            if message is None:
                break
            else:
                self._stg_repository.order_events_insert(message['object_id'], 
                                                     message['object_type'],
                                                     message['sent_dttm'],
                                                     json.dumps(message['payload']))
                self._logger.info(f"{datetime.utcnow()}: POSTGRES FINISHED")
        
          
                products_list=[]
                for product in message['payload']['order_items']:
                        for item in self._redis.get(message['payload']['restaurant']['id'])['menu']:
                            if item['_id']==product["id"]:
                                product['name']=item['name']
                                product['category']=item['category']
                                
                        products_list.append(product)
            
                result={
                    "object_id": message['object_id'],
                    "object_type":message['object_type'],
                    "payload": {
                        "id": message['object_id'],
                        "date": message['payload']['date'],
                        "cost": message['payload']['cost'],
                        "payment": message['payload']['payment'],
                        "status": message['payload']['final_status'],
                        "restaurant": {
                                        "id": message['payload']['restaurant']['id'],
                                        "name": self._redis.get(message['payload']['restaurant']['id'])['name']
                                    },
                        "user": {
                            "id": message['payload']['user']['id'],
                            "name": self._redis.get(message['payload']['user']['id'])['name']
                        },
                        "products": products_list
                        }
                    }
                self._producer.produce(result)
            i+=1

     

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

 

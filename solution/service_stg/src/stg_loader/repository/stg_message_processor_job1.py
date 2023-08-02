import uuid
import time
import json
from datetime import datetime
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer 
from stg_loader.repository.stg_repository import DdsRepository

class DdsMessageProcessor:


    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository:DdsRepository,
                 batch_size: int, 
                 logger: Logger
                 ) -> None:
        self._consumer=consumer
        self._producer=producer
        self._dds_repository=dds_repository
        self._batch_size=batch_size
        self._logger = logger

    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")
        
        hubs=['user', 'product', 'category', 'restaurant', 'order']
        links=['order_product', 'product_category', 'order_user', 'product_restaurant']
        sattelites=[ 'order_cost', 'order_status', 'product_names', 'restaurant_names', 'user_names' ]
        load_dt=datetime.now()
        load_src='stg-order-service'

        i=0
        while i< self._batch_size:
            self._logger.info(f"{datetime.utcnow()}: {self._consumer}")
            self._logger.info(f"{datetime.utcnow()}: {type(self._dds_repository)}")
            message=self._consumer.consume()
            
            self._logger.info(f"{datetime.utcnow()}: {message}")
		
            if message is None:
                break
            else:
                category_list=[product['category'] for product in message['payload']['products']]
                products_id_list=[product['id'] for product in message['payload']['products']]
                self._logger.info(f"{datetime.utcnow()}: START OF THE DOWNLOAD CYCLE")
                h_object_pk_dict={}
         
                for hub in hubs:
                    
                    if hub=='order':
                        h_object_pk=uuid.uuid3(uuid.NAMESPACE_DNS, str(message['object_id']))
                      #  h_object_pk_dict[f'h_{hub}_pk']=h_object_pk
 
                        self._dds_repository.hubs_insert(f'{hub}', h_object_pk, message['object_id'], message['payload']['date'], load_dt, load_src)

                        self._logger.info(f"{datetime.utcnow()}: POSTGRES_HUB_ORDER FINISHED")
  
                    elif hub=='user' or hub=='restaurant':
                        h_object_pk=uuid.uuid3(uuid.NAMESPACE_DNS, message['payload'][hub]['id'])
                    #    h_object_pk_dict[f'h_{hub}_pk']=h_object_pk
 
                        self._dds_repository.hubs_insert(f'{hub}', h_object_pk, message['payload'][hub]['id'], load_dt, load_src)

                        self._logger.info(f"{datetime.utcnow()}: POSTGRES_HUB_OTHERS FINISHED")
                        
                    elif hub=='product':
                        for product_id in products_id_list:
                            h_object_pk=uuid.uuid3(uuid.NAMESPACE_DNS, product_id)
                           # h_object_pk_dict[f'{product_id}']=h_object_pk
    
                            self._dds_repository.hubs_insert(f'{hub}', h_object_pk, product_id, load_dt, load_src)
                            self._logger.info(f"{datetime.utcnow()}: POSTGRES_HUB_PRODUCT FINISHED")
 
                    elif hub=='category':
                        for category in category_list:
                            h_object_pk=uuid.uuid3(uuid.NAMESPACE_DNS, category)
                        #   h_object_pk_dict[f'{category}']=h_object_pk
                            self._dds_repository.hubs_insert(f'{hub}', h_object_pk, category, load_dt, load_src)
 
                            self._logger.info(f"{datetime.utcnow()}: POSTGRES_HUB_CATEGORY FINISHED")
                
                self._logger.info(f"H_OBJECT_PK_DICT:{h_object_pk_dict}")

                for link in links:
                    link_names=link.split("_")

                    h_hub1name_pk1=h_object_pk_dict[f'h_{link_names[0]}_pk']
                    self._logger.info(f"h_hub1name_pk1: {h_hub1name_pk1}")

                    h_hub1name_pk2=h_object_pk_dict[f'h_{link_names[1]}_pk']
                    self._logger.info(f"h_hub1name_pk2: {h_hub1name_pk2}")

                    hk_hub1name_hub2name_pk=uuid.uuid3(uuid.NAMESPACE_DNS, str(h_hub1name_pk1)+str(h_hub1name_pk2))
                    self._logger.info(f"hk_hub1name_hub2name_pk: {hk_hub1name_hub2name_pk}")

                    self._dds_repository.links_insert(f'{link}', hk_hub1name_hub2name_pk, h_hub1name_pk1, h_hub1name_pk2, load_dt, load_src)
                    self._logger.info(f"{datetime.utcnow()}: POSTGRES_LINKS  FINISHED")


                for sattelite in sattelites:
                    sattelite_names=sattelite.split("_")
                    h_hubname_pk=h_object_pk_dict[f'h_{sattelite_names[0]}_pk']
                    self._logger.info(f"h_hub1name_pk1: {h_hubname_pk}")

                    order_cost: "cost"  message['payload']['cost'], "payment" message['payload']['payment']
                    product_names:"name"
                    user_names:"username", "userlogin"
                    order_status:"status"
                    restaurant_names:"name"

   
                    hk_tablename_hashdiff=uuid.uuid3(uuid.NAMESPACE_DNS, str(h_hubname_pk)+(load_dt).strftime('%Y-%m-%d %H:%M:%S')+load_src  )
                    self._logger.info(f"hk_hub1name_hub2name_pk: {hk_hub1name_hub2name_pk}")

                    self._dds_repository.sattelite_insert(f'{sattelite}', h_hubname_pk, h_hub1name_pk1, load_dt, load_src, hk_tablename_hashdiff)
                    self._logger.info(f"{datetime.utcnow()}: POSTGRES_LINKS  FINISHED")    

            self._logger.info(f"{datetime.utcnow()}: POSTGRES  FINISHED")
          
                # products_list=[]
                # for product in message['payload']['order_items']:
                #         for item in self._redis.get(message['payload']['restaurant']['id'])['menu']:
                #             if item['_id']==product["id"]:
                #                 product['name']=item['name']
                #                 product['category']=item['category']
                                
                #         products_list.append(product)
            
                # result={
                #     "object_id": message['object_id'],
                #     "object_type":message['object_type'],
                #     "payload": {
                #         "id": message['object_id'],
                #         "date": message['payload']['date'],
                #         "cost": message['payload']['cost'],
                #         "payment": message['payload']['payment'],
                #         "status": message['payload']['final_status'],
                #         "restaurant": {
                #                         "id": message['payload']['restaurant']['id'],
                #                         "name": self._redis.get(message['payload']['restaurant']['id'])['name']
                #                     },
                #         "user": {
                #             "id": message['payload']['user']['id'],
                #             "name": self._redis.get(message['payload']['user']['id'])['name']
                #         },
                #         "products": products_list
                #         }
                #     }
                # self._producer.produce(result)
            i+=1
                # Имитация работы. Здесь будет реализована обработка сообщений.
        time.sleep(2)

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

 

from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int, 
                 logger: Logger
                 ) -> None:

        self._consumer=consumer
        self._cdm_repository=cdm_repository
        self._batch_size=batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        i=0
        while i< self._batch_size:
       
            message=self._consumer.consume()
            self._logger.info(f"{datetime.utcnow()}: {message}")
		
            if message is None:
                break
            else:
                #user product count
                user_id=message['user_id']
                for product_id, product_name in zip(message['product_id_list'], message['product_name_list']):
                    self._cdm_repository.cdm_insert('user_product', user_id, product_id, product_name)
 
                #user category count
                for category_id, category_name in zip(message['category_id_list'], message['category_name_list']):
                    self._cdm_repository.cdm_insert('user_category', user_id, category_id, category_name)
 		
            self._logger.info(f"{datetime.utcnow()}: CDM SENT")
            self._logger.info(f"{datetime.utcnow()}: FINISH")
            i+=1
	

import uuid
from datetime import datetime
from typing import Any, Dict, List
import time
from logging import Logger
 
from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect, logger: Logger) -> None:
        self._db = db
        self._logger = logger

    def cdm_insert(self,
                            cdm: str,
                            *args: list
 
                            ) -> None:
        
       
        values_args=list(args)
        #values_args.append(datetime.now())
        #values_args.append('stg-order-service')
        pk=(cdm.split("_"))[1]

        with self._db.connection() as conn:
            with conn.cursor() as cur:

                cur.execute(f"Select * FROM cdm.{cdm}_counters LIMIT 0")
                colnames = [desc[0] for desc in cur.description]
 
        values_set=", ".join(['%('+colname+')s' for colname in colnames[1:-1]])
        
        self._logger.info(f"values_set: {values_set}")

        params={col: val for col, val in zip(colnames[1:-1], values_args)}

        self._logger.info(f"params: {params}")

        query=  f"""
                    INSERT INTO cdm.{cdm}_counters(user_id, {pk}_id,{pk}_name, order_cnt)
                    VALUES ({values_set}, 1)
                    ON CONFLICT (user_id, {pk}_id) DO UPDATE
                    
                    SET 
                        {pk}_name=EXCLUDED.{pk}_name, 
                        order_cnt={cdm}_counters.order_cnt+1
                    RETURNING order_cnt
                        
                """
        self._logger.info(f"query: {query}")

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
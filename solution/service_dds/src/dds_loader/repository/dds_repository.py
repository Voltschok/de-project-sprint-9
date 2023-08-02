import uuid
from datetime import datetime
from typing import Any, Dict, List
import time
from logging import Logger
 
from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect, logger: Logger) -> None:
        self._db = db
        self._logger = logger

    def hubs_insert(self,
                            hub: str,
                            *args: list
 
                            ) -> None:
        
       
        values_args=list(args)
        #values_args.append(datetime.now())
        #values_args.append('stg-order-service')
 

        with self._db.connection() as conn:
            with conn.cursor() as cur:

                cur.execute(f"Select * FROM dds.h_{hub} LIMIT 0")
                colnames = [desc[0] for desc in cur.description]
 
        values_set=", ".join(['%('+colname+')s' for colname in colnames])
        colnames_set=", ".join([colname+'=EXCLUDED.'+colname for colname in colnames[1:]])
        params={col: val for col, val in zip(colnames, values_args)}
 
        query=  f"""
                    INSERT INTO dds.h_{hub}({", ".join(colnames)}) 
                    VALUES ({values_set})
                    ON CONFLICT (h_{hub}_pk) DO UPDATE
                    SET
                        {colnames_set }
                """
        self._logger.info(f"query: {query}")

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)


    def links_insert(self,
                            link: str,
                            *args: list
 
                            
                            ) -> None:

        values_args=list(args)
       #values_args.append(datetime.now())
        #values_args.append('stg-order-service')

        with self._db.connection() as conn:
            with conn.cursor() as cur:

                cur.execute(f"Select * FROM dds.l_{link} LIMIT 0")
                colnames = [desc[0] for desc in cur.description]
 
        values_set=", ".join(['%('+colname+')s' for colname in colnames])
        colnames_set=", ".join([colname+'=EXCLUDED.'+colname for colname in colnames[1:]])
        params={col: val for col, val in zip(colnames, values_args)}
 
        query=  f"""
                    INSERT INTO dds.l_{link}({", ".join(colnames)}) 
                    VALUES ({values_set})
                    ON CONFLICT (hk_{link}_pk) DO UPDATE
                    SET
                        {colnames_set }
                """
        
        self._logger.info(f"query: {query}")

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

    def sattelite_insert(self,
                            sattelite: str,
                            *args: list
                            
                            ) -> None:
        values_args=list(args)
        pk=(sattelite.split("_"))[0]
       #values_args.append(datetime.now())
        #values_args.append('stg-order-service')
        self._logger.info(f"values_args: {values_args}")
        with self._db.connection() as conn:
            with conn.cursor() as cur:

                cur.execute(f"Select * FROM dds.s_{sattelite} LIMIT 0")
                colnames = [desc[0] for desc in cur.description]
 
        values_set=", ".join(['%('+colname+')s' for colname in colnames])
        colnames_on_conflict=colnames.copy()
        colnames_on_conflict.remove('load_dt')
        colnames_set=", ".join([colname+'=EXCLUDED.'+colname for colname in colnames_on_conflict[1:]])
      
        params={col: val for col, val in zip(colnames, values_args)}
 
        query=  f"""
                    INSERT INTO dds.s_{sattelite}({", ".join(colnames)}) 
                    VALUES ({values_set})
                    ON CONFLICT (h_{pk}_pk, load_dt) DO UPDATE
                    SET
                        {colnames_set }
                """
        
        self._logger.info(f"query: {query}")

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)        
                       
                
                

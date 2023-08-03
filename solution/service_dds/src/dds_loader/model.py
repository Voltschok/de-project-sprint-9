from pydantic import BaseModel

class Hub(BaseModel):
  h_object_pk: uuid,
  object_id: str,
  load_dt: datetime,
  load_src: str

class OrderHub(Hub):
  
  

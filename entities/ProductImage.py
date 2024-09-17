
from . import Entity

from typing import List, Dict

class ProductImage(Entity):

    class __meta__(Entity.__meta__):
        ENTITY_NAME = "ProductImage"
        DB_TABLE_NAME = "DimProductImage"

    _instances: Dict[int, "ProductImage"] = {}

    def __init__(self,
                 product_id: int,
                 image_name: str,
                 source_url: str,
                 order_id: int) -> None:
        
        self.product_image_id = self.increment()
        self.product_id = product_id
        self.image_name = image_name
        self.source_url = source_url
        self.order_id = order_id

        ProductImage._instances[self.product_image_id] = self

    @classmethod
    def instances(cls) -> List["ProductImage"]:
        return list(cls._instances.values())

    def insert_sql(self):
        query_temp = """
        INSERT INTO [dbo].[DimProductImage] (ProductID, ImageName, SourceUrl, OrderId, CreateDate, UpdateDate, IsDeleted, IsShow, DeleteDate)
        VALUES ({ProductID}, '{ImageName}', '{SourceUrl}', {OrderId}, {CreateDate}, {UpdateDate}, {IsDeleted}, {IsShow}, {DeleteDate})
        """

        return query_temp.format(
            **{
                "ProductID": self.product_id,
                "ImageName": self.image_name,
                "SourceUrl": self.source_url,
                "OrderId": self.order_id,
                "CreateDate": self.get_curr_date(),
                "UpdateDate": "NULL",
                "IsDeleted": 0,
                "IsShow": 1,
                "DeleteDate": "NULL"
            }
        )
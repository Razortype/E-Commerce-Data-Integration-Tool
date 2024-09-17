
from . import Entity

from typing import List, Dict

class CategoryModel(Entity):
    
    class __meta__(Entity.__meta__):
        ENTITY_NAME = "CategoryModel"
        DB_TABLE_NAME = "DimCategoryModel"

    _instances: Dict[int, "CategoryModel"] = {}

    def __init__(self,
                 category_id: int,
                 model: str) -> None:
        
        self.category_model_id = self.increment()
        self.category_id = category_id
        self.model = model

        CategoryModel._instances[self.category_model_id] = self

    @classmethod
    def instances(cls) -> List["CategoryModel"]:
        return list(cls._instances.values())

    def insert_sql(self):
        query_temp = """
        INSERT INTO [dbo].[DimCategoryModel] (CategoryID, ModelCode)
        VALUES({CategoryID},{ModelCode})
        """

        return query_temp.format(
            **{
                "CategoryID": self.category_id,
                "ModelCode": self.model
            }
        )
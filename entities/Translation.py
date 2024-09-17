
from . import Entity

from typing import List, Dict

class Translation(Entity):

    class __meta__(Entity.__meta__):
        ENTITY_NAME = "Translation"
        DB_TABLE_NAME = "DimTranslations"

    _instances: Dict[int, "Translation"] = {}

    def __init__(self,
                 instance_id: int,
                 instance_type: str,
                 language: str,
                 param_name: str,
                 value: str) -> None:
        
        self.translation_id = self.increment()
        self.instance_id = instance_id
        self.instance_type = instance_type
        self.language = language
        self.param_name = param_name
        self.value = value if value != "'" else ""

        Translation._instances[self.translation_id] = self

    @classmethod
    def instances(cls) -> List["Translation"]:
        return list(cls._instances.values())

    def insert_sql(self):
        temp_query = """
        INSERT INTO [dbo].[DimTranslations] 
        (InstanceID, InstanceType, ParamName, Value, Language)
        VALUES ({InstanceID}, '{InstanceType}', '{ParamName}', '{Value}', '{Language}');
        """

        return temp_query.format(
            **{
                "InstanceID": self.instance_id,
                "InstanceType": self.instance_type,
                "ParamName": self.param_name,
                "Value": self.value,
                "Language": self.language
            }
        )

        
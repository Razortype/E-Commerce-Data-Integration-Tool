
from . import Entity

from typing import List, Dict, Any

class Category(Entity):

    class __meta__(Entity.__meta__):
        ENTITY_NAME = "Category"
        DB_TABLE_NAME = "DimCategory"

    _instances: Dict[int, "Category"] = {}

    def __init__(self, 
                 en_name: str,
                 tr_name: str,
                 parent: "Category" = None) -> None:
        super().__init__()

        self.category_id = Category.increment()
        self.en_name = en_name
        self.tr_name = tr_name
        self._parent = parent
        self.models: List[str] = []

        self.translation_attrs = {
            "category_name": {
                "EN": "en_name",
                "TR": "tr_name"
            }
        }

        Category._instances[self.category_id] = self

    @classmethod
    def instances(cls) -> List["Category"]:
        return list(cls._instances.values())

    @property
    def parent(self) -> "Category":
        return self._parent

    @parent.setter
    def parent(self, parent: "Category") -> None:
        self._parent = parent

    @classmethod
    def get_elem_by_en_name(cls, en_name: str) -> "Category":
        for ins in cls._instances.values():
            if ins.en_name == en_name:
                return ins
            
    @classmethod
    def get_elem_by_tr_name(cls, tr_name: str) -> "Category":
        for ins in cls._instances.values():
            if ins.tr_name == tr_name:
                return ins
            
    def insert_sql(self):
        parent_id = str(self.parent.category_id) if self._parent else "NULL"
        date_str = self.get_curr_date()
        return f"""
        INSERT INTO [dbo].[DimCategory] 
        (CategoryErpCode, ParentID,CategoryImageUrl, CategorySortOrder,IsActive, IsDeleted, DeleteDate) 
        VALUES (NULL, {parent_id}, NULL, {self.category_id}, 1, 0, {date_str})
        """
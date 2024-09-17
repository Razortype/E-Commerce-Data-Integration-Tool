
from . import Entity
from . import Category

from typing import List, Dict, Tuple, TypeVar
from utils.custom_types import SKU, URL

class Product(Entity):

    class __meta__(Entity.__meta__):
        ENTITY_NAME = "Product"
        DB_TABLE_NAME = "DimProducts"

    _instances: Dict[SKU, "Product"] = {}

    def __init__(self,
                 sku: SKU,
                 ean_number: str,
                 brand_name: str,
                 category_group: Category,
                 product_category: Category,
                 weight_kg: float,
                 weight_net_kg: float,
                 product_title_tr: str,
                 product_title_en: str,
                 product_description_tr: str,
                 product_description_en: str,
                 country_of_origin: str,
                 main_image: str = "",
                 images: List[Tuple[str, URL]] = None,
                 product_category_: Category = None,
                 category_group_: Category = None) -> None:
        super().__init__()
        self.product_id = Product.increment()
        self.sku = sku
        self.ean_number = ean_number
        self.brand_name = brand_name
        self.category_group = category_group
        self.product_category = product_category
        self.weight_kg = weight_kg
        self.weight_net_kg = weight_net_kg
        self.product_title_en = product_title_en
        self.product_title_tr = product_title_tr
        self.product_description_en = product_description_en
        self.product_description_tr = product_description_tr
        self.country_of_origin = country_of_origin
        self.main_image = main_image
        self.images = images if images else list()
        self.product_category_ = product_category_
        self.category_group_ = category_group_

        self._instances[self.sku] = self

    @classmethod
    def instances(cls) -> List["Product"]:
        return list(cls._instances.values())

    @classmethod
    def get_by_sku(cls, sku: SKU) -> "Product":
        for ins in cls._instances.values():
            if ins.sku == sku:
                return ins
            
    def fit(self):
        super().fit()

    def insert_sql(self):
        curr_data = self.get_curr_date()
        weight = self.weight_kg if self.weight_kg else "NULL"
        product_category_id = self.product_category_.category_id if self.product_category_ else "NULL"
        category_group_id = self.category_group_.category_id if self.category_group_ else "NULL"
        main_img = self.main_image if self.main_image else "NULL"
        return f"""
        INSERT INTO [dbo].[DimProducts] 
        (ErpCode, SKU, Barcode, Weight, WeightNet, CountryID, IsShow, CreateDate, ProductCategoryID, CategoryGroupID, MainImage) 
        VALUES (NULL, {self.sku}, {self.ean_number}, {weight}, {self.weight_net_kg}, NULL, 1, {curr_data}, {product_category_id}, {category_group_id}, '{main_img}')
        """
    
    @classmethod
    def reset_index_sql(cls):
        return """
        DECLARE @id INT
        SET @id = 0 
        UPDATE table SET @id = id = @id + 1 
        """
import pandas as pd
import os
import psutil
import time

from config import logging, rsa_db
from config import EXCEL_PATH, IMAGE_PATH, LOGO_PATH
from entities import Entity, Category, Product, Translation, CategoryModel, ProductImage
from utils.image_util import ImageManager

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from utils.db_conn import DatabaseConnector


# // SHEET NAMES
# - Sheet1, 
# - ANA BAŞLIKLAR, 
# - AĞAÇ İŞLEME MAKİNALARI, 
# - İNŞAAT MAKİNALARI, 
# - ATÖLYE MAKİNALARI, 
# - TAŞLAMA MAKİNALARI, 
# - ODUN MAKİNALARI, 
# - BAHÇE MAKİNALARI, 
# - TESTERE MAKİNALARI

class ExtractData:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.data = {
            "main": {},
            "categories": {},
            "models": {}
        }
        self.rsa_db: DatabaseConnector = rsa_db
        self.logger = logging.getLogger(__name__)

        self._print_logo()

        self.extract()

    def extract(self):

        self.logger.info("Starting data extraction from {EXCEL_PATH}")

        try:

            # // MAIN SEPERATION
            self.data["main"]["Products"] = pd.read_excel(EXCEL_PATH, usecols='A:E,H:M,O:AT', sheet_name="Sheet1")
            self.logger.info(f"Loaded main product data")

            # // CATEGORY SEPERATION
            self.data["categories"]["MainCategory"] = pd.read_excel(EXCEL_PATH, usecols='A:B', nrows=8, header=None, sheet_name= "ANA BAŞLIKLAR", names=['EN', 'TR'])
            self.data["categories"]["WoodworkingCategory"] = pd.read_excel(EXCEL_PATH, usecols='A:B', nrows=5, skiprows=1, header=None, sheet_name="AĞAÇ İŞLEME MAKİNALARI", names=['TR', 'EN'], dtype={i: str for i in  ['Mil Kalıpçı', 'Planya', 'Ahşap Torna Tezgahı','Toz Toplayıcı', 'Testere Tablaları']})
            self.data["categories"]["ConstructionCategory"] = pd.read_excel(EXCEL_PATH, usecols='A:B', nrows=14, skiprows=1, header=None, sheet_name="İNŞAAT MAKİNALARI", names=['TR', 'EN'], dtype={i: str for i in ['Titreşimli Tokmak', 'Alçıpan Kaldırıcı', 'Hava Temizleyici', 'Duvar Kaplama Aletleri', 'Kompaktör', 'Boya Püskürtme', 'Jeneratör', 'Taş Kesme Makinası ve Fayans Kesici', 'Boya ve Harç Karıştırıcı', 'Kırıcı Delici', 'Kırıcı ', 'Beton Mikseri', 'Taş Ayrıştırıcı', 'Damperli Araç']})
            self.data["categories"]["WorkshopCategory"] = pd.read_excel(EXCEL_PATH, usecols='A:B', nrows=9, skiprows=1, header=None, sheet_name="ATÖLYE MAKİNALARI", names=['TR', 'EN'], dtype={i: str for i in ['Matkap', 'Kaldırma Ekipmanları', 'Atölye Ekipmanları', 'Metal Kesme Makinaları', 'Temizlik Makinaları', 'Kompresör', 'Kül Temizleme', 'Kaynak Makinaları', 'Islak / Kuru Temizlik Makinaları']})
            self.data["categories"]["GrindingCategory"] = pd.read_excel(EXCEL_PATH, usecols='A:B', nrows=9, skiprows=1, header=None, sheet_name="TAŞLAMA MAKİNALARI", names=['TR', 'EN'], dtype={i: str for i in ['Cilalama-Parlatma Makinaları', 'Bant ve Disk Zımpara', 'Taş Motoru', 'Salınımlı Silindirik Zımpara', 'Islak ve Kuru Taşlama Makinaları', 'Islak Taşlama Makinaları', 'Matkap Ucu Bileyici', 'Salınımlı Mil Zımpara', 'Takım Bileme']})
            self.data["categories"]["FirewoodCategory"] = pd.read_excel(EXCEL_PATH, usecols='A:B', nrows=4, skiprows=1, header=None, sheet_name="ODUN MAKİNALARI", names=['TR', 'EN'], dtype={i: str for i in ['Yakacak Odun Testereleri', 'Taşıma Sistemleri', 'Kütük Parçalayıcı', 'Sehpa']})
            self.data["categories"]["GardenCategory"] = pd.read_excel(EXCEL_PATH, usecols='A:B', nrows=15, skiprows=1, header=None, sheet_name="BAHÇE MAKİNALARI", names=['TR', 'EN'], dtype={i: str for i in ['Elek Makinaları', 'Bahçe Öğütme Makinaları', 'Zincir Bileyici', 'Burgu Makinaları', 'Çok İşlevli Bahçe Makinaları', 'Yüksek Basınçlı Yıkama Makinaları', 'Pompa', 'Süpürme Araçları', 'Motorlu Testereler', 'Çit Kesim Makinaları', 'Tırpanlar', 'Üfleyiciler', 'Çim Biçme Makinaları', 'Çim Kazıyıcılar', 'Çapa Makinaları']})
            self.data["categories"]["SawingCategory"] = pd.read_excel(EXCEL_PATH, usecols='A:B', nrows=11, header=None, sheet_name="TESTERE MAKİNALARI", names=['TR', 'EN'])
            self.logger.info("Loaded category data")

            # // MODELS SEPERATION
            self.data["models"]["WoodworkingModels"] = pd.read_excel(EXCEL_PATH, usecols='B:F', nrows=4, skiprows=7, sheet_name="AĞAÇ İŞLEME MAKİNALARI", dtype={"Mil Kalıpçı": str})
            self.data["models"]["ConstructionModels"] = pd.read_excel(EXCEL_PATH, usecols='B:O', nrows=8, skiprows=16, sheet_name="İNŞAAT MAKİNALARI")
            self.data["models"]["WorkshopModels"] = pd.read_excel(EXCEL_PATH, usecols='B:J', nrows=17, skiprows=13, sheet_name="ATÖLYE MAKİNALARI")
            self.data["models"]["GrindingModels"] = pd.read_excel(EXCEL_PATH, usecols='B:J', nrows=9, skiprows=12, sheet_name="TAŞLAMA MAKİNALARI")
            self.data["models"]["FirewoodModels"] = pd.read_excel(EXCEL_PATH, usecols='B:E', nrows=17, skiprows=7, sheet_name="ODUN MAKİNALARI")
            self.data["models"]["GardenModels"] = pd.read_excel(EXCEL_PATH, usecols='B:P', nrows=12, skiprows=18, sheet_name="BAHÇE MAKİNALARI")
            self.data["models"]["SawingModels"] = pd.read_excel(EXCEL_PATH, usecols='B:K', nrows=8, skiprows=14, sheet_name="TESTERE MAKİNALARI")
            self.logger.info("Loaded model data")
        
        except Exception as e:
            self.logger.error(f"Error during data extraction: {e}")
            raise

        return self._instance

    # !! RETURN SELF DECORATOR REQ (def::void=False)
    def execute(self, print_param_ = False):

        # // DATA EXTACTION // #
        self.logger.info("Starting data execution")
        
        # : Category Section
        exc_cats = ["WoodworkingCategory", "ConstructionCategory", "WorkshopCategory", "GrindingCategory", "FirewoodCategory", "GardenCategory", "SawingCategory", "AccessoryCategory"]
        main_cat = { exc_cats[index]: Category(row["EN"], row["TR"]) for index, row in self.data["categories"]["MainCategory"].iterrows()}
        self.logger.info("Main categories seperated")
        self.logger.debug(f"Main category amount: {len(main_cat)}")

        excluded_main_cat = self.data["categories"].copy()
        del excluded_main_cat["MainCategory"]
        for key, val in excluded_main_cat.items():
            parent_ = main_cat[key]
            for _, row in val.iterrows():
                Category(en_name = row["EN"], 
                        tr_name = row["TR"], 
                        parent=parent_)
                
        for c in Category.instances():
            c.fit()
                
        self.logger.info("Sub categories seperated")
        self.logger.debug(f"Sub category amount: {len(Category.instances())-len(main_cat)}")
        
        # : Models Section
        for name, models in self.data["models"].items():
            for column in models.columns:
                category_ins_ = Category.get_elem_by_tr_name(column)
                
                for model_code in models[column]:
                    if not pd.isnull(model_code):
                        category_ins_.models.append(model_code)
                        CategoryModel(category_ins_.category_id, model_code)

        self.logger.info("Category models seperated")

        # : Product Section 

        for _, row in self.data["main"]["Products"].iterrows():
            weight_kg = row["Weight_kg"]
            if isinstance(weight_kg, str):
                weight_kg = float(weight_kg.replace("Kg", ""))

            weight_net_kg = row["Weight_net_kg"]
            if isinstance(weight_net_kg, str):
                weight_net_kg = float(weight_net_kg.replace("Kg", ""))

            product_ins_ = Product(sku = row["SKU"],
                            ean_number= row["EANNumber"],
                            brand_name = row["Brandname"],
                            category_group = row["CategoryGroup_1"],
                            product_category = row["ProductCategory"],
                            weight_kg = weight_kg,
                            weight_net_kg = weight_net_kg,
                            product_title_en = row["ProductTitle_de"],
                            product_title_tr = row["ProductTitle_TR"],
                            product_description_en = row["Description"],
                            product_description_tr = row["Description (TR)"],
                            country_of_origin = row["CountryOfOrigin"])
            
            product_ins_.fit()

        # : Product - Category Section
            product_ins_.product_category_ = Category.get_elem_by_en_name(product_ins_.product_category)
            product_ins_.category_group_ = Category.get_elem_by_en_name(product_ins_.category_group)

        # : Product Image Section

            img_start = f"sp-{product_ins_.sku}-"
            # self.logger.debug(f"Product seperated img starter tag created : {img_start}_[1-30]")

            main_image = row["MainImageURL"]
            if not pd.isnull(main_image):
                main_image_name = img_start + "main"
                product_ins_.images.append((main_image_name, str(main_image)))
                product_ins_.main_image = main_image_name
                ProductImage(product_ins_.product_id, main_image_name, str(main_image), 1)


            for index in range(1, 31):
                img_str = f"ImageURL_{index}"
                image_url = row[img_str]
                if not pd.isnull(image_url):
                    product_ins_.images.append((img_start + str(index), str(image_url)))
                    ProductImage(product_ins_.product_id, img_start + str(index), str(image_url), index)

        self.logger.info("Products seperated")

        # : Translation Section

        for c in Category.instances():
            Translation(c.category_id, c.__meta__.DB_TABLE_NAME, "EN", "category_name_en", Category.convert_to_html(c.en_name))
            Translation(c.category_id, c.__meta__.DB_TABLE_NAME, "TR", "category_name_tr", Category.convert_to_html(c.tr_name))

        for p in Product.instances():
            Translation(p.product_id, p.__meta__.DB_TABLE_NAME, "EN", "product_title_en", p.product_title_en)
            Translation(p.product_id, p.__meta__.DB_TABLE_NAME, "TR", "product_title_en", p.product_title_tr)
            Translation(p.product_id, p.__meta__.DB_TABLE_NAME, "EN", "product_description_en", Product.convert_to_html(p.product_description_en))
            Translation(p.product_id, p.__meta__.DB_TABLE_NAME, "TR", "product_description_tr", Product.convert_to_html(p.product_description_tr))

        if print_param_:
            # > Category
            for c in Category.instances():
                print(c)
            # > Product
            for c in Product.instances():
                print(c)
            # > Translation
            for t in Translation.instances():
                print(t)
            # > Category Model
            for cm in CategoryModel.instances():
                print(cm)
            # > Product Image
            for pi in ProductImage.instances():
                print(pi)

            print()
            print("<| INSTANCE AMOUNT |> ")
            print(" + Product:", len(Product.instances()))
            print(" + Category:", len(Category.instances()))
            print(" + Category Model:", len(CategoryModel.instances()))
            print(" + Product Image:", len(ProductImage.instances()))
            print(" + Translation:", len(Translation.instances()))
            print()

        # //////////////////// #

        return self._instance

    # !! RETURN SELF DECORATOR REQ (def::void=False)
    def insert(self, opt_= {}):

        if not opt_:
            self.logger.warning("opt_ (operation) not declared or empty")
            return

        self.rsa_db.connect()

        if (opt_.get("img", {}).get("clear_all", False)):
            # remove all image
            if os.path.exists(IMAGE_PATH):
                for img in os.listdir(IMAGE_PATH):
                    os.remove(img)

        # for comm, entities in opt_.get("commands", {}).items():
        #     for entity in entities:
        #         _entity_cls: Entity | None = entity if issubclass(entity, Entity) else None
        #         if not _entity_cls:
        #             self.logger.warning(f"Child class is not instance of Entity::{entity.__class__.__name__}")
        #             continue

        #         match (comm):
        #             case "clear":
        #                 self.logger.info(f"Clearing the instance of table @{_entity_cls.__name__}::{_entity_cls.__meta__.DB_TABLE_NAME}")
        #                 self.rsa_db.execute(_entity_cls.clear_table_sql())
        #             case "insert":
        #                 self.logger.info(f"Inserting the instance of table @{_entity_cls.__name__}::{_entity_cls.__meta__.DB_TABLE_NAME}")
        #                 for ins in _entity_cls.instances():
        #                     self.rsa_db.execute(ins.insert_sql())
                        
        #             case _:
        #                 self.logger.warning("Entity command not found: " + comm, "(ex: clear, insert, )")
        #                 raise RuntimeError

        self.rsa_db.close()

        # print_opt_ = opt_.get("debug", {}).get("print", {}) 
        # total_h = len(self.rsa_db._execution_history)
        # broken_h = 0
        # if (print_opt_):
        #     if (print_opt_.get("db_history", False)):
        #         for q, s in self.rsa_db._execution_history:
        #             if not s:
        #                 broken_h += 1
        #             print(q)
        #             print("\t=>", s)
        #     print(f"\n\t > Success Rate: {total_h-broken_h}/{total_h} = ~{((total_h-broken_h)//total_h)*100}%")

        
        self.logger.info("Insert Completed !")
        
        return self._instance
    
    def image_space(self):
        total_count = 0
        for p in Product.instances():
            total_count += len(p.images)

        required_space = round(total_count * 0.00593, 2)
        free_space = round(psutil.disk_usage('/').free / (1024 ** 3), 2)
        space_str = str(required_space) + "GB "
        if (required_space / 2) > free_space:
            space_str += " 🟡"
        elif (required_space > free_space):
            space_str += f" 🔴 (free:{free_space})"
        else:
            space_str += " 🟢"

        print(" IMAGE ".center(45, "/"))
        print()
        print("Count: " + str(total_count))
        print("Required space: ~" + space_str)
        print()
        print(45 * "/")

    def _print_logo(self):
        
        with open(LOGO_PATH, "r") as lf:
            print(lf.read())

ExtractData() \
    .execute(
        print_param_ = False
    ) \
    .insert(
        opt_ = {
            "commands": {
                "clear": [Category, Product, CategoryModel, ProductImage, Translation],
                "insert": [Category, Product, CategoryModel, ProductImage, Translation],
            },
            "img": {
                "clear_all": False,
                "check_all": True,
                "download_all": False
            },
            "debug": {
                "print": {
                    "db_history": False
                } 
            }
        }
    )
    
print("\n\tSystem exit ~~")

from uuid import uuid4
import inspect
import pandas as pd
from datetime import datetime

from typing import List, Dict, Any
from uuid import UUID

from decorators.entity_decorators import not_implemented_method, not_implemented_class_method

class Entity:

    _entity_ins: Dict[UUID, "Entity"] = {}
    _increment_counter = {}

    class __meta__:

        DEFAULT_PARAMS = [
            "ENTITY_NAME",
            "DB_TABLE_NAME"
        ]
        
        @classmethod
        def get_elem(cls, param_name: str) -> Any:
            return getattr(cls, param_name)

    def __init__(self) -> None:
        
        self._id: UUID = uuid4()
        Entity._entity_ins[self._id] = self

    def __str__(self) -> str:
        def format_value(value: Any, indent: int = 1) -> str:
            indent_str = ' ' * (indent * 4)
            next_indent_str = ' ' * ((indent + 1) * 4)
            
            if isinstance(value, list):
                formatted_list = ",\n".join(next_indent_str + format_value(item, indent + 1) for item in value)
                return f"[\n{formatted_list}\n{indent_str}]"
            elif isinstance(value, dict):
                formatted_dict = ",\n".join(
                    next_indent_str + f"{k}: {format_value(v, indent + 1)}" for k, v in value.items()
                )
                return f"{{\n{formatted_dict}\n{indent_str}}}"
            elif hasattr(value, '__dict__'):
                return value.__str__().replace("\n", "\n" + next_indent_str)
            else:
                return repr(value)

        class_name: str = self.__class__.__name__
        ins_id: Any = getattr(self, f"{class_name.lower()}_id", None) or ""
        attr_str: str = ',\n'.join(f" * {key}: {format_value(value)}" for key, value in self._elem_attrs().items())
        return f"{class_name}#{ins_id} </\n{attr_str} />"
    
    def _elem_attrs(self) -> Dict[str, Any]:
        attributes: Dict[str, Any] = vars(self)
        return {key.lstrip('_'): value for key, value in attributes.items()}

    @classmethod
    def increment(cls) -> int:
        c_name = cls.__name__.lower() + "_counter"

        if c_name not in cls._increment_counter:
            cls._increment_counter[c_name] = 0
        cls._increment_counter[c_name] += 1

        return cls._increment_counter[c_name]
    
    @staticmethod
    def convert_to_html(value: str | None) -> str:
        if not value and not isinstance(value, str):
            return
        space_esc_char = "&nbsp;"
        replace_chars = {
            "\n": "<br>",
            "\t": 4 * space_esc_char,
            " ": space_esc_char,
            "'": "''"
        }
        for item, rep in replace_chars.items():
            value = value.replace(item, rep)
        return value

    def fit(self):
        
        for name, item in self._elem_attrs().items():
            if not any([isinstance(item, _type) for _type in [bool, list, dict, set]]) and pd.isnull(item):
                setattr(self, name, None)

    @not_implemented_method
    def insert_sql(self):
        pass
    
    @not_implemented_class_method
    @classmethod
    def reset_index_sql(cls):
        pass

    @classmethod
    def clear_table_sql(cls):
        return f"TRUNCATE TABLE [dbo].[{cls.__meta__.DB_TABLE_NAME}]"

    @classmethod
    def get_curr_date(cls) -> str:
        return f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'"
    
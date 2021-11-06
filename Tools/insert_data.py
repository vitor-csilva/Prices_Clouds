import os
import json
from datetime import datetime
from typing import Any, Dict, List, Tuple
from Database.conn_database import SQL
from Model.ToolsModel import Tools as t


def verify_key_dict(keys_dict: List[Any], keys: List[Any]):
    """Function verify if dict contains the key.
        keys_dict: Keys of dictionary
        keys: List of string will be verify if exist in dictionary
    """

    for key in keys:
        found = [i for i in keys_dict if key.upper() in i.upper()]
        if len(found):
            return found[0]

def map_columns_machine():
    return {
        "memory": ["memory", "ram", "mem"],
        "cpu": ["cpu"],
        "storage": ["stg", "stor"]
    }  # Map columns for insert db


def struct_machine_json_insert(machine: Dict[Any, Any], date, machine_name="unnamed"):
    """ Struct the json data for insert in database
        machine: All informations of machine colected
        date: Date of colected data
    """
    map_columns = map_columns_machine()
    dict_table = {"machine_name": machine_name, "colection_data": date}
    for collum, values in map_columns.items():
        key = verify_key_dict(list(machine.keys()), values)
        if key:
            dict_table[collum] = machine[key]
    return dict_table


def get_dict_pric(pric: Dict[Any, Any], keys_remove: List[Any] = None):
    keys_pric = list(pric.keys())
    dict_pric = {}
    if not isinstance(pric[keys_pric[0]], dict):
        return {"Unamed System": pric}
    if keys_remove:
        for key in keys_remove:
            dict_pric.update(pric[key])
    return pric

def insert_foreing_key(systems_machine: Dict[Any,Any], machines_insert: List[Any]):

    systems_machine_insert = []
    for num_machine in range(len(machines_insert)):

        machine_id = machines_insert[num_machine]['id']
        for system_machine in systems_machine[num_machine]:
            dict_aux = system_machine
            dict_aux['machine_id'] = machine_id
            systems_machine_insert.append(dict_aux)

    return  systems_machine_insert


def struct_pric_machine(pric: Dict[str, Any]) -> List[Any]:
    """ """
    list_system = []
    for name_sytem, regions in pric.items():
        for region, price in regions.items():
            list_system.append({
                "price": price,
                "region": region,
                "system_name": name_sytem,
                "machine_id": None,
            })
    return list_system

def struct_json_insert(data: Dict[str, Any], create_db=False, keys_remove=None):
    machines_insert = []
    systems_machine = {}
    colection_data = datetime.strptime(data['_id']['$date'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
    data.pop('_id', None)
    sql = SQL("root", "M!nerv%40123", "localhost", "clouds")
    if create_db:
        sql.create_tables_database()
    cont = 0
    for name_machine, machine in data.items():
        machine_dict_insert = struct_machine_json_insert(machine, colection_data, name_machine)
        machines_insert.append(machine_dict_insert)
        price_machine = get_dict_pric(machine['pric'], keys_remove)
        systems_machine[cont] = struct_pric_machine(price_machine)
        cont += 1

    sql.insert_in_bulk_machine(machines_insert)
    systems_machine_insert = insert_foreing_key(systems_machine, machines_insert)
    sql.insert_in_bulk_system_machine(systems_machine_insert)


def insert(path: str):
    lista_arquivos2 = ['aws_ondemand.json']

    for pasta, sub_pastas, arquivos in os.walk(path):
        for nome_arquivo in arquivos:
            if nome_arquivo in lista_arquivos2:
                caminho = os.path.abspath(f"{pasta}/{nome_arquivo}")
                with open(caminho) as f:
                    data = json.load(f)
                    struct_json_insert(data, False)


if __name__ == '__main__':
    insert("C:\\Users\\wanderson.viana\\Downloads\\Prices_Clouds\\Prices_Clouds")

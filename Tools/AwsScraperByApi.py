import ast
import time
import boto3
from Tools.insert_data import *
import pandas as pd
from Database.conn_database import SQL

def build_machine_insert(instance_attributes):
    machine_insert = {
        'machine_name' :instance_attributes["instanceType"],
        'memory': instance_attributes['memory'],
        'cpu': instance_attributes['vcpu'],
        'storage': instance_attributes['storage'],
        'processor': instance_attributes['physicalProcessor'],
        'colection_data': datetime.now()
    }
    return machine_insert


def get_prices_machines(prices, so, region, type):

        if prices is None:
            return []
        list_system = []
        for key, priceDimensions in prices.items():

            values = priceDimensions['priceDimensions']
            term_attributes = priceDimensions.get('termAttributes', None)
            find_key = [i for i in values.keys()][0]
            value = values[find_key]['pricePerUnit']['USD']
            description = values[find_key]['description']
            dict_insert = {'machine_id': None, 'price': value,
                           'region': region, 'system_name': so,
                           'description': description, 'type_machine': type
                          }
            if term_attributes:
                dict_insert['lease_contract_length'] = term_attributes['LeaseContractLength']
                dict_insert['offering_class'] = term_attributes['OfferingClass']
                dict_insert['purchase_option'] = term_attributes['PurchaseOption']

            list_system.append(dict_insert)

        return list_system

def build_request(operational_systems, pre_installed_sws, locations):
    values = "AttributeValues"
    systems = {"system": [[value['Value'] for value in operational_systems[values] if value['Value'] != 'NA']],
               "softwares": [[value['Value'] for value in pre_installed_sws[values] if value['Value'] != 'NA']],
               "locations": [[value['Value'] for value in locations[values] if value['Value'] != 'NA']]}
    df = pd.DataFrame.from_dict(systems, orient='index')
    df = df.transpose()
    df = df.explode('system')
    df = df.explode('softwares')
    return df.explode('locations')


def search():
    session = boto3.Session(region_name="us-east-1")
    pricing = session.client('pricing')
    sql = SQL("root", "M!nerv%40123", "localhost", "clouds")
    operational_systems = pricing.get_attribute_values(ServiceCode='AmazonEC2', AttributeName='operatingSystem') # get systems names
    pre_installed_sws = pricing.get_attribute_values(ServiceCode='AmazonEC2', AttributeName='preInstalledSw')# get software pre installed
    locations = pricing.get_attribute_values(ServiceCode='AmazonEC2', AttributeName='location') # get locations of machines ec2
    machines_insert = []
    systems_machine = {}
    current_query = 0
    df_request = build_request(operational_systems, pre_installed_sws, locations)
    qtd_query = len(df_request.index)

    for params_request in df_request.to_numpy():
        system, software, location = params_request
        print(f'AWS querying {system} {software}: {current_query+1} de {qtd_query}')
        more_results = True
        next_token = ''
        while more_results:
            response = pricing.get_products(
                ServiceCode='AmazonEC2',
                Filters=[
                    {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': system},
                    {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': software},
                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': location},
                    {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'}
                ],
                NextToken=next_token,
            )
            next_token = more_results = response['NextToken'] if 'NextToken' in response else ''
            for result in response['PriceList']:  # take atributes and price of machines

                price_description = ast.literal_eval(result)  # Convert str to dict
                instance_attributes = price_description['product']['attributes'] # informations of machine aws
                atributes_machine = build_machine_insert(instance_attributes)  # get atributes for insert bd

                machines_insert.append(atributes_machine)

                ondemand = price_description['terms'].get('OnDemand', None)
                reserved = price_description['terms'].get('Reserved', None)
                prices_machines = get_prices_machines(ondemand, system, location, 'OnDemand')
                prices_machines += get_prices_machines(reserved, system, location, 'Reserved')
                systems_machine[current_query] = prices_machines

        current_query += 1

    sql.insert_in_bulk_machine(machines_insert)
    systems_machine_insert = insert_foreing_key(systems_machine, machines_insert)
    sql.insert_in_bulk_system_machine(systems_machine_insert)

if __name__ == '__main__':
    start_time = time.time()
    search()
    end_time = time.time()
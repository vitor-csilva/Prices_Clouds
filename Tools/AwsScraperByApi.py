import ast
import time
import boto3
from Tools.insert_data import *

def encode_key(key):
    if key is None:
        return None
    return key.replace("\\", "\\\\").replace("$", "\\u0024").replace(".", "\\u002e")

def decode_key(key):
    return key.replace("\\u002e", ".").replace("\\u0024", "$").replace("\\\\", "\\")


def enconde_all_keys(d):
    new_d = {}
    for k, v in d.items():
        if isinstance(v, dict):
            new_d[encode_key(k)] = enconde_all_keys(v)
        else:
            new_d[encode_key(k)] = v
    return new_d


def save_json(collection, columns):
    document = enconde_all_keys(columns)
    #collection_database = database[collection]
    document['_id'] = datetime.now()
   # collection_database.insert_one(document)


def desc_to_code(region):

    regions = {
     'AWS GovCloud (US-East)': 'us-gov-east-1',
     'AWS GovCloud (US-West)': 'us-gov-west-1',
     'Africa (Cape Town)': 'af-south-1',
     'Asia Pacific (Hong Kong)': 'ap-east-1',
     'Asia Pacific (KDDI) - Osaka': 'ap-northeast-1-wl1-kix1',
     'Asia Pacific (KDDI) - Tokyo': 'ap-northeast-1-wl1-nrt1',
     'Asia Pacific (Mumbai)': 'ap-south-1',
     'Asia Pacific (Osaka)': 'ap-northeast-3',
     'Asia Pacific (SKT) - Daejeon': 'ap-northeast-2-wl1-cjj1',
     'Asia Pacific (Seoul)': 'ap-northeast-2',
     'Asia Pacific (Singapore)': 'ap-southeast-1',
     'Asia Pacific (Sydney)': 'ap-southeast-2',
     'Asia Pacific (Tokyo)': 'ap-northeast-1',
     'Canada (Central)': 'ca-central-1',
     'EU (Frankfurt)': 'eu-central-1',
     'EU (Ireland)': 'eu-west-1',
     'EU (London)': 'eu-west-2',
     'EU (Milan)': 'eu-south-1',
     'EU (Paris)': 'eu-west-3',
     'EU (Stockholm)': 'eu-north-1',
     'Middle East (Bahrain)': 'me-south-1',
     'South America (Sao Paulo)': 'sa-east-1',
     'US East (Boston)': 'us-east-1-wl1',
     'US East (Dallas)': 'us-east-1-dfw-1a',
     'US East (Philadelphia)': 'us-east-1-phl-1a',
     'US East (Houston)': 'us-east-1-iah-1',
     'US East (Miami)': 'us-east-1-mia-1',
     'US East (N. Virginia)': 'us-east-1',
     'US East (Ohio)': 'us-east-2',
     'US East (Verizon) - Atlanta': 'us-east-1-wl1-atl1',
     'US East (Verizon) - Boston': 'us-east-1-wl1',
     'US East (Verizon) - Dallas': 'us-east-1-wl1-dfw1',
     'US East (Verizon) - Miami': 'us-east-1-wl1-mia1',
     'US East (Verizon) - New York': 'us-east-1-wl1-nyc1',
     'US East (Verizon) - Washington DC': 'us-east-1-wl1-was1',
     'US West (Los Angeles)': 'us-west-2-lax-1',
     'US West (N. California)': 'us-west-1',
     'US West (Oregon)': 'us-west-2',
     'US West (Verizon) - Denver': 'us-west-2-wl1-den1',
     'US West (Verizon) - Las Vegas': 'us-west-2-wl1-las1',
     'US West (Verizon) - San Francisco Bay Area': 'us-west-2-wl1',
     'US West (Verizon) - Seattle': 'us-west-2-wl1-sea1',
     'US West (Denver)': 'us-west-2-den-1a',
     'US East (Verizon) - Chicago': 'us-east-1-wl1-chi-wlz-1',
     'US East (Verizon) - Houston': 'us-east-1-wl1-iah-wlz-1',
     'US West (Verizon) - Phoenix': 'us-west-2-wl1-phx-wlz-1',
     'US East (Chicago)': 'us-east-1-chi-1a',
     'US East (Kansas City 2)': 'us-east-1-iah-1a',
     'US East (Minneapolis)':'us-east-1-wl1-mia-wlz-1',
     'EU West (Vodafone) - London':'eu-west-2-wl1-lon-wlz-1',
     'Europe (Vodafone) - London':'eu-west-2-wl1-lon-wlz-1'
     }
    
    region_get = regions.get(region, None)
    if region_get:
        return region_get
    raise RuntimeError("Invalid region: " + region)

def keys_rename():
    return {'currentGeneration':'gen','instanceFamily':'fam','physicalProcessor':'proc','clockSpeed':'cs','memory':'mem',
     'storage':'stg', 'networkPerformance':'net', 'processorArchitecture':'parc','dedicatedEbsThroughput':'ebt',
     'enhancedNetworkingSupported':'ens','intelAvxAvailable':'iaa','intelAvx2Available':'i2a',  'intelTurboAvailable':'ita',
     'normalizationSizeFactor':'nsf','processorFeatures':'pf', 'capacitystatus':'cap', 'usagetype':'ust', 'licenseModel':'lim' }

def keys_delet_dict():
    return ['operation', 'capacitystatus', 'instancesku', 'location', 'operatingSystem', 'locationType', 'servicecode', 'servicename','preInstalledSw','tenancy', 'instanceType']

def search():
    session = boto3.Session(region_name="us-east-1")
    pricing = session.client('pricing')
    operational_systems = pricing.get_attribute_values(ServiceCode='AmazonEC2', AttributeName='operatingSystem')
    pre_installed_sws = pricing.get_attribute_values(ServiceCode='AmazonEC2', AttributeName='preInstalledSw')
    locations = pricing.get_attribute_values(ServiceCode='AmazonEC2', AttributeName='location')
    doc_ondemand = {}
    doc_res_std = {}
    doc_res_con = {}
    i = 1
    qtd_query = len(operational_systems['AttributeValues']) * len(pre_installed_sws['AttributeValues']) * \
                len(locations['AttributeValues'])
    for operational_system in operational_systems['AttributeValues']:
        for pre_installed_sw in pre_installed_sws['AttributeValues']:
            for location in locations['AttributeValues']:
                pre_sw = pre_installed_sw['Value']
                so = operational_system['Value']
                region = location['Value']

                if so != 'NA' and region != 'NA':
                    region_code = desc_to_code(region)
                    full_so = so if pre_sw == 'NA' else f'{so} {pre_sw}'
                    print(f'AWS querying {full_so}: {i} de {qtd_query}')
                    i += 1
                    more_results = True
                    next_token = ''

                    while more_results:
                        if next_token == '':
                            response = pricing.get_products(
                                ServiceCode='AmazonEC2',
                                Filters=[
                                    {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': so},
                                    {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': pre_sw},
                                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region},
                                    {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'}
                                ],
                            )
                        else:
                            response = pricing.get_products(
                                ServiceCode='AmazonEC2',
                                Filters=[
                                    {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': so},
                                    {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': pre_sw},
                                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region},
                                    {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'}
                                ],
                                NextToken=next_token,
                            )

                        next_token = response['NextToken'] if 'NextToken' in response else ''

                        if next_token == '':
                            more_results = False

                        for result in response['PriceList']:
                            price_description = ast.literal_eval(result)
                            instance_attributes = price_description['product']['attributes']
                            instance_type = instance_attributes['instanceType']
                            [instance_attributes.pop(k, None) for k in keys_delet_dict()]
                            for key, map in keys_rename().items():
                                value = instance_attributes.get(key, None)
                                if value:
                                    instance_attributes.pop(key)
                                    instance_attributes[map] = value

                            terms = price_description['terms']
                            upfront = 0
                            for term in terms:
                                for price in terms[term]:
                                    for price_dim in terms[term][price]['priceDimensions']:
                                        unit = terms[term][price]['priceDimensions'][price_dim]['unit']
                                        value = float(terms[term][price]['priceDimensions'][price_dim]['pricePerUnit']['USD'])
                                        if term == 'OnDemand':
                                            if 'Reservation' not in terms[term][price]['priceDimensions'][price_dim]['description']:
                                                if instance_type not in doc_ondemand:
                                                    doc_ondemand[instance_type] = instance_attributes.copy()
                                                if 'pric' not in doc_ondemand[instance_type]:
                                                    doc_ondemand[instance_type]['pric'] = {}
                                                if full_so not in doc_ondemand[instance_type]['pric']:
                                                    doc_ondemand[instance_type]['pric'][full_so] = {}
                                                doc_ondemand[instance_type]['pric'][full_so][region_code] = value
                                        elif term == 'Reserved':
                                            duration = terms[term][price]['termAttributes']['LeaseContractLength']
                                            offer_class = terms[term][price]['termAttributes']['OfferingClass']
                                            purchase = terms[term][price]['termAttributes']['PurchaseOption']
                                            if purchase == 'No Upfront':
                                                purchase = 'nup'
                                            elif purchase == 'Partial Upfront':
                                                purchase = 'pup'
                                            elif purchase == 'All Upfront':
                                                purchase = 'aup'

                                            if offer_class == 'standard':
                                                if instance_type not in doc_res_std:
                                                    doc_res_std[instance_type] = instance_attributes.copy()
                                                if 'pric' not in doc_res_std[instance_type]:
                                                    doc_res_std[instance_type]['pric'] = {}
                                                if duration not in doc_res_std[instance_type]['pric']:
                                                    doc_res_std[instance_type]['pric'][duration] = {}
                                                if purchase not in doc_res_std[instance_type]['pric'][duration]:
                                                    doc_res_std[instance_type]['pric'][duration][purchase] = {}
                                                if full_so not in doc_res_std[instance_type]['pric'][duration][purchase]:
                                                    doc_res_std[instance_type]['pric'][duration][purchase][full_so] = {}
                                                if purchase == 'nup' and unit == 'Hrs':
                                                    doc_res_std[instance_type]['pric'][duration][purchase][full_so][region_code] = {'upront': 0, 'hour': value}
                                                elif unit == 'Quantity':
                                                    upfront = value
                                                elif unit == 'Hrs':
                                                    doc_res_std[instance_type]['pric'][duration][purchase][full_so][region_code] = {'upront': upfront,
                                                                                                                               'hour': value}
                                            elif offer_class == 'convertible':
                                                if instance_type not in doc_res_con:
                                                    doc_res_con[instance_type] = instance_attributes.copy()
                                                if 'pric' not in doc_res_con[instance_type]:
                                                    doc_res_con[instance_type]['pric'] = {}
                                                if duration not in doc_res_con[instance_type]['pric']:
                                                    doc_res_con[instance_type]['pric'][duration] = {}
                                                if purchase not in doc_res_con[instance_type]['pric'][duration]:
                                                    doc_res_con[instance_type]['pric'][duration][purchase] = {}
                                                if full_so not in doc_res_con[instance_type]['pric'][duration][purchase]:
                                                    doc_res_con[instance_type]['pric'][duration][purchase][full_so] = {}
                                                if purchase == 'nup' and unit == 'Hrs':
                                                    doc_res_con[instance_type]['pric'][duration][purchase][full_so][region_code] = {'upront': 0,
                                                                                                                               'hour': value}
                                                elif unit == 'Quantity':
                                                    upfront = value
                                                elif unit == 'Hrs':
                                                    doc_res_con[instance_type]['pric'][duration][purchase][full_so][region_code] = {
                                                        'upront': upfront,
                                                        'hour': value}
                else:
                    i = i + (len(operational_systems['AttributeValues']) * len(pre_installed_sws['AttributeValues']))

    struct_json_insert(doc_ondemand)
    save_json('aws_ondemand', doc_ondemand)
    save_json('aws_res_std', doc_res_std)
    save_json('aws_res_con', doc_res_con)


if __name__ == '__main__':
    start_time = time.time()
    search()
    end_time = time.time()
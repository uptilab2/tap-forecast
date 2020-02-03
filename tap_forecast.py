import os
import json
import singer
import singer.metrics as metrics
import singer.bookmarks as bookmarks
import requests
import collections

session = requests.Session()
logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['API_KEY', 'API_URL']


NO_ID_PROPERTIES = ['team', 'rate_card_rates']

class AuthException(Exception):
    pass

class NotFoundException(Exception):
    pass

# TODO : real raise auth or not found, hope this doesn't send pagination
def request_get(url, headers={}):
    resp = session.request(method='get', url=url)
    if resp.status_code == 401:
        raise AuthException(resp.text)
    if resp.status_code == 403:
        raise AuthException(resp.text)
    if resp.status_code == 404:
        raise NotFoundException(resp.text)
    return resp

# TODO : generic func to record and bookmark the data
def get_all_data(name, schema, state, url, mdata=None):
    response = request_get(url+name)
    if response:
        with metrics.record_counter(name) as counter:
            records = response.json()
            extraction_time = singer.utils.now()
            for record in records:
                with singer.Transformer() as transformer:
                    rec = transformer.transform(record, schema)
                singer.write_record(name, rec, time_extracted=extraction_time)
                singer.write_bookmark(state, name, {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state

def get_all_data_with_projects(name, schema, state, url, mdata=None):
    for project_id in get_all_objects_id(url, 'projects'):
        response = request_get(url+f'projects/{project_id}/{name}')
        if response:
            with metrics.record_counter(name) as counter:
                records = response.json()
                extraction_time = singer.utils.now()
                for record in records:
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(record, schema)
                    singer.write_record(name, rec, time_extracted=extraction_time)
                    singer.write_bookmark(state, name, {'since': singer.utils.strftime(extraction_time)})
                    counter.increment()
    return state

def get_all_objects_id(url, name):
    response = request_get(url+name)
    if response:
        objects = response.json()
        for obj in objects:
            yield obj.get('id')

def get_all_rate_card_rates(name, schema, state, url, mdata=None):
    for rate_card_id in get_all_objects_id(url, 'rate_cards'):
        print(rate_card_id)
        print(name)
        response = request_get(url+f'rate_cards/{rate_card_id}/{name}')
        if response:
            with metrics.record_counter(name) as counter:
                records = response.json()
                extraction_time = singer.utils.now()
                for records in records:
                    with metrics.record_counter(name) as counter:
                        rec = transformer.transform(record, schema)
                    singer.write_record(name, rec, time_extracted=extraction_time)
                    singer.write_bookmark(state, name, {'since': singer.utils.strftime(extraction_time)})
                    counter.increment()
    return state

def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        # mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            # 'metadata' : metadata.to_list(mdata),
            'key_properties': ['id'] if schema_name in NO_ID_PROPERTIES else '',
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('tap_forecast')):
        logger.info(f'extracting {filename} ========================')
        path = get_abs_path('tap_forecast') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

def translate_state(state, catalog, organization):
    nested_dict = lambda: collections.defaultdict(nested_dict)
    new_state = nested_dict()

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        for org in organization:
            if bookmarks.get_bookmark(state, org, stream_name):
                return state
            if bookmarks.get_bookmark(state, stream_name, 'since'):
                new_state['bookmarks'][org][stream_name]['since'] = bookmarks.get_bookmark(state, stream_name, 'since')

    return new_state
    
CUSTOM_SYNC_FUNC = {
    'milestones': get_all_data_with_projects,
    'team': get_all_data_with_projects,
    'rate_card_rates': get_all_rate_card_rates,
    # 'reapeating': get_all_rate_card_rates, maybe use projects
    'sprints': get_all_data_with_projects,
    'sub_tasks': get_all_data_with_projects,
    'workflow_columns': get_all_data_with_projects,
}

def do_sync_mode(config, state, catalog):
    session.headers.update({'X-FORECAST-API-KEY': config['API_KEY']})

    state = translate_state(state, catalog, ['uptilab'])

    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        
        singer.write_schema(stream_id, stream_schema, stream['key_properties'])

        # properties who need id from another properties
        if stream_id in CUSTOM_SYNC_FUNC:
            sync_func = CUSTOM_SYNC_FUNC[stream_id]
            state = sync_func(stream['stream'], stream_schema, state, url=config['API_URL'])
        else:
            state = get_all_data(stream['stream'], stream_schema, state, url=config['API_URL'])

        singer.write_state(state)

def do_discover():
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        do_discover()
    else:
        catalog = get_catalog()
        do_sync_mode(args.config, args.state, catalog)


if __name__ == '__main__':
    main()

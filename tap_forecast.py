import os
import json
import singer
import singer.metrics as metrics
import singer.bookmarks as bookmarks
import singer.metadata as metadata
import requests
import collections

session = requests.Session()
logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['API_KEY']
API_URL = "https://api.forecast.it/api/v1/"
NO_ID_PROPERTIES = ['team', 'rates']


class AuthException(Exception):
    pass


class NotFoundException(Exception):
    pass


def request_get(url, headers={}):
    resp = session.request(method='get', url=url)
    if resp.status_code == 401:
        raise AuthException(resp.text)
    if resp.status_code == 403:
        raise AuthException(resp.text)
    if resp.status_code == 404:
        raise NotFoundException(resp.text)
    return resp


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
                singer.write_bookmark(state, name, 'updated_at',
                                      singer.utils.strftime(extraction_time))
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
                    singer.write_record(name, rec,
                                        time_extracted=extraction_time)
                    singer.write_bookmark(
                        state,
                        name,
                        'updated_at',
                        singer.utils.strftime(extraction_time)
                    )
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
        response = request_get(url+f'rate_cards/{rate_card_id}/{name}')
        if response:
            with metrics.record_counter(name) as counter:
                records = response.json()
                extraction_time = singer.utils.now()
                for record in records:
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(record, schema)
                    singer.write_record(name, rec,
                                        time_extracted=extraction_time)
                    singer.write_bookmark(
                        state,
                        name,
                        'updated_at',
                        singer.utils.strftime(extraction_time)
                    )
                    counter.increment()
    return state


def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': metadata.get_standard_metadata(
                schema,
                schema_name,
                ['id'] if schema_name not in NO_ID_PROPERTIES else None,
                'updated_at',
            ),
            'key_properties': ['id'] if schema_name in NO_ID_PROPERTIES else ''
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
        if bookmarks.get_bookmark(state, stream_name, 'updated_at'):
            new_state['bookmarks'][stream_name]['updated_at'] = bookmarks. \
                get_bookmark(state, stream_name, 'updated_at')

    return new_state


CUSTOM_SYNC_FUNC = {
    'milestones': get_all_data_with_projects,
    'team': get_all_data_with_projects,
    'rates': get_all_rate_card_rates,
    'repeating_cards': get_all_data_with_projects,
    'sprints': get_all_data_with_projects,
    'sub_tasks': get_all_data_with_projects,
    'workflow_columns': get_all_data_with_projects,
}


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', False):
            selected_streams.append(stream['tap_stream_id'])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected', None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams


def do_sync_mode(config, state, catalog):
    session.headers.update({'X-FORECAST-API-KEY': config['API_KEY']})

    selected_stream_ids = get_selected_streams(catalog)

    state = translate_state(state, catalog, ['uptilab'])

    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']

        if stream_id in selected_stream_ids:
            logger.info(f'{stream_id} is selected')
            singer.write_schema(stream_id, stream_schema,
                                stream['key_properties'])

            # properties who need id from another properties
            if stream_id in CUSTOM_SYNC_FUNC:
                sync_func = CUSTOM_SYNC_FUNC[stream_id]
                state = sync_func(stream['stream'], stream_schema, state,
                                  url=API_URL)
            else:
                state = get_all_data(stream['stream'], stream_schema, state,
                                     url=API_URL)

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
        catalog = args.properties if args.properties else get_catalog()
        do_sync_mode(args.config, args.state, catalog)


if __name__ == '__main__':
    main()

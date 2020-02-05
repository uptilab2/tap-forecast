import os
import json
import singer
import singer.metrics as metrics
import singer.metadata as metadata
import requests
import collections

session = requests.Session()
logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['API_KEY']
API_URL = "https://api.forecast.it/api/v1/"
CUSTOM_KEY_PROPERTIES = {
    'team': ['project_id'],
    'rates': ['rate_card_id'],
    'milestones': ['id', 'project_id'],
    'repeating_cards': ['id', 'project_id'],
    'sprints': ['id', 'project_id'],
    'sub_tasks': ['id', 'project_id'],
    'workflow_columns': ['id', 'project_id']
}


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
        bookmark = singer.get_bookmark(state, name, 'updated_at')
        if bookmark is None:
            args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
            bookmark = args.config['start_date']
        new_bookmark = bookmark

        with metrics.record_counter(name) as counter:
            records = response.json()
            extraction_time = singer.utils.now()
            for record in records:
                with singer.Transformer() as transformer:
                    rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                    new_bookmark = max(new_bookmark, rec['updated_at'])
                    if rec.get('updated_at') > bookmark:
                        singer.write_record(name, rec,
                                            time_extracted=extraction_time)
                        counter.increment()
                singer.write_bookmark(
                    state,
                    name,
                    'updated_at',
                    new_bookmark
                )
    return state


def get_all_data_with_projects(name, schema, state, url, mdata):
    with metrics.record_counter(name) as counter:
        for project_id in get_all_objects_id(url, 'projects'):
            response = request_get(url+f'projects/{project_id}/{name}')
            if response:
                bookmark = singer.get_bookmark(state, name, 'updated_at')
                if bookmark is None:
                    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
                    bookmark = args.config['start_date']
                new_bookmark = bookmark
                
                records = response.json()
                extraction_time = singer.utils.now()
                for record in records:
                    with singer.Transformer() as transformer:
                        record['project_id'] = project_id
                        rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                        new_bookmark = max(new_bookmark, rec['updated_at'])
                        if rec.get('updated_at') > bookmark:
                            singer.write_record(name, rec,
                                                time_extracted=extraction_time)
                            counter.increment()
                    singer.write_bookmark(
                        state,
                        name,
                        'updated_at',
                        new_bookmark
                    )
    return state


def get_all_objects_id(url, name):
    response = request_get(url+name)
    if response:
        objects = response.json()
        for obj in objects:
            yield obj.get('id')


def get_all_rate_card_rates(name, schema, state, url, mdata=None):
    with metrics.record_counter(name) as counter:
        for rate_card_id in get_all_objects_id(url, 'rate_cards'):
            response = request_get(url+f'rate_cards/{rate_card_id}/{name}')
            if response:
                # get bookmark and if doesn't exists get['start_date'] as first init
                bookmark = singer.get_bookmark(state, name, 'updated_at')
                if bookmark is None:
                    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
                    bookmark = args.config['start_date']
                new_bookmark = bookmark

                records = response.json()
                extraction_time = singer.utils.now()
                for record in records:
                    with singer.Transformer() as transformer:
                        record['rate_card_id'] = rate_card_id
                        rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                        new_bookmark = max(new_bookmark, rec['updated_at'])
                        if rec.get('updated_at') > bookmark:
                            singer.write_record(name, rec,
                                                time_extracted=extraction_time)
                            counter.increment()
                    singer.write_bookmark(
                        state,
                        name,
                        'updated_at',
                        new_bookmark
                    )
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
                schema=schema,
                schema_name=schema_name,
                key_properties=['id'] if schema_name not in CUSTOM_KEY_PROPERTIES else CUSTOM_KEY_PROPERTIES[schema_name],
                valid_replication_keys=['updated_at'],
                replication_method="INCREMENTAL"
            ),
            'key_properties': ['id'] if schema_name not in CUSTOM_KEY_PROPERTIES else CUSTOM_KEY_PROPERTIES[schema_name],
            'replication_method': "INCREMENTAL",
            'replication_key': 'updated_at',

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


CUSTOM_SYNC_FUNC = {
    'milestones': get_all_data_with_projects,
    'team': get_all_data_with_projects,
    'rates': get_all_rate_card_rates,
    'repeating_cards': get_all_data_with_projects,
    'sprints': get_all_data_with_projects,
    'sub_tasks': get_all_data_with_projects,
    'workflow_columns': get_all_data_with_projects,
}


def do_sync_mode(config, state, catalog):
    logger.info('Starting Sync..')
    session.headers.update({'X-FORECAST-API-KEY': config['API_KEY']})
    
    for catalog_entry in catalog.get_selected_streams(state):
        logger.info(f'{catalog_entry.stream} is selected')
        schema = catalog_entry.schema.to_dict()
        singer.write_schema(catalog_entry.stream, schema,
                            catalog_entry.key_properties)

        # properties who need id from another properties
        if catalog_entry.stream in CUSTOM_SYNC_FUNC:
            sync_func = CUSTOM_SYNC_FUNC[catalog_entry.stream]
            state = sync_func(
                catalog_entry.stream,
                schema,
                state,
                url=API_URL,
                mdata=catalog_entry.metadata
            )
        else:
            state = get_all_data(
                catalog_entry.stream,
                schema,
                state,
                url=API_URL,
                mdata=catalog_entry.metadata
            )

        singer.write_state(state)

    logger.info('Finished Sync..')


def do_discover():
    catalog = get_catalog()
    print(json.dumps(catalog, indent=2))


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        do_discover()
    elif args.catalog:
        do_sync_mode(args.config, args.state, args.catalog)


if __name__ == '__main__':
    main()

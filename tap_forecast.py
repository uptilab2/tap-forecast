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
    'expense_items': ['id', 'project_id'],
    'sprints': ['id', 'project_id'],
    'sub_tasks': ['id', 'project_id'],
    'workflow_columns': ['id', 'project_id']
}

# in order to have complete data, we are not using state
FULL_DATA_STREAM = ['roles', 'clients', 'persons', 'rate_cards']

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

def get_projects(schema,
                state,
                url,
                start_date,
                replication_key,
                replication_method,
                sync=True,
                mdata=None):
    response = request_get(url+'projects')
    if response:
        records = response.json()
        projects = []

        bookmark = singer.get_bookmark(state, 'projects', replication_key)
        if bookmark is None:
            bookmark = start_date
        new_bookmark = bookmark

        with metrics.record_counter('projects') as counter:
            extraction_time = singer.utils.now()
            for record in records:
                with singer.Transformer() as transformer:
                    rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                    new_bookmark = max(new_bookmark, rec[replication_key])
                    if (replication_method == 'INCREMENTAL' and rec.get(replication_key) > bookmark) or \
                    (replication_method == 'FULL_TABLE' and rec.get(replication_key) > start_date):
                        projects.append(rec)
                        if sync:
                            singer.write_record('projects', rec,
                                                time_extracted=extraction_time)
                            counter.increment()
        if sync:
            singer.write_bookmark(
                state,
                'projects',
                replication_key,
                new_bookmark
            )
            singer.write_state(state)
    return projects


def get_data(name,
            schema,
            state,
            url,
            start_date,
            replication_key,
            replication_method,
            mdata=None,
            sync=True,
            by_pass_date=False):
    response = request_get(url+name)
    if response:
        bookmark = singer.get_bookmark(state, name, replication_key)
        if bookmark is None:
            bookmark = start_date
        new_bookmark = bookmark
        data=[]
        with metrics.record_counter(name) as counter:
            records = response.json()
            extraction_time = singer.utils.now()
            for record in records:
                with singer.Transformer() as transformer:
                    rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                    new_bookmark = max(new_bookmark, rec[replication_key])
                    if (replication_method == 'INCREMENTAL' and rec.get(replication_key) > bookmark) or \
                    (replication_method == 'FULL_TABLE' and rec.get(replication_key) > start_date) or \
                    by_pass_date:
                        # if sync is False just return data
                        if not sync:
                            data.append(rec)
                        else:
                            singer.write_record(name, rec,
                                                time_extracted=extraction_time)
                            counter.increment()
            if sync:
                singer.write_bookmark(
                    state,
                    name,
                    replication_key,
                    new_bookmark
                )

    return state if sync else data


def get_data_with_projects(name, schema, state, url, start_date, replication_key, replication_method, projects_data, mdata=None):
    logger.info(f'get stream {name} with projects id')
    with metrics.record_counter(name) as counter:
        for project in projects_data:
            project_id = project.get('id')
            response = request_get(url+f'projects/{project_id}/{name}')
            if response:
                records = response.json()
                if replication_key:
                    bookmark = singer.get_bookmark(state, name, replication_key)
                    if bookmark is None:
                        bookmark = start_date
                    new_bookmark = bookmark
                # checking if it is not a list, this if for financials stream
                if not isinstance(records, list):
                    records = [records]

                extraction_time = singer.utils.now()
                for record in records:
                    with singer.Transformer() as transformer:
                        record['project_id'] = project_id
                        rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                        if replication_key:
                            new_bookmark = max(new_bookmark, rec[replication_key])
                        singer.write_record(name, rec,
                                            time_extracted=extraction_time)
                        counter.increment()
                if replication_key:
                    singer.write_bookmark(
                        state,
                        name,
                        replication_key,
                        new_bookmark
                    )
    return state


def get_rate_cards_rates(name, schema, state, url, start_date, replication_key, replication_method, sync=False, stream_rate_cards=None, mdata=None):
    with metrics.record_counter(name) as counter:
        for rate_card in get_data('rate_cards',
                                     stream_rate_cards.schema.to_dict(),
                                     state,
                                     url,
                                     start_date,
                                     stream_rate_cards.replication_key,
                                     stream_rate_cards.replication_method,
                                     mdata=stream_rate_cards.metadata,
                                     sync=False,
                                     by_pass_date=True):
            rate_card_id = rate_card.get('id')
            response = request_get(url+f'rate_cards/{rate_card_id}/{name}')
            if response:
                bookmark = singer.get_bookmark(state, name, replication_key)
                if bookmark is None:
                    bookmark = start_date
                new_bookmark = bookmark

                records = response.json()
                extraction_time = singer.utils.now()
                for record in records:
                    with singer.Transformer() as transformer:
                        record['rate_card_id'] = rate_card_id
                        rec = transformer.transform(record, schema, metadata=metadata.to_map(mdata))
                        new_bookmark = max(new_bookmark, rec[replication_key])
                        singer.write_record(name, rec,
                                            time_extracted=extraction_time)
                        counter.increment()
                    singer.write_bookmark(
                        state,
                        name,
                        replication_key,
                        new_bookmark
                    )
    return state


def get_catalog(replication_method):
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
                valid_replication_keys=['updated_at' if not schema_name == 'financials' else ''],
                replication_method=replication_method if replication_method else None
            ),
            'key_properties': ['id'] if schema_name not in CUSTOM_KEY_PROPERTIES else CUSTOM_KEY_PROPERTIES[schema_name],
            'replication_key': 'updated_at' if not schema_name == 'financials' else '',
            'replication_method': replication_method if replication_method else None,
        }
        streams.append(catalog_entry)
    return {'streams': streams}


def load_schemas():
    schemas = {}
    for filename in os.listdir(get_abs_path('tap_forecast')):
        logger.info(f'extracting {filename}..')
        path = get_abs_path('tap_forecast') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

SYNC_WITH_PROJECTS = ['cards', 'milestones', 'team', 'repeating_cards','sprints', 'sub_tasks', 'workflow_columns', 'expense_items', 'financials', 'time_registrations']


def do_sync_mode(config, state, catalog):
    logger.info('Starting Sync..')
    session.headers.update({'X-FORECAST-API-KEY': config['API_KEY']})

    # get projects data and sync it if stream is selected
    projects_stream_entry = catalog.get_stream('projects')
    if projects_stream_entry.is_selected():
        singer.write_schema(projects_stream_entry.stream, projects_stream_entry.schema.to_dict(),
                        projects_stream_entry.key_properties)
    projects = get_projects(
        projects_stream_entry.schema.to_dict(),
        state,
        url=API_URL,
        start_date=config['start_date'],
        replication_key=projects_stream_entry.replication_key,
        replication_method=projects_stream_entry.replication_method,
        sync=projects_stream_entry.is_selected(),
        mdata=projects_stream_entry.metadata
    )

    for catalog_entry in catalog.get_selected_streams(state):
        if catalog_entry.stream == 'projects':
        # already sync above
            continue
        logger.info(f'{catalog_entry.stream} is selected')
        schema = catalog_entry.schema.to_dict()
        singer.write_schema(catalog_entry.stream, schema,
                            catalog_entry.key_properties)


        
        # properties who need id from another properties
        if catalog_entry.stream in SYNC_WITH_PROJECTS:
            state = get_data_with_projects(
                catalog_entry.stream,
                schema,
                state,
                url=API_URL,
                mdata=catalog_entry.metadata,
                start_date=config['start_date'],
                replication_key=catalog_entry.replication_key,
                replication_method=catalog_entry.replication_method,
                projects_data=projects
            )
        elif catalog_entry.stream == 'rates':
            # rates is a sub object of rate_card
            state = get_rate_cards_rates(
                catalog_entry.stream,
                schema,
                state,
                url=API_URL,
                mdata=catalog_entry.metadata,
                start_date=config['start_date'],
                replication_key=catalog_entry.replication_key,
                replication_method=catalog_entry.replication_method,
                stream_rate_cards=catalog.get_stream('rate_cards')
            )
        else:
            state = get_data(
                catalog_entry.stream,
                schema,
                state,
                url=API_URL,
                mdata=catalog_entry.metadata,
                start_date=config['start_date'],
                replication_key=catalog_entry.replication_key,
                replication_method=catalog_entry.replication_method,
                sync=True,
                by_pass_date=bool(catalog_entry.stream in FULL_DATA_STREAM)
            )

        singer.write_state(state)

    logger.info('Finished Sync..')


def do_discover(config):
    replication_method = 'INCREMENTAL'
    if 'rewrite_replication_method' in config:
        replication_method = config['rewrite_replication_method']
    catalog = get_catalog(replication_method)
    print(json.dumps(catalog, indent=2))


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        do_discover(args.config)
    elif args.catalog:
        do_sync_mode(args.config, args.state, args.catalog)


if __name__ == '__main__':
    main()

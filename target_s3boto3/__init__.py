#!/usr/bin/env python3

import argparse
import boto3
import collections
import http.client
import io
import json
import os
import pkg_resources
import re
import singer
import sys
import tempfile
import threading
import urllib
from botocore.exceptions import ClientError
from datetime import datetime
from jsonschema.validators import Draft4Validator


logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}
    out_files = {}

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    logger.info('Connecting to s3 ...')
    s3_client = boto3.client(
        service_name='s3',
        region_name=config.get("region_name"),
        api_version=config.get("api_version"),
        use_ssl=config.get("use_ssl"),
        verify=config.get("verify"),
        endpoint_url=config.get("endpoint_url"),
        aws_access_key_id=config.get("aws_access_key_id"),
        aws_secret_access_key=config.get("aws_secret_access_key"),
        aws_session_token=config.get("aws_session_token"),
        config=config.get("config")
    )

    logger.info('Validating target_bucket_key: {}...'.format(config.get("target_bucket_key")))
    # Remove empty strings and any s3 Prefix defined in the target_location
    bucket_prefix_regex = re.compile(r'.*(?<!:)$')
    target_location = list(filter(bucket_prefix_regex.match, filter(None, config.get("target_bucket_key").split("/"))))
    # Use first element in the target_location as the target_bucket
    target_bucket = target_location[0]
    # Use all elements except the last as the target_key
    target_key = "/".join(target_location[1:])

    logger.info('[{}] Processing input ...'.format(now))
    # Check if the s3 bucket exists
    try:
        s3_client.head_bucket(Bucket=target_bucket)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == '404':
            raise Exception("Bucket {0} does not exist!".format(target_bucket))

    # create temp directory for processing
    with tempfile.TemporaryDirectory() as temp_dir:

        # Loop over lines from stdin
        for line in lines:
            try:
                o = json.loads(line)
            except json.decoder.JSONDecodeError:
                logger.error("Unable to parse:\n{}".format(line))
                raise

            if 'type' not in o:
                raise Exception("Line is missing required key 'type': {}".format(line))
            t = o['type']

            if t == 'RECORD':
                if 'stream' not in o:
                    raise Exception("Line is missing required key 'stream': {}".format(line))
                if o['stream'] not in schemas:
                    raise Exception("A record for stream {}".format(o['stream']) +
                                    " was encountered before a corresponding schema")

                # Validate record
                validators[o['stream']].validate(o['record'])

                #  writing to a file for the stream
                out_files[o['stream']].write(json.dumps(o['record']) + '\n')

                state = None
            elif t == 'STATE':
                logger.debug('Setting state to {}'.format(o['value']))
                state = o['value']
            elif t == 'SCHEMA':
                if 'stream' not in o:
                    raise Exception("Line is missing required key 'stream': {}".format(line))
                stream = o['stream']
                schemas[stream] = o['schema']
                validators[stream] = Draft4Validator(o['schema'])
                if 'key_properties' not in o:
                    raise Exception("key_properties field is required")
                key_properties[stream] = o['key_properties']
                out_files[stream] = open(os.path.join(temp_dir, "{0}-{1}.json".format(stream, now)), 'a')
                out_files[stream + "_schemas"] = open(os.path.join(temp_dir, "{0}_schemas-{1}.json".format(stream, now)), 'a')
                out_files[stream + "_schemas"].write(json.dumps(o) + '\n')
            else:
                raise Exception("Unknown message type {} in message {}"
                                .format(o['type'], o))

        # Close all stream files and move to s3 target location
        for file_iter in out_files.keys():
            out_files[file_iter].close()
            now = datetime.now().strftime('%Y%m%dT%H%M%S')
            try:
                logger.info('[{0}] Moving file ({1}) to s3 location: {2}/{3} ...'.format(now,
                                                                                         out_files[file_iter].name,
                                                                                         target_bucket,
                                                                                         target_key))
                s3_client.upload_file(out_files[file_iter].name,
                                      target_bucket,
                                      target_key + "/" + os.path.basename(out_files[file_iter].name))
            except ClientError as e:
                logger.error(e)

    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-csv').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-s3boto3',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as args_input:
            config = json.load(args_input)
    else:
        config = {}

    # Validate required config settings
    if config.get("aws_access_key_id") is None:
        raise Exception("ERROR: 'aws_access_key_id' MUST be defined in config.")
    if config.get("aws_secret_access_key") is None:
        raise Exception("ERROR: 'aws_secret_access_key' MUST be defined in config.")
    if config.get("target_bucket_key") is None:
        raise Exception("ERROR: 'target_bucket_key' MUST be defined in config.")

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    std_input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, std_input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()

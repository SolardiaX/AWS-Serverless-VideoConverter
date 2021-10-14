# -*- coding: utf-8 -*-

import logging
import traceback
from task import *

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    logger.info("Manual task request: " + json.dumps(event, indent=2))

    msg = event['Records'][0]
    attributes = msg['messageAttributes']

    try:
        taskid = msg['messageId']

        bucket = attributes.get('Bucket', dict()).get('stringValue', None)
        key = attributes.get('Key', dict()).get('stringValue', None)
        template = attributes.get('TemplateName', dict()).get('stringValue', None)
        condition = attributes.get('Filter', dict()).get('stringValue', None)
        force = attributes.get('Force', dict()).get('stringValue', 'False').upper() == 'TRUE'

        if bucket is None:
            raise ValueError('bucket is none')

        if is_task_exists(bucket, key, condition) and not force:
            logger.info('Task already exists, exit!')
            return {'status': 400, 'event': event, 'message': 'task already exists'}

        if template is None:
            template = get_bucket_template_name(bucket)

        task = create_task(taskid, bucket, key, condition, template)
        total = 0

        if not task.key or task.key.endswith('/'):
            client = boto3.client('s3').get_paginator('list_objects_v2')
            if not task.key:
                paginator = client.paginate(Bucket=task.bucket)
            else:
                paginator = client.paginate(Bucket=task.bucket, Prefix=task.key)
            files = paginator.search(condition if condition is not None else 'Contents[]')

            for f in files:
                key = f['Key']
                if key.endswith('/'):
                    continue

                logger.info('Job recieved, source - %s' % get_source(bucket, key))

                if not key.endswith('/'):
                    if force or not is_taskitem_exists(task.bucket, key):
                        create_converter_job(task.taskId, task.bucket, key, task.template_name)
                        total += 1
                    else:
                        logger.info('Job already exists, source - %s' % get_source(bucket, key))
                else:
                    logger.info('Job source is directory, source - %s' % get_source(bucket, key))

        elif not task.key.endswith('/'):
            logger.info('Job recieved, source - %s' % get_source(bucket, key))

            if source_file_exists(task.bucket, task.key) > 0:
                if force or not is_taskitem_exists(task.bucket, key):
                    create_converter_job(task.taskId, task.bucket, task.key, task.template_name)
                    total += 1
                else:
                    logger.info('Job already exists, source - %s' % get_source(bucket, key))
            else:
                logger.info('Job source not exists, source - %s' % get_source(bucket, key))

        set_task_total(task.taskId, total)
        logger.info('Manual task started, total job - %d' % total)
    except Exception as err:
        error = ''.join(traceback.format_exception(None, err, err.__traceback__))

        # TODO log error to dynamodb

        logger.error("Manual task executed with error - " + error)
        return {'status': 400, 'event': event, 'message': error}

    return {"status": 200, "event": event, 'message': None}

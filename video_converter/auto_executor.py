# -*- coding: utf-8 -*-

import urllib.parse
import logging
import traceback
from task import *


logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    logger.info("Received event: " + json.dumps(event, indent=2))

    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

        if not key.endswith('/') and source_file_exists(bucket, key):
            # always create task when invoked by s3 event notification
            template = get_bucket_template_name(bucket)
            task = create_task(uuid.uuid4().hex, bucket, key, None, template)
            create_converter_job(task.taskId, task.bucket, task.key, task.template_name)
            set_task_total(task.taskId, 1)

            logger.info('Task created.')
            return {"status": 200, "event": event, 'message': None}
    except Exception as err:
        error = ''.join(traceback.format_exception(None, err, err.__traceback__))

        logger.error("Manual task execute error: " + error)
        return {'status': 400, 'event': event, 'message': error}

    return {"status": 400, "event": event, 'message': 'Ignored. Key is directory or task already exists.'}

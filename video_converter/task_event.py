# -*- coding: utf-8 -*-

import logging
import math
from task import *

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    logger.info("Received event: " + json.dumps(event, indent=2))

    itemid = event['detail']['jobId']
    status = event['detail']['status']

    taskitem = get_task_item(itemid)
    if taskitem is not None:
        update_taskitem_status(itemid, status)

        if status == 'COMPLETE':
            increase_task_finished_counter(taskitem.taskid)
        elif status == 'ERROR':
            increase_task_error_counter(taskitem.itemid)
        elif status == 'STATUS_UPDATE':
            progress = event['detail']['jobProgress']['jobPercentComplete']
            update_taskitem_process(itemid, math.floor(float(progress)))

        logger.info("Job(%s) status is updated to [%s] " % (itemid, status))
        return {"status": 200, "event": event, 'message': None}
    else:
        logger.info("Job(%s) not exists. " % itemid)

    logger.info("Job(%s) status is unknown - [%s] " % (itemid, status))
    return {"status": 400, "event": event, 'message': 'Unknown status.'}

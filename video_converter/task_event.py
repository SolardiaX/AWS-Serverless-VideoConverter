# -*- coding: utf-8 -*-

import logging
import math
from task import *

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    logger.info("Received event: " + json.dumps(event, indent=2))

    code = 200
    error = None
    progress = None

    itemid = event['detail']['jobId']
    status = event['detail']['status']

    taskitem = get_task_item(itemid)
    if taskitem is not None:
        if status == 'COMPLETE':
            increase_task_finished_counter(taskitem.taskid)
        elif status == 'ERROR':
            error = event['detail']['errorMessage']
            increase_task_error_counter(taskitem.itemid)
        elif status == 'STATUS_UPDATE':
            progress = math.floor(float(event['detail']['jobProgress']['jobPercentComplete']))
            update_taskitem_progress(itemid, progress)

        if status != 'STATUS_UPDATE':
            update_taskitem_status(itemid, status, error)
            update_taskitem_progress(itemid, 100 if status == 'COMPLETE' else '-1')

        logger.info("Job(%s) status is updated to [%s] "
                    % (itemid, str(progress) if status == 'STATUS_UPDATE' else status))
    else:
        code = 400
        error = "Job not exists"

        logger.info("Job(%s) not exists. " % itemid)

    return {"status": code, "event": event, 'message': error}

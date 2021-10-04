# -*- coding: utf-8 -*-

import boto3
import json
import uuid
from datetime import datetime

task_table_name = 'video-converter-tasks'
taskitem_table_name = 'video-converter-task-items'
options_table_name = 'video-converter-options'

mc_client = boto3.client('mediaconvert')
mc_endpoints = mc_client.describe_endpoints()
converter = boto3.client('mediaconvert', endpoint_url=mc_endpoints['Endpoints'][0]['Url'], verify=False)
db = boto3.resource('dynamodb')


class Task:
    def __init__(self):
        self.taskId = None
        self.bucket = None
        self.template_name = None
        self.key = None
        self.filter = None
        self.executedAt = None
        self.total = 0
        self.finished = 0
        self.running = 0
        self.error = 0

    def as_dict(self):
        return {
            'S_TaskId': self.taskId,
            'S_Bucket': self.bucket,
            'S_TemplateName': self.template_name,
            'S_Key': self.key,
            'S_Filter': self.filter,
            'S_ExecutedAt': self.executedAt,
            'N_Total': self.total,
            'N_Finished': self.finished,
            'N_Running': self.running,
            'N_Error': self.error,
        }

    @classmethod
    def from_item(cls, item: dict):
        task = cls()

        task.taskId = item['S_TaskId']
        task.bucket = item['S_Bucket']
        task.template_name = item.get('S_TemplateName', None)
        task.key = item.get('S_Key', None)
        task.filter = item.get('S_Filter', None)
        task.executedAt = item.get('S_ExecutedAt', None)
        task.total = item.get('N_Total', 0)
        task.finished = item.get('N_Finished', 0)
        task.running = item.get('N_Running', 0)
        task.error = item.get('N_Error', 0)

        return task


class TaskItem:
    def __init__(self):
        self.itemid = None
        self.source = None
        self.taskid = None
        self.status = None
        self.created_at = None
        self.finished_at = None

    def as_dict(self):
        return {
            'S_ItemId': self.itemid,
            'S_Source': self.source,
            'S_TaskId': self.taskid,
            'S_Status': self.status,
            'S_CreatedAt': self.created_at,
            'S_FinishedAt': self.finished_at,
        }

    @classmethod
    def from_item(cls, item: dict):
        task = cls()

        task.itemid = item['S_ItemId']
        task.source = item['S_Source']
        task.taskid = item['S_TaskId']
        task.status = item.get('S_Status', None)
        task.created_at = item.get('S_CreatedAt', None)
        task.finished_at = item.get('S_FinishedAt', None)

        return task


def get_task(taskid: str) -> Task:
    resp = db.Table(taskitem_table_name).get_item(Key={'S_TaskId': taskid})
    task = Task.from_item(resp['Item']) if resp.get('Item', None) is not None else None

    return task


def create_task(taskid: str, bucket: str, key: str = None, condition: str = None, template: str = None) -> Task:
    task = Task()
    task.taskId = taskid
    task.bucket = bucket
    task.key = key
    task.filter = condition
    task.template_name = template

    db.Table(task_table_name).put_item(
        Item=task.as_dict(),
        ReturnValues='NONE'
    )

    return task


def get_task_item(itemid: str) -> TaskItem:
    resp = db.Table(taskitem_table_name).get_item(Key={'S_ItemId': itemid})
    item = TaskItem.from_item(resp['Item']) if resp.get('Item', None) is not None else None

    return item


def set_task_total(taskid: str, total: int):
    db.Table(task_table_name).update_item(
        Key={'S_TaskId': taskid},
        UpdateExpression='SET N_Total = :total, S_ExecutedAt = :at',
        ExpressionAttributeValues={
            ':total': total,
            ':at': datetime.now().strftime('%Y-%m-%d %H:%M:%S%z')
        },
        ReturnValues='NONE'
    )


def increase_task_running_counter(taskid: str):
    db.Table(task_table_name).update_item(
        Key={'S_TaskId': taskid},
        UpdateExpression='SET N_Running = N_Running + :one',
        ExpressionAttributeValues={':one': 1},
        ReturnValues='NONE'
    )


def increase_task_finished_counter(taskid: str):
    db.Table(task_table_name).update_item(
        Key={'S_TaskId': taskid},
        UpdateExpression='SET N_Finished = N_Finished + :one, N_Running = N_Running - :one',
        ExpressionAttributeValues={':one': 1},
        ReturnValues='NONE'
    )


def increase_task_error_counter(taskid: str):
    db.Table(task_table_name).update_item(
        Key={'S_TaskId': taskid},
        UpdateExpression='SET N_Error = N_Error + :one, N_Running = N_Running - :one',
        ExpressionAttributeValues={':one': 1},
        ReturnValues='NONE'
    )


def update_taskitem_status(itemid: str, status: str):
    db.Table(taskitem_table_name).update_item(
        Key={'S_ItemId': itemid},
        UpdateExpression='SET S_Status = :status, S_FinishedAt = :at',
        ExpressionAttributeValues={
            ':status': status,
            ':at': datetime.now().strftime('%Y-%m-%d %H:%M:%S%z')
        },
        ReturnValues='NONE'
    )


def update_taskitem_process(itemid: str, process: int):
    db.Table(taskitem_table_name).update_item(
        Key={'S_ItemId': itemid},
        UpdateExpression='SET N_Process = :process',
        ExpressionAttributeValues={':status': process},
        ReturnValues='NONE'
    )


def update_task_total(taskid: str, total: int):
    db.Table(task_table_name).update_item(
        Key={'S_TaskId': taskid},
        UpdateExpression='SET N_Total = :total',
        ExpressionAttributeValues={':total': total},
        ReturnValues='NONE'
    )


def is_taskitem_exists(bucket: str, key: str) -> bool:
    item = db.Table(taskitem_table_name).query(
        KeyConditionExpression='S_Source = :source',
        ExpressionAttributeValues={':source': get_source(bucket, key)},
        IndexName='SourceIndex',
        Select='COUNT'
    )

    return item.get('Count', 0) > 0


def is_task_exists(bucket: str, key: str, condition: str) -> bool:
    item = db.Table(task_table_name).query(
        KeyConditionExpression='S_Bucket = :bucket',
        IndexName='BucketIndex',
        FilterExpression='S_Key = :key AND S_Filter = :filter',
        ExpressionAttributeValues={
            ':bucket': bucket,
            ':key': key,
            ':filter': condition
        },
        Select='COUNT'
    )

    return item.get('Count', 0) > 0


def create_converter_job(taskid: str, bucket: str, key: str, template_name: str):
    source = get_source(bucket, key)

    with open('./task_params.json', 'r') as f:
        params = json.load(f)
        params['Role'] = _get_options('MediaConvertJobRole')
        params['JobTemplate'] = template_name
        params['Settings']['Inputs'][0]['FileInput'] = source

    template_params = converter.get_job_template(Name=template_name)
    dest = template_params['JobTemplate']['Settings']['OutputGroups'][0]['OutputGroupSettings'].get('Destination', None)

    if dest is None:
        dest = _get_options('%s-OutputBucket' % bucket)
        if dest is None:
            dest = _get_options('default-OutputBucket')

        sub = ''
        if '/' in key:
            sub = key[0:key.rindex('/') + 1]

        params['Settings']['OutputGroups'] = [{
            'OutputGroupSettings': {
                'Type': 'FILE_GROUP_SETTINGS',
                'FileGroupSettings': {
                    'Destination': 's3://%s/%s' % (dest, sub)
                },
            }
        }]

    increase_task_running_counter(taskid)

    # noinspection PyBroadException
    try:
        resp = converter.create_job(**params)

        status = 'RUNNING'
        itemid = resp['Job']['Id']
        created_at = resp['Job']['CreatedAt'].strftime('%Y-%m-%d %H:%M:%S%z')
        finished_at = None
    except:
        increase_task_error_counter(taskid)

        status = 'ERROR'
        itemid = uuid.uuid4().hex
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S%z')
        finished_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S%z')

    db.Table(taskitem_table_name).put_item(
        Item={
            'S_ItemId': itemid,
            'S_Source': source,
            'S_Target': get_source(dest, key),
            'S_TaskId': taskid,
            'S_Status': status,
            'S_CreatedAt': created_at,
            'S_FinishedAt': finished_at,
        },
        ReturnValues='NONE')


def source_file_exists(bucket: str, key: str) -> bool:
    if bucket is None or len(bucket.strip()) == 0:
        return False
    if key is None or len(key.strip()) == 0:
        return False

    # noinspection PyBroadException
    try:
        f = boto3.resource('s3').Object(bucket, key)
        if f.content_length == 0:
            return False
    except:
        return False

    return True


def get_bucket_template_name(bucket: str):
    template = _get_options('%s-JobTemplate' % bucket)
    if template is None:
        template = _get_options('default-JobTemplate')

    return template


def get_source(bucket: str, key: str) -> str:
    return "s3://%s/%s" % (bucket, key)


_options = dict()  # global option store


def _get_options(key: str) -> any:
    opt = _options.get(key, None)
    if opt is not None:
        return opt

    item = db.Table(options_table_name).get_item(
        Key={'S_Key': key}
    )

    value = item.get('Item', dict()).get('S_Value', None)
    _options[key] = value
    return value

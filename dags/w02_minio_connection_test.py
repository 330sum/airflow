from airflow.hooks.base import BaseHook

import boto3
import json


def get_s3_client():
    conn = BaseHook.get_connection(conn_id="minio_test")
    extra_config = json.loads(conn.extra)

    s3_client = boto3.client(
        "s3",
        endpoint_url=extra_config["endpoint_url"],
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
    )

    return s3_client


def get_storage_ops():
    conn = BaseHook.get_connection(conn_id="minio_test")
    extra_config = json.loads(conn.extra)

    storage_opts = {'key': conn.login,
                    'secret': conn.password,
                    'client_kwargs': {'endpoint_url': extra_config["endpoint_url"]}}

    return storage_opts

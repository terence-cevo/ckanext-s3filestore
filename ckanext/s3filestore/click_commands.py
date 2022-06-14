
import os
import click
from boto3.s3.transfer import TransferConfig

from sqlalchemy import create_engine
from sqlalchemy.sql import text
from ckantoolkit import config
from ckanext.s3filestore.uploader import BaseS3Uploader
import magic
import sys
import threading

@click.command(u's3-upload',
               short_help=u'Uploads all resources '
                          u'from "ckan.storage_path"'
                          u' to the configured s3 bucket')
def upload_resources():
    storage_path = config.get('ckan.storage_path',
                              '/var/lib/ckan/default/resources')
    sqlalchemy_url = config.get('sqlalchemy.url',
                                'postgresql://user:pass@localhost/db')
    bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')
    acl = config.get('ckanext.s3filestore.acl', 'public-read')
    resource_ids_and_paths = {}
    mime = magic.Magic(mime=True)
    for root, dirs, files in os.walk(storage_path):
        if files:
            resource_id = root.split('/')[-2] + root.split('/')[-1] + files[0]
            resource_ids_and_paths[resource_id] = os.path.join(root, files[0])

    click.secho(
        'Found {0} resource files in '
        'the file system'.format(len(resource_ids_and_paths.keys())),
        fg=u'green',
        bold=True)

    engine = create_engine(sqlalchemy_url)
    connection = engine.connect()

    resource_ids_and_names = {}

    try:
        for resource_id, file_path in resource_ids_and_paths.items():
            resource = connection.execute(text('''
                   SELECT id, url, url_type
                   FROM resource
                   WHERE id = :id
               '''), id=resource_id)
            if resource.rowcount:
                _id, url, _type = resource.first()
                if _type == 'upload' and url:
                    file_name = url.split('/')[-1] if '/' in url else url
                    resource_ids_and_names[_id] = file_name.lower()
    finally:
        connection.close()
        engine.dispose()

    click.secho('{0} resources matched on the database'.format(
        len(resource_ids_and_names.keys())),
        fg=u'green',
        bold=True)

    uploader = BaseS3Uploader()
    s3_connection = uploader.get_s3_resource()
    click.secho('Using the new transfer config parameters')
    transfer_config = TransferConfig(multipart_threshold=1024 * 25,
                                     max_concurrency=10,
                                     multipart_chunksize=1024 * 25,
                                     use_threads=True)

    uploaded_resources = []
    for resource_id, file_name in resource_ids_and_names.items():
        total = 0
        uploaded = 0
        key = 'resources/{resource_id}/{file_name}'.format(
            resource_id=resource_id, file_name=file_name)
        click.secho('Processing file path : {0}'.format(resource_ids_and_paths[resource_id]), fg=u'blue', bold=True)
        buffered_bytes = open(resource_ids_and_paths[resource_id], u'rb')
        click.secho('Mimetype for file : {0} '.format(mime.from_file(resource_ids_and_paths[resource_id])))
        total = os.stat(resource_ids_and_paths[resource_id]).st_size
        click.secho('Total FileSize : {0}'.format(total), fg=u'yellow', bold=True)
        s3_connection.Object(bucket_name, key) \
            .upload_fileobj(buffered_bytes,
                            ExtraArgs={
                                'StorageClass': 'INTELLIGENT_TIERING',
                                'ACL': acl,
                                'ContentType': mime.from_file(resource_ids_and_paths[resource_id]) or 'text/plain'
                            },
                            Config=transfer_config,
                            Callback=ProcessPercentage(resource_ids_and_paths[resource_id]))
        uploaded_resources.append(resource_id)
        click.secho(
            'Uploaded resource {0} ({1}) to S3'.format(resource_id,
                                                       file_name),
            fg=u'green',
            bold=True)

    click.secho(
        'Done, uploaded {0} resources to S3'.format(
            len(uploaded_resources)),
        fg=u'green',
        bold=True)


class ProcessPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)\r" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

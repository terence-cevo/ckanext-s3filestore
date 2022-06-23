import logging
import ckan.lib.helpers as h
import ckan.model as model
import ckan.plugins.toolkit as toolkit
import ckan.lib.munge as munge
from ckantoolkit import config
from ckan.lib.uploader import get_resource_uploader

from ckanext.s3filestore.uploader import S3ResourceUploader

log = logging.getLogger(__name__)


def initiate_multipart(context, data_dict):
    """Initiate new Multipart Upload.

    :param context:
    :param data_dict: dict with required keys:
        id: resource's id
        name: filename
        size: filesize

    :returns: ObjectWith SignedUrls info
    :rtype: dict

    """
    log.debug('Called from inside s3filestore_initiate_multipart')
    h.check_access("s3filestore_initiate_multipart", data_dict)
    # File parts of 5GB to save generating many presigned urlsG
    # S3 min file size is 5MB and max 5GB
    presigned_urls = []
    upload_id=''
    file_chunk_size = config.get('ckanext.s3filestore.file.chunk_size_in_bytes', '5368709120')
    id, name, size, file_type = toolkit.get_or_bust(data_dict, ["id", "name", "size", "type"])
    log.debug('File Type : {0}'.format(file_type))
    user_obj = model.User.get(context["user"])
    user_id = user_obj.id if user_obj else None
    log.debug('The upload file is of size : {0}'.format(size))
    uploader = get_resource_uploader({"multipart_name": name, "id": id})
    if not isinstance(uploader, S3ResourceUploader):
        raise toolkit.ValidationError(
            {
                "uploader": [
                    "Must be S3ResourceUploader or its subclass, not"
                    f" {type(uploader)}"
                ]
            }
        )
    chunk_count = size/int(file_chunk_size)
    part_number = 0
    log.debug('Generate Presigned url for : {0}'.format(chunk_count))
    if chunk_count > 1:
        while chunk_count > 1:
            part_number += 1
            chunk_count -= 1
            key = 'resources/{0}/{1}'.format(id, name)
            presigned_urls.append(uploader.create_multipart_upload_part(key, part_number).get('url', None))
            upload_id=uploader.create_multipart_upload_part(key, part_number).get('upload_id', None)
    else:
        log.debug('File chunk size is not bigger than : {0}'.format(config.get('ckanext.s3filestore.file.chunk_size_in_bytes', "4294967296")))
        key = 'resources/{0}/'.format(id) + munge.munge_filename('{0}'.format(name))
        log.debug('Creating sigv4 with key: {0}'.format(key))
        presigned_urls.append(uploader.get_signed_url_to_key_for_upload('put_object', key))

    return {"id": id, "name": name, "signed_urls": presigned_urls, "upload_id":upload_id}


def upload_multipart(context, data_dict):
    h.check_access("s3filestore_upload_multipart", data_dict)
    upload_id, part_number, part_content = toolkit.get_or_bust(
        data_dict, ["uploadId", "partNumber", "upload"]
    )

    upload = model.Session.query(MultipartUpload).get(upload_id)
    uploader = get_resource_uploader({"id": upload.resource_id})

    data = _get_underlying_file(part_content).read()
    resp = uploader.driver.connection.request(
        _get_object_url(uploader, upload.name),
        params={"uploadId": upload_id, "partNumber": part_number},
        method="PUT",
        headers={"Content-Length": len(data)},
        data=data,
    )
    if resp.status != 200:
        raise toolkit.ValidationError("Upload failed: part %s" % part_number)

    _save_part_info(part_number, resp.headers["etag"], upload)
    return {"partNumber": part_number, "ETag": resp.headers["etag"]}


def finish_multipart(context, data_dict):
    log.debug('Called from inside s3filestore_finish_multipart')
    h.check_access("s3filestore_finish_multipart", data_dict)
    parts = data_dict['parts']
    uploadId, name, id, s3_upload_id = toolkit.get_or_bust(data_dict, ["uploadId", "name", "id", "S3upload_id"])
    log.debug('Parts : {0}'.format(parts))
    uploader = get_resource_uploader({"multipart_name": name, "id": id})
    if not isinstance(uploader, S3ResourceUploader):
        raise toolkit.ValidationError(
            {
                "uploader": [
                    "Must be S3ResourceUploader or its subclass, not"
                    f" {type(uploader)}"
                ]
            }
        )
    key = 'resources/{0}/{1}'.format(id, name)
    complete = uploader.complete_multipart_upload(key, parts, s3_upload_id)

    return {"commited": True}


from ckanext.s3filestore.logic.action import multipart


def get_actions():
    return {
        "s3filestore_initiate_multipart": multipart.initiate_multipart,
        "s3filestore_upload_multipart": multipart.upload_multipart,
        "s3filestore_finish_multipart": multipart.finish_multipart
    }

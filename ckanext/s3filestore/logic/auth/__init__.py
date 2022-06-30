from ckanext.s3filestore.logic.auth import multipart


def get_auth_functions():
    return {
        "s3filestore_initiate_multipart": multipart.initiate_multipart,
        "s3filestore_upload_multipart": multipart.upload_multipart,
        "s3filestore_finish_multipart": multipart.finish_multipart,
        "s3filestore_keep_alive_multipart": multipart.keep_alive_multipart,
        "s3filestore_abort_multipart": multipart.abort_multipart,
        "s3filestore_check_multipart": multipart.check_multipart,
        "s3filestore_clean_multipart": multipart.clean_multipart,
    }

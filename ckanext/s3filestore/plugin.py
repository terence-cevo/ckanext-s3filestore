# encoding: utf-8
import ckan.plugins as plugins
import ckantoolkit as toolkit

import ckanext.s3filestore.uploader
from ckanext.s3filestore.logic.action import get_actions
from ckanext.s3filestore.logic.auth import get_auth_functions
from ckanext.s3filestore.views import resource, uploads
from ckanext.s3filestore.click_commands import upload_resources
from ckanext.s3filestore import helpers
import ckan.lib.munge as munge


class S3FileStorePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IClick)
    plugins.implements(plugins.IResourceController, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        # We need to register the following templates dir
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_resource("fantastic/scripts", "s3filestore-js")

    # ITemplateHelpers
    def get_helpers(self):
        return dict(
            s3filestore_max_file_upload_size_in_bytes=helpers.max_file_upload_size,
            s3filestore_max_file_part_size_in_bytes=helpers.max_file_part_size,
        )

    # IConfigurable

    def configure(self, config):
        # Certain config options must exists for the plugin to work. Raise an
        # exception if they're missing.
        missing_config = "{0} is not configured. Please amend your .ini file."
        config_options = (
            'ckanext.s3filestore.aws_bucket_name',
            'ckanext.s3filestore.region_name',
            'ckanext.s3filestore.signature_version'
        )

        if not config.get('ckanext.s3filestore.aws_use_ami_role'):
            config_options += ('ckanext.s3filestore.aws_access_key_id',
                               'ckanext.s3filestore.aws_secret_access_key')

        for option in config_options:
            if not config.get(option, None):
                raise RuntimeError(missing_config.format(option))

        # Check that options actually work, if not exceptions will be raised
        if toolkit.asbool(
                config.get('ckanext.s3filestore.check_access_on_startup',
                           True)):
            ckanext.s3filestore.uploader.BaseS3Uploader().get_s3_bucket(
                config.get('ckanext.s3filestore.aws_bucket_name'))

    # IActions

    def get_actions(self):
        return get_actions()

    # IAuthFunctions

    def get_auth_functions(self):
        return get_auth_functions()

    # IUploader

    def get_resource_uploader(self, data_dict):
        '''Return an uploader object used to upload resource files.'''
        return ckanext.s3filestore.uploader.S3ResourceUploader(data_dict)

    def get_uploader(self, upload_to, old_filename=None):
        '''Return an uploader object used to upload general files.'''
        return ckanext.s3filestore.uploader.S3Uploader(upload_to,
                                                       old_filename)

    # IBlueprint

    def get_blueprint(self):
        blueprints = resource.get_blueprints() +\
                     uploads.get_blueprints()
        return blueprints

    # IClick

    def get_commands(self):
        return [upload_resources]

    # IResourceController
    def before_create(self, context, resource):
        filename = munge.munge_filename(resource.get('name'))
        resource['name'] = filename

    def before_delete(self, context, resource, resources):
        # let's get all info about our resource. It somewhere in resources
        # but if there is some possibility that it isn't(magic?) we have
        # `else` clause
        for res in resources:
            if res["id"] == resource["id"]:
                break
        else:
            return
        # just ignore simple links
        if res["url_type"] != "upload":
            return

        # we don't want to change original item from resources, just in case
        # someone will use it in another `before_delete`. So, let's copy it
        # and add `clear_upload` flag
        res_dict = dict(list(res.items()) + [("clear_upload", True)])

        uploader = self.get_resource_uploader(res_dict)

        # and now uploader removes our file.
        uploader.upload(resource["id"])

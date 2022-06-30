#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ckan.plugins.toolkit as tk


# AWS supports generating only upto 1000 pre-signed urls at one time.
def max_file_upload_size():
    return tk.config.get("ckanext.s3filestore.max_file_upload_size_in_bytes", 10737418240)


# AWS supports upto 5GB in a single PUT and this value should not be greater.
def max_file_part_size():
    return tk.config.get("ckanext.s3filestore.max_file_part_size_in_bytes", 4294967296)

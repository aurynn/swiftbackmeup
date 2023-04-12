# Copyright 2016 Yanis Guenane <yguenane@redhat.com>
# Author: Yanis Guenane <yguenane@redhat.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from swiftbackmeup import stores
from swiftbackmeup import exceptions

import os
import re
# import swiftclient
import openstack


class Swift(stores.Store):
    def __init__(self, conf):
        # Switch to relying on the /etc/openstack/clouds.yaml file
        self.connection = openstack.connection.Connection(cloud=conf["os_cloud_name"])

    def delete(self, container, filename):
        try:
            self.connection.object_store.delete_object(filename, container=container)
        except openstack.exceptions.ResourceNotFound:
            raise exceptions.StoreExceptions(
                "An error occured while deleting %s" % filename
            )

    def get(self, container, filename, output_directory):
        try:
            cont = self.connection.object_store.get_container_metadata(container) 
        except openstack.exceptions.ResourceNotFound:
            raise exceptions.StoreExceptions(
                "%s: Container not found in store" % container
            )
        try:
            obj = self.connection.object_store.get_object_metadata(container, filename)
        except openstack.exceptions.ResourceNotFound:
            raise exceptions.StoreExceptions(
                "%s: File not found in store" % filename
            )
                
        
        backup_directory = os.path.dirname("%s/%s" % (output_directory, filename))
        if not os.path.exists(backup_directory):
            os.makedirs(backup_directory)
        
        self.connection.object_store.get_object(
            filename,
            container=container,
            outfile="%s/%s" % (output_directory, filename),
        )

    def list(
        self,
        item,
        item_type,
        container,
        filename=None,
        pseudo_folder=None,
        filename_prefix=None,
        filename_suffix=None,
    ):
        
        if pseudo_folder:
            if filename:
                backup_name_pattern = "%s/%s" % (pseudo_folder, filename)
            else:
                backup_name_pattern = pseudo_folder or ""
                if filename_prefix and filename_suffix:
                    backup_name_pattern += "/%s.*%s" % (
                        filename_prefix,
                        filename_suffix,
                    )
                elif filename_prefix and not filename_suffix:
                    backup_name_pattern += "/%s.*" % filename_prefix
                elif not filename_prefix and filename_suffix:
                    backup_name_pattern += "/.*%s" % filename_suffix

        else:
            if filename:
                backup_name_pattern = filename
            else:
                backup_name_pattern = ""
                if filename_prefix and filename_suffix:
                    backup_name_pattern += "%s.*%s" % (filename_prefix, filename_suffix)
                elif filename_prefix and not filename_suffix:
                    backup_name_pattern += "%s.*" % filename_prefix
                elif not filename_prefix and filename_suffix:
                    backup_name_pattern += ".*%s" % filename_suffix

        data = self.connection.object_store.objects(container=container)

        result = []
        for backup in data:
            m = re.search(backup_name_pattern, backup["name"])
            if m:
                result.append(
                    {
                        "item": item,
                        "type": item_type,
                        "filename": m.group(0),
                        "last-modified": backup["last_modified"],
                    }
                )
        return result

    def upload(self, container, file_path, pseudo_folder=None, create_container=True):
        try:
            self.connection.object_store.get_container_metadata(container)
        except openstack.exceptions.ResourceNotFound as exc:
            if create_container:
                self.connection.create_container(name=container)

        if pseudo_folder:
            swift_path = "%s/%s" % (pseudo_folder, os.path.basename(file_path))
        else:
            swift_path = os.path.basename(file_path)
        
        self.connection.object_store.upload_object(
            container=container,
            filename=file_path,
            name=swift_path
        )
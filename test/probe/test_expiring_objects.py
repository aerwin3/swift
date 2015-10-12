#!/usr/bin/python -u
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import unittest
import time
import pickle

from os.path import dirname
from uuid import uuid4

from swift.common.manager import Manager
from swift.common.utils import Timestamp, hash_path, readconf
from swift.obj.diskfile import get_data_dir

from test.probe.common import ReplProbeTest

from swiftclient import client


RETRIES = 5


def get_files(obj_dir):
    files = []
    # We might need to try a few times if a request hasn't yet settled. For
    # instance, a PUT can return success when just 2 of 3 nodes has completed.
    for attempt in range(RETRIES + 1):
        try:
            files = sorted(os.listdir(obj_dir), reverse=True)
            break
        except Exception:
            if attempt < RETRIES:
                time.sleep(1)
            else:
                raise
    return files


class TestExpiringObjects(ReplProbeTest):

    def setUp(self):
        self.cont_replicator = Manager(['container-replicator'])
        self.cont_updater = Manager(['container-updater'])
        self.obj_replicator = Manager(['object-replicator'])
        self.obj_auditor = Manager(['object-auditor'])
        super(TestExpiringObjects, self).setUp()

    def test_expire_objects_and_replicate(self):
        # Create container1
        container1 = 'container-%s' % uuid4()
        cpart, cnodes = self.container_ring.get_nodes(self.account, container1)
        client.put_container(self.url, self.token, container1)
        obj1 = 'obj1'
        ts = Timestamp(time.time())
        client.put_object(self.url,
                          self.token,
                          container1,
                          obj1, 'test',
                          4,
                          headers={'X-Timestamp': ts.internal,
                                   'X-Delete-At': int(ts) + 1})
        client.put_object(self.url,
                          self.token,
                          container1,
                          'object2', 'test',
                          4,
                          headers={'X-Timestamp': ts.internal})
        self.obj_replicator.once()
        # Update account stats with new objects

        self.cont_updater.once()
        before_acct_stats = client.head_account(self.url, self.token)
        while not int(before_acct_stats['x-account-object-count']) == 2:
            time.sleep(1)
            before_acct_stats = client.head_account(self.url, self.token)

        cont_stats = client.head_container(self.url,
                                           self.token,
                                           container1)
        time.sleep(2)
        self.cont_replicator.once()
        changed = False
        while not changed:
            time.sleep(1)
            new_cont_stats = client.head_container(self.url,
                                                   self.token,
                                                   container1)
            if not new_cont_stats['x-container-object-count'] == \
                    cont_stats['x-container-object-count']:
                cont_stats = new_cont_stats
                changed = True
        self.assertEqual(int(cont_stats['x-container-object-count']), 1)
        self.assertEqual(int(cont_stats['x-container-bytes-used']), 4)

        # Check the account stats
        self.cont_updater.once()
        after_acct_stats = client.head_account(self.url, self.token)
        self.assertEqual(int(after_acct_stats['x-account-object-count']), 1)

        # Check Auditor drops ts when expired object is found
        self.obj_auditor.once()
        obj_hash = hash_path(self.account, container1, obj1)
        opart, onodes = self.object_ring.get_nodes(self.account,
                                                   container1,
                                                   obj1)

        for node in onodes:
            device = node['device']
            node_id = (node['port'] - 6000) / 10
            obj_server_conf = readconf(self.configs['object-server'][node_id])
            devices = obj_server_conf['app:object-server']['devices']
            obj_dir = '%s/%s/%s/%s/%s/%s/' % (devices, device,
                                              get_data_dir(self.policy),
                                              opart, obj_hash[-3:], obj_hash)
            obj_files = get_files(obj_dir)
            for name in obj_files:
                self.assertFalse(name.endswith('.data'))
            suffix_path = dirname(dirname(dirname(obj_dir)))
            with open(suffix_path + '/hashes.pkl', 'rb') as fd:
                hashes = pickle.load(fd)
            for suffix in hashes:
                self.assertIsNone(hashes[suffix])

if __name__ == "__main__":
    unittest.main()

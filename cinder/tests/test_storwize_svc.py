# Copyright 2013 IBM Corp.
# Copyright 2012 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
# Authors:
#   Ronen Kat <ronenkat@il.ibm.com>
#   Avishay Traeger <avishay@il.ibm.com>

"""
Tests for the IBM Storwize family and SVC volume driver.
"""


import random
import re
import socket

from cinder import context
from cinder import exception
from cinder.openstack.common import excutils
from cinder.openstack.common import log as logging
from cinder.openstack.common import processutils
from cinder import test
from cinder import units
from cinder import utils
from cinder.volume import configuration as conf
from cinder.volume.drivers import storwize_svc
from cinder.volume import volume_types

from eventlet import greenthread

LOG = logging.getLogger(__name__)


class StorwizeSVCFakeDB:
    def __init__(self):
        self.volume = None

    def volume_get(self, context, vol_id):
        return self.volume

    def volume_set(self, vol):
        self.volume = vol


class StorwizeSVCManagementSimulator:
    def __init__(self, pool_name):
        self._flags = {'storwize_svc_volpool_name': pool_name}
        self._volumes_list = {}
        self._hosts_list = {}
        self._mappings_list = {}
        self._fcmappings_list = {}
        self._other_pools = {'openstack2': {}, 'openstack3': {}}
        self._next_cmd_error = {
            'lsportip': '',
            'lsfabric': '',
            'lsiscsiauth': '',
            'lsnodecanister': '',
            'mkvdisk': '',
            'lsvdisk': '',
            'lsfcmap': '',
            'prestartfcmap': '',
            'startfcmap': '',
            'rmfcmap': '',
            'lslicense': '',
        }
        self._errors = {
            'CMMVC5701E': ('', 'CMMVC5701E No object ID was specified.'),
            'CMMVC6035E': ('', 'CMMVC6035E The action failed as the '
                               'object already exists.'),
            'CMMVC5753E': ('', 'CMMVC5753E The specified object does not '
                               'exist or is not a suitable candidate.'),
            'CMMVC5707E': ('', 'CMMVC5707E Required parameters are missing.'),
            'CMMVC6581E': ('', 'CMMVC6581E The command has failed because '
                               'the maximum number of allowed iSCSI '
                               'qualified names (IQNs) has been reached, '
                               'or the IQN is already assigned or is not '
                               'valid.'),
            'CMMVC5754E': ('', 'CMMVC5754E The specified object does not '
                               'exist, or the name supplied does not meet '
                               'the naming rules.'),
            'CMMVC6071E': ('', 'CMMVC6071E The VDisk-to-host mapping was '
                               'not created because the VDisk is already '
                               'mapped to a host.'),
            'CMMVC5879E': ('', 'CMMVC5879E The VDisk-to-host mapping was '
                               'not created because a VDisk is already '
                               'mapped to this host with this SCSI LUN.'),
            'CMMVC5840E': ('', 'CMMVC5840E The virtual disk (VDisk) was '
                               'not deleted because it is mapped to a '
                               'host or because it is part of a FlashCopy '
                               'or Remote Copy mapping, or is involved in '
                               'an image mode migrate.'),
            'CMMVC6527E': ('', 'CMMVC6527E The name that you have entered '
                               'is not valid. The name can contain letters, '
                               'numbers, spaces, periods, dashes, and '
                               'underscores. The name must begin with a '
                               'letter or an underscore. The name must not '
                               'begin or end with a space.'),
            'CMMVC5871E': ('', 'CMMVC5871E The action failed because one or '
                               'more of the configured port names is in a '
                               'mapping.'),
            'CMMVC5924E': ('', 'CMMVC5924E The FlashCopy mapping was not '
                               'created because the source and target '
                               'virtual disks (VDisks) are different sizes.'),
            'CMMVC6303E': ('', 'CMMVC6303E The create failed because the '
                               'source and target VDisks are the same.'),
            'CMMVC7050E': ('', 'CMMVC7050E The command failed because at '
                               'least one node in the I/O group does not '
                               'support compressed VDisks.'),
            'CMMVC6430E': ('', 'CMMVC6430E The command failed because the '
                               'target and source managed disk groups must '
                               'be different.'),
            'CMMVC6353E': ('', 'CMMVC6353E The command failed because the '
                               'copy specified does not exist.'),
            'CMMVC6446E': ('', 'The command failed because the managed disk '
                               'groups have different extent sizes.'),
            # Catch-all for invalid state transitions:
            'CMMVC5903E': ('', 'CMMVC5903E The FlashCopy mapping was not '
                               'changed because the mapping or consistency '
                               'group is another state.'),
        }
        self._transitions = {'begin': {'make': 'idle_or_copied'},
                             'idle_or_copied': {'prepare': 'preparing',
                                                'delete': 'end',
                                                'delete_force': 'end'},
                             'preparing': {'flush_failed': 'stopped',
                                           'wait': 'prepared'},
                             'end': None,
                             'stopped': {'prepare': 'preparing',
                                         'delete_force': 'end'},
                             'prepared': {'stop': 'stopped',
                                          'start': 'copying'},
                             'copying': {'wait': 'idle_or_copied',
                                         'stop': 'stopping'},
                             # Assume the worst case where stopping->stopped
                             # rather than stopping idle_or_copied
                             'stopping': {'wait': 'stopped'},
                             }

    def _state_transition(self, function, fcmap):
        if (function == 'wait' and
                'wait' not in self._transitions[fcmap['status']]):
            return ('', '')

        if fcmap['status'] == 'copying' and function == 'wait':
            if fcmap['copyrate'] != '0':
                if fcmap['progress'] == '0':
                    fcmap['progress'] = '50'
                else:
                    fcmap['progress'] = '100'
                    fcmap['status'] = 'idle_or_copied'
            return ('', '')
        else:
            try:
                curr_state = fcmap['status']
                fcmap['status'] = self._transitions[curr_state][function]
                return ('', '')
            except Exception:
                return self._errors['CMMVC5903E']

    # Find an unused ID
    def _find_unused_id(self, d):
        ids = []
        for k, v in d.iteritems():
            ids.append(int(v['id']))
        ids.sort()
        for index, n in enumerate(ids):
            if n > index:
                return str(index)
        return str(len(ids))

    # Check if name is valid
    def _is_invalid_name(self, name):
        if re.match("^[a-zA-Z_][\w ._-]*$", name):
            return False
        return True

    # Convert argument string to dictionary
    def _cmd_to_dict(self, arg_list):
        no_param_args = [
            'autodelete',
            'autoexpand',
            'bytes',
            'compressed',
            'force',
            'nohdr',
        ]
        one_param_args = [
            'chapsecret',
            'cleanrate',
            'copy',
            'copyrate',
            'delim',
            'easytier',
            'filtervalue',
            'grainsize',
            'hbawwpn',
            'host',
            'iogrp',
            'iscsiname',
            'mdiskgrp',
            'name',
            'rsize',
            'scsi',
            'size',
            'source',
            'target',
            'unit',
            'vdisk',
            'warning',
            'wwpn',
        ]

        # Handle the special case of lsnode which is a two-word command
        # Use the one word version of the command internally
        if arg_list[0] in ('svcinfo', 'svctask'):
            if arg_list[1] == 'lsnode':
                if len(arg_list) > 4:  # e.g. svcinfo lsnode -delim ! <node id>
                    ret = {'cmd': 'lsnode', 'node_id': arg_list[-1]}
                else:
                    ret = {'cmd': 'lsnodecanister'}
            else:
                ret = {'cmd': arg_list[1]}
            arg_list.pop(0)
        else:
            ret = {'cmd': arg_list[0]}

        skip = False
        for i in range(1, len(arg_list)):
            if skip:
                skip = False
                continue
            if arg_list[i][0] == '-':
                if arg_list[i][1:] in no_param_args:
                    ret[arg_list[i][1:]] = True
                elif arg_list[i][1:] in one_param_args:
                    ret[arg_list[i][1:]] = arg_list[i + 1]
                    skip = True
                else:
                    raise exception.InvalidInput(
                        reason=_('unrecognized argument %s') % arg_list[i])
            else:
                ret['obj'] = arg_list[i]
        return ret

    def _print_info_cmd(self, rows, delim=' ', nohdr=False, **kwargs):
        """Generic function for printing information."""
        if nohdr:
            del rows[0]

        for index in range(len(rows)):
            rows[index] = delim.join(rows[index])
        return ('%s' % '\n'.join(rows), '')

    def _print_info_obj_cmd(self, header, row, delim=' ', nohdr=False):
        """Generic function for printing information for a specific object."""
        objrows = []
        for idx, val in enumerate(header):
            objrows.append([val, row[idx]])

        if nohdr:
            for index in range(len(objrows)):
                objrows[index] = ' '.join(objrows[index][1:])
        for index in range(len(objrows)):
            objrows[index] = delim.join(objrows[index])
        return ('%s' % '\n'.join(objrows), '')

    def _convert_bytes_units(self, bytestr):
        num = int(bytestr)
        unit_array = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        unit_index = 0

        while num > 1024:
            num = num / 1024
            unit_index += 1

        return '%d%s' % (num, unit_array[unit_index])

    def _convert_units_bytes(self, num, unit):
        unit_array = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        unit_index = 0

        while unit.lower() != unit_array[unit_index].lower():
            num = num * 1024
            unit_index += 1

        return str(num)

    def _cmd_lslicense(self, **kwargs):
        rows = [None] * 3
        rows[0] = ['used_compression_capacity', '0.08']
        rows[1] = ['license_compression_capacity', '0']
        if self._next_cmd_error['lslicense'] == 'no_compression':
            self._next_cmd_error['lslicense'] = ''
            rows[2] = ['license_compression_enclosures', '0']
        else:
            rows[2] = ['license_compression_enclosures', '1']
        return self._print_info_cmd(rows=rows, **kwargs)

    # Print mostly made-up stuff in the correct syntax
    def _cmd_lssystem(self, **kwargs):
        rows = [None] * 2
        rows[0] = ['id', '0123456789ABCDEF']
        rows[1] = ['name', 'storwize-svc-sim']
        return self._print_info_cmd(rows=rows, **kwargs)

    # Print mostly made-up stuff in the correct syntax, assume -bytes passed
    def _cmd_lsmdiskgrp(self, **kwargs):
        rows = [None] * 4
        rows[0] = ['id', 'name', 'status', 'mdisk_count',
                   'vdisk_count', 'capacity', 'extent_size',
                   'free_capacity', 'virtual_capacity', 'used_capacity',
                   'real_capacity', 'overallocation', 'warning',
                   'easy_tier', 'easy_tier_status']
        rows[1] = ['1', self._flags['storwize_svc_volpool_name'], 'online',
                   '1', str(len(self._volumes_list)), '3573412790272',
                   '256', '3529926246400', '1693247906775', '277841182',
                   '38203734097', '47', '80', 'auto', 'inactive']
        rows[2] = ['2', 'openstack2', 'online',
                   '1', '0', '3573412790272', '256',
                   '3529432325160', '1693247906775', '277841182',
                   '38203734097', '47', '80', 'auto', 'inactive']
        rows[3] = ['3', 'openstack3', 'online',
                   '1', '0', '3573412790272', '128',
                   '3529432325160', '1693247906775', '277841182',
                   '38203734097', '47', '80', 'auto', 'inactive']
        if 'obj' not in kwargs:
            return self._print_info_cmd(rows=rows, **kwargs)
        else:
            if kwargs['obj'] == self._flags['storwize_svc_volpool_name']:
                row = rows[1]
            elif kwargs['obj'] == 'openstack2':
                row = rows[2]
            elif kwargs['obj'] == 'openstack3':
                row = rows[3]
            else:
                return self._errors['CMMVC5754E']

            objrows = []
            for idx, val in enumerate(rows[0]):
                objrows.append([val, row[idx]])

            if 'nohdr' in kwargs:
                for index in range(len(objrows)):
                    objrows[index] = ' '.join(objrows[index][1:])

            if 'delim' in kwargs:
                for index in range(len(objrows)):
                    objrows[index] = kwargs['delim'].join(objrows[index])

            return ('%s' % '\n'.join(objrows), '')

    # Print mostly made-up stuff in the correct syntax
    def _cmd_lsnodecanister(self, **kwargs):
        rows = [None] * 3
        rows[0] = ['id', 'name', 'UPS_serial_number', 'WWNN', 'status',
                   'IO_group_id', 'IO_group_name', 'config_node',
                   'UPS_unique_id', 'hardware', 'iscsi_name', 'iscsi_alias',
                   'panel_name', 'enclosure_id', 'canister_id',
                   'enclosure_serial_number']
        rows[1] = ['1', 'node1', '', '123456789ABCDEF0', 'online', '0',
                   'io_grp0',
                   'yes', '123456789ABCDEF0', '100',
                   'iqn.1982-01.com.ibm:1234.sim.node1', '', '01-1', '1', '1',
                   '0123ABC']
        rows[2] = ['2', 'node2', '', '123456789ABCDEF1', 'online', '0',
                   'io_grp0',
                   'no', '123456789ABCDEF1', '100',
                   'iqn.1982-01.com.ibm:1234.sim.node2', '', '01-2', '1', '2',
                   '0123ABC']

        if self._next_cmd_error['lsnodecanister'] == 'header_mismatch':
            rows[0].pop(2)
            self._next_cmd_error['lsnodecanister'] = ''
        if self._next_cmd_error['lsnodecanister'] == 'remove_field':
            for row in rows:
                row.pop(0)
            self._next_cmd_error['lsnodecanister'] = ''

        return self._print_info_cmd(rows=rows, **kwargs)

    # Print information of every single node of SVC
    def _cmd_lsnode(self, **kwargs):
        node_infos = dict()
        node_infos['1'] = r'''id!1
name!node1
port_id!500507680210C744
port_status!active
port_speed!8Gb
port_id!500507680220C744
port_status!active
port_speed!8Gb
'''
        node_infos['2'] = r'''id!2
name!node2
port_id!500507680220C745
port_status!active
port_speed!8Gb
port_id!500507680230C745
port_status!inactive
port_speed!N/A
'''
        node_id = kwargs.get('node_id', None)
        stdout = node_infos.get(node_id, '')
        return stdout, ''

    # Print mostly made-up stuff in the correct syntax
    def _cmd_lsportip(self, **kwargs):
        if self._next_cmd_error['lsportip'] == 'ip_no_config':
            self._next_cmd_error['lsportip'] = ''
            ip_addr1 = ''
            ip_addr2 = ''
            gw = ''
        else:
            ip_addr1 = '1.234.56.78'
            ip_addr2 = '1.234.56.79'
            gw = '1.234.56.1'

        rows = [None] * 17
        rows[0] = ['id', 'node_id', 'node_name', 'IP_address', 'mask',
                   'gateway', 'IP_address_6', 'prefix_6', 'gateway_6', 'MAC',
                   'duplex', 'state', 'speed', 'failover']
        rows[1] = ['1', '1', 'node1', ip_addr1, '255.255.255.0',
                   gw, '', '', '', '01:23:45:67:89:00', 'Full',
                   'online', '1Gb/s', 'no']
        rows[2] = ['1', '1', 'node1', '', '', '', '', '', '',
                   '01:23:45:67:89:00', 'Full', 'online', '1Gb/s', 'yes']
        rows[3] = ['2', '1', 'node1', '', '', '', '', '', '',
                   '01:23:45:67:89:01', 'Full', 'unconfigured', '1Gb/s', 'no']
        rows[4] = ['2', '1', 'node1', '', '', '', '', '', '',
                   '01:23:45:67:89:01', 'Full', 'unconfigured', '1Gb/s', 'yes']
        rows[5] = ['3', '1', 'node1', '', '', '', '', '', '', '', '',
                   'unconfigured', '', 'no']
        rows[6] = ['3', '1', 'node1', '', '', '', '', '', '', '', '',
                   'unconfigured', '', 'yes']
        rows[7] = ['4', '1', 'node1', '', '', '', '', '', '', '', '',
                   'unconfigured', '', 'no']
        rows[8] = ['4', '1', 'node1', '', '', '', '', '', '', '', '',
                   'unconfigured', '', 'yes']
        rows[9] = ['1', '2', 'node2', ip_addr2, '255.255.255.0',
                   gw, '', '', '', '01:23:45:67:89:02', 'Full',
                   'online', '1Gb/s', 'no']
        rows[10] = ['1', '2', 'node2', '', '', '', '', '', '',
                    '01:23:45:67:89:02', 'Full', 'online', '1Gb/s', 'yes']
        rows[11] = ['2', '2', 'node2', '', '', '', '', '', '',
                    '01:23:45:67:89:03', 'Full', 'unconfigured', '1Gb/s', 'no']
        rows[12] = ['2', '2', 'node2', '', '', '', '', '', '',
                    '01:23:45:67:89:03', 'Full', 'unconfigured', '1Gb/s',
                    'yes']
        rows[13] = ['3', '2', 'node2', '', '', '', '', '', '', '', '',
                    'unconfigured', '', 'no']
        rows[14] = ['3', '2', 'node2', '', '', '', '', '', '', '', '',
                    'unconfigured', '', 'yes']
        rows[15] = ['4', '2', 'node2', '', '', '', '', '', '', '', '',
                    'unconfigured', '', 'no']
        rows[16] = ['4', '2', 'node2', '', '', '', '', '', '', '', '',
                    'unconfigured', '', 'yes']

        if self._next_cmd_error['lsportip'] == 'header_mismatch':
            rows[0].pop(2)
            self._next_cmd_error['lsportip'] = ''
        if self._next_cmd_error['lsportip'] == 'remove_field':
            for row in rows:
                row.pop(1)
            self._next_cmd_error['lsportip'] = ''

        return self._print_info_cmd(rows=rows, **kwargs)

    def _cmd_lsfabric(self, **kwargs):
        host_name = kwargs['host'] if 'host' in kwargs else None
        target_wwpn = kwargs['wwpn'] if 'wwpn' in kwargs else None
        host_infos = []

        for hk, hv in self._hosts_list.iteritems():
            if not host_name or hv['host_name'].startswith(host_name):
                for mk, mv in self._mappings_list.iteritems():
                    if mv['host'] == hv['host_name']:
                        if not target_wwpn or target_wwpn in hv['wwpns']:
                            host_infos.append(hv)
                            break

        if not len(host_infos):
            return ('', '')

        rows = []
        rows.append(['remote_wwpn', 'remote_nportid', 'id', 'node_name',
                     'local_wwpn', 'local_port', 'local_nportid', 'state',
                     'name', 'cluster_name', 'type'])
        for host_info in host_infos:
            for wwpn in host_info['wwpns']:
                rows.append([wwpn, '123456', host_info['id'], 'nodeN',
                            'AABBCCDDEEFF0011', '1', '0123ABC', 'active',
                            host_info['host_name'], '', 'host'])

        if self._next_cmd_error['lsfabric'] == 'header_mismatch':
            rows[0].pop(0)
            self._next_cmd_error['lsfabric'] = ''
        if self._next_cmd_error['lsfabric'] == 'remove_field':
            for row in rows:
                row.pop(0)
            self._next_cmd_error['lsfabric'] = ''
        return self._print_info_cmd(rows=rows, **kwargs)

    # Create a vdisk
    def _cmd_mkvdisk(self, **kwargs):
        # We only save the id/uid, name, and size - all else will be made up
        volume_info = {}
        volume_info['id'] = self._find_unused_id(self._volumes_list)
        volume_info['uid'] = ('ABCDEF' * 3) + ('0' * 14) + volume_info['id']

        if 'name' in kwargs:
            volume_info['name'] = kwargs['name'].strip('\'\'')
        else:
            volume_info['name'] = 'vdisk' + volume_info['id']

        # Assume size and unit are given, store it in bytes
        capacity = int(kwargs['size'])
        unit = kwargs['unit']
        volume_info['capacity'] = self._convert_units_bytes(capacity, unit)
        volume_info['IO_group_id'] = kwargs['iogrp']
        volume_info['IO_group_name'] = 'io_grp%s' % kwargs['iogrp']

        if 'easytier' in kwargs:
            if kwargs['easytier'] == 'on':
                volume_info['easy_tier'] = 'on'
            else:
                volume_info['easy_tier'] = 'off'

        if 'rsize' in kwargs:
            # Fake numbers
            volume_info['used_capacity'] = '786432'
            volume_info['real_capacity'] = '21474816'
            volume_info['free_capacity'] = '38219264'
            if 'warning' in kwargs:
                volume_info['warning'] = kwargs['warning'].rstrip('%')
            else:
                volume_info['warning'] = '80'
            if 'autoexpand' in kwargs:
                volume_info['autoexpand'] = 'on'
            else:
                volume_info['autoexpand'] = 'off'
            if 'grainsize' in kwargs:
                volume_info['grainsize'] = kwargs['grainsize']
            else:
                volume_info['grainsize'] = '32'
            if 'compressed' in kwargs:
                volume_info['compressed_copy'] = 'yes'
            else:
                volume_info['compressed_copy'] = 'no'
        else:
            volume_info['used_capacity'] = volume_info['capacity']
            volume_info['real_capacity'] = volume_info['capacity']
            volume_info['free_capacity'] = '0'
            volume_info['warning'] = ''
            volume_info['autoexpand'] = ''
            volume_info['grainsize'] = ''
            volume_info['compressed_copy'] = 'no'

        vol_cp = {'id': '0',
                  'status': 'online',
                  'sync': 'yes',
                  'primary': 'yes',
                  'mdisk_grp_id': '1',
                  'mdisk_grp_name': self._flags['storwize_svc_volpool_name'],
                  'easy_tier': volume_info['easy_tier'],
                  'compressed_copy': volume_info['compressed_copy']}
        volume_info['copies'] = {'0': vol_cp}

        if volume_info['name'] in self._volumes_list:
            return self._errors['CMMVC6035E']
        else:
            self._volumes_list[volume_info['name']] = volume_info
            return ('Virtual Disk, id [%s], successfully created' %
                    (volume_info['id']), '')

    # Delete a vdisk
    def _cmd_rmvdisk(self, **kwargs):
        force = True if 'force' in kwargs else False

        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        vol_name = kwargs['obj'].strip('\'\'')

        if vol_name not in self._volumes_list:
            return self._errors['CMMVC5753E']

        if not force:
            for k, mapping in self._mappings_list.iteritems():
                if mapping['vol'] == vol_name:
                    return self._errors['CMMVC5840E']
            for k, fcmap in self._fcmappings_list.iteritems():
                if ((fcmap['source'] == vol_name) or
                        (fcmap['target'] == vol_name)):
                    return self._errors['CMMVC5840E']

        del self._volumes_list[vol_name]
        return ('', '')

    def _cmd_expandvdisksize(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        vol_name = kwargs['obj'].strip('\'\'')

        # Assume unit is gb
        if 'size' not in kwargs:
            return self._errors['CMMVC5707E']
        size = int(kwargs['size'])

        if vol_name not in self._volumes_list:
            return self._errors['CMMVC5753E']

        curr_size = int(self._volumes_list[vol_name]['capacity'])
        addition = size * units.GiB
        self._volumes_list[vol_name]['capacity'] = str(curr_size + addition)
        return ('', '')

    def _get_fcmap_info(self, vol_name):
        ret_vals = {
            'fc_id': '',
            'fc_name': '',
            'fc_map_count': '0',
        }
        for k, fcmap in self._fcmappings_list.iteritems():
            if ((fcmap['source'] == vol_name) or
                    (fcmap['target'] == vol_name)):
                ret_vals['fc_id'] = fcmap['id']
                ret_vals['fc_name'] = fcmap['name']
                ret_vals['fc_map_count'] = '1'
        return ret_vals

    # List information about vdisks
    def _cmd_lsvdisk(self, **kwargs):
        rows = []
        rows.append(['id', 'name', 'IO_group_id', 'IO_group_name',
                     'status', 'mdisk_grp_id', 'mdisk_grp_name',
                     'capacity', 'type', 'FC_id', 'FC_name', 'RC_id',
                     'RC_name', 'vdisk_UID', 'fc_map_count', 'copy_count',
                     'fast_write_state', 'se_copy_count', 'RC_change'])

        for k, vol in self._volumes_list.iteritems():
            if (('filtervalue' not in kwargs) or
                    (kwargs['filtervalue'] == 'name=' + vol['name'])):
                fcmap_info = self._get_fcmap_info(vol['name'])

                if 'bytes' in kwargs:
                    cap = self._convert_bytes_units(vol['capacity'])
                else:
                    cap = vol['capacity']
                rows.append([str(vol['id']), vol['name'], vol['IO_group_id'],
                            vol['IO_group_name'], 'online', '0',
                            self._flags['storwize_svc_volpool_name'],
                            cap, 'striped',
                            fcmap_info['fc_id'], fcmap_info['fc_name'],
                            '', '', vol['uid'],
                            fcmap_info['fc_map_count'], '1', 'empty',
                            '1', 'no'])

        if 'obj' not in kwargs:
            return self._print_info_cmd(rows=rows, **kwargs)
        else:
            if kwargs['obj'] not in self._volumes_list:
                return self._errors['CMMVC5754E']
            vol = self._volumes_list[kwargs['obj']]
            fcmap_info = self._get_fcmap_info(vol['name'])
            cap = vol['capacity']
            cap_u = vol['used_capacity']
            cap_r = vol['real_capacity']
            cap_f = vol['free_capacity']
            if 'bytes' not in kwargs:
                for item in [cap, cap_u, cap_r, cap_f]:
                    item = self._convert_bytes_units(item)
            rows = []

            rows.append(['id', str(vol['id'])])
            rows.append(['name', vol['name']])
            rows.append(['IO_group_id', vol['IO_group_id']])
            rows.append(['IO_group_name', vol['IO_group_name']])
            rows.append(['status', 'online'])
            rows.append(['mdisk_grp_id', '0'])
            rows.append([
                'mdisk_grp_name',
                self._flags['storwize_svc_volpool_name']])
            rows.append(['capacity', cap])
            rows.append(['type', 'striped'])
            rows.append(['formatted', 'no'])
            rows.append(['mdisk_id', ''])
            rows.append(['mdisk_name', ''])
            rows.append(['FC_id', fcmap_info['fc_id']])
            rows.append(['FC_name', fcmap_info['fc_name']])
            rows.append(['RC_id', ''])
            rows.append(['RC_name', ''])
            rows.append(['vdisk_UID', vol['uid']])
            rows.append(['throttling', '0'])

            if self._next_cmd_error['lsvdisk'] == 'blank_pref_node':
                rows.append(['preferred_node_id', ''])
                self._next_cmd_error['lsvdisk'] = ''
            elif self._next_cmd_error['lsvdisk'] == 'no_pref_node':
                self._next_cmd_error['lsvdisk'] = ''
            else:
                rows.append(['preferred_node_id', '1'])
            rows.append(['fast_write_state', 'empty'])
            rows.append(['cache', 'readwrite'])
            rows.append(['udid', ''])
            rows.append(['fc_map_count', fcmap_info['fc_map_count']])
            rows.append(['sync_rate', '50'])
            rows.append(['copy_count', '1'])
            rows.append(['se_copy_count', '0'])
            rows.append(['mirror_write_priority', 'latency'])
            rows.append(['RC_change', 'no'])
            rows.append(['used_capacity', cap_u])
            rows.append(['real_capacity', cap_r])
            rows.append(['free_capacity', cap_f])
            rows.append(['autoexpand', vol['autoexpand']])
            rows.append(['warning', vol['warning']])
            rows.append(['grainsize', vol['grainsize']])
            rows.append(['easy_tier', vol['easy_tier']])
            rows.append(['compressed_copy', vol['compressed_copy']])

            if 'nohdr' in kwargs:
                for index in range(len(rows)):
                    rows[index] = ' '.join(rows[index][1:])

            if 'delim' in kwargs:
                for index in range(len(rows)):
                    rows[index] = kwargs['delim'].join(rows[index])

            return ('%s' % '\n'.join(rows), '')

    def _cmd_lsiogrp(self, **kwargs):
        rows = [None] * 6
        rows[0] = ['id', 'name', 'node_count', 'vdisk_count', 'host_count']
        rows[1] = ['0', 'io_grp0', '2', '0', '4']
        rows[2] = ['1', 'io_grp1', '2', '0', '4']
        rows[3] = ['2', 'io_grp2', '0', '0', '4']
        rows[4] = ['3', 'io_grp3', '0', '0', '4']
        rows[5] = ['4', 'recovery_io_grp', '0', '0', '0']
        return self._print_info_cmd(rows=rows, **kwargs)

    def _add_port_to_host(self, host_info, **kwargs):
        if 'iscsiname' in kwargs:
            added_key = 'iscsi_names'
            added_val = kwargs['iscsiname'].strip('\'\"')
        elif 'hbawwpn' in kwargs:
            added_key = 'wwpns'
            added_val = kwargs['hbawwpn'].strip('\'\"')
        else:
            return self._errors['CMMVC5707E']

        host_info[added_key].append(added_val)

        for k, v in self._hosts_list.iteritems():
            if v['id'] == host_info['id']:
                continue
            for port in v[added_key]:
                if port == added_val:
                    return self._errors['CMMVC6581E']
        return ('', '')

    # Make a host
    def _cmd_mkhost(self, **kwargs):
        host_info = {}
        host_info['id'] = self._find_unused_id(self._hosts_list)

        if 'name' in kwargs:
            host_name = kwargs['name'].strip('\'\"')
        else:
            host_name = 'host' + str(host_info['id'])

        if self._is_invalid_name(host_name):
            return self._errors['CMMVC6527E']

        if host_name in self._hosts_list:
            return self._errors['CMMVC6035E']

        host_info['host_name'] = host_name
        host_info['iscsi_names'] = []
        host_info['wwpns'] = []

        out, err = self._add_port_to_host(host_info, **kwargs)
        if not len(err):
            self._hosts_list[host_name] = host_info
            return ('Host, id [%s], successfully created' %
                    (host_info['id']), '')
        else:
            return (out, err)

    # Add ports to an existing host
    def _cmd_addhostport(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        host_name = kwargs['obj'].strip('\'\'')

        if host_name not in self._hosts_list:
            return self._errors['CMMVC5753E']

        host_info = self._hosts_list[host_name]
        return self._add_port_to_host(host_info, **kwargs)

    # Change host properties
    def _cmd_chhost(self, **kwargs):
        if 'chapsecret' not in kwargs:
            return self._errors['CMMVC5707E']
        secret = kwargs['obj'].strip('\'\'')

        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        host_name = kwargs['obj'].strip('\'\'')

        if host_name not in self._hosts_list:
            return self._errors['CMMVC5753E']

        self._hosts_list[host_name]['chapsecret'] = secret
        return ('', '')

    # Remove a host
    def _cmd_rmhost(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']

        host_name = kwargs['obj'].strip('\'\'')
        if host_name not in self._hosts_list:
            return self._errors['CMMVC5753E']

        for k, v in self._mappings_list.iteritems():
            if (v['host'] == host_name):
                return self._errors['CMMVC5871E']

        del self._hosts_list[host_name]
        return ('', '')

    # List information about hosts
    def _cmd_lshost(self, **kwargs):
        if 'obj' not in kwargs:
            rows = []
            rows.append(['id', 'name', 'port_count', 'iogrp_count', 'status'])

            found = False
            for k, host in self._hosts_list.iteritems():
                filterstr = 'name=' + host['host_name']
                if (('filtervalue' not in kwargs) or
                        (kwargs['filtervalue'] == filterstr)):
                    rows.append([host['id'], host['host_name'], '1', '4',
                                'offline'])
                    found = True
            if found:
                return self._print_info_cmd(rows=rows, **kwargs)
            else:
                return ('', '')
        else:
            if kwargs['obj'] not in self._hosts_list:
                return self._errors['CMMVC5754E']
            host = self._hosts_list[kwargs['obj']]
            rows = []
            rows.append(['id', host['id']])
            rows.append(['name', host['host_name']])
            rows.append(['port_count', '1'])
            rows.append(['type', 'generic'])
            rows.append(['mask', '1111'])
            rows.append(['iogrp_count', '4'])
            rows.append(['status', 'online'])
            for port in host['iscsi_names']:
                rows.append(['iscsi_name', port])
                rows.append(['node_logged_in_count', '0'])
                rows.append(['state', 'offline'])
            for port in host['wwpns']:
                rows.append(['WWPN', port])
                rows.append(['node_logged_in_count', '0'])
                rows.append(['state', 'active'])

            if 'nohdr' in kwargs:
                for index in range(len(rows)):
                    rows[index] = ' '.join(rows[index][1:])

            if 'delim' in kwargs:
                for index in range(len(rows)):
                    rows[index] = kwargs['delim'].join(rows[index])

            return ('%s' % '\n'.join(rows), '')

    # List iSCSI authorization information about hosts
    def _cmd_lsiscsiauth(self, **kwargs):
        if self._next_cmd_error['lsiscsiauth'] == 'no_info':
            self._next_cmd_error['lsiscsiauth'] = ''
            return ('', '')
        rows = []
        rows.append(['type', 'id', 'name', 'iscsi_auth_method',
                     'iscsi_chap_secret'])

        for k, host in self._hosts_list.iteritems():
            method = 'none'
            secret = ''
            if 'chapsecret' in host:
                method = 'chap'
                secret = host['chapsecret']
            rows.append(['host', host['id'], host['host_name'], method,
                         secret])
        return self._print_info_cmd(rows=rows, **kwargs)

    # Create a vdisk-host mapping
    def _cmd_mkvdiskhostmap(self, **kwargs):
        mapping_info = {}
        mapping_info['id'] = self._find_unused_id(self._mappings_list)

        if 'host' not in kwargs:
            return self._errors['CMMVC5707E']
        mapping_info['host'] = kwargs['host'].strip('\'\'')

        if 'scsi' not in kwargs:
            return self._errors['CMMVC5707E']
        mapping_info['lun'] = kwargs['scsi'].strip('\'\'')

        if 'obj' not in kwargs:
            return self._errors['CMMVC5707E']
        mapping_info['vol'] = kwargs['obj'].strip('\'\'')

        if mapping_info['vol'] not in self._volumes_list:
            return self._errors['CMMVC5753E']

        if mapping_info['host'] not in self._hosts_list:
            return self._errors['CMMVC5754E']

        if mapping_info['vol'] in self._mappings_list:
            return self._errors['CMMVC6071E']

        for k, v in self._mappings_list.iteritems():
            if ((v['host'] == mapping_info['host']) and
                    (v['lun'] == mapping_info['lun'])):
                return self._errors['CMMVC5879E']

        for k, v in self._mappings_list.iteritems():
            if (v['lun'] == mapping_info['lun']) and ('force' not in kwargs):
                return self._errors['CMMVC6071E']

        self._mappings_list[mapping_info['id']] = mapping_info
        return ('Virtual Disk to Host map, id [%s], successfully created'
                % (mapping_info['id']), '')

    # Delete a vdisk-host mapping
    def _cmd_rmvdiskhostmap(self, **kwargs):
        if 'host' not in kwargs:
            return self._errors['CMMVC5707E']
        host = kwargs['host'].strip('\'\'')

        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        vol = kwargs['obj'].strip('\'\'')

        mapping_ids = []
        for k, v in self._mappings_list.iteritems():
            if v['vol'] == vol:
                mapping_ids.append(v['id'])
        if not mapping_ids:
            return self._errors['CMMVC5753E']

        this_mapping = None
        for mapping_id in mapping_ids:
            if self._mappings_list[mapping_id]['host'] == host:
                this_mapping = mapping_id
        if this_mapping is None:
            return self._errors['CMMVC5753E']

        del self._mappings_list[this_mapping]
        return ('', '')

    # List information about host->vdisk mappings
    def _cmd_lshostvdiskmap(self, **kwargs):
        index = 1
        no_hdr = 0
        delimeter = ''
        host_name = kwargs['obj']

        if host_name not in self._hosts_list:
            return self._errors['CMMVC5754E']

        rows = []
        rows.append(['id', 'name', 'SCSI_id', 'vdisk_id', 'vdisk_name',
                     'vdisk_UID'])

        for k, mapping in self._mappings_list.iteritems():
            if (host_name == '') or (mapping['host'] == host_name):
                volume = self._volumes_list[mapping['vol']]
                rows.append([mapping['id'], mapping['host'],
                            mapping['lun'], volume['id'],
                            volume['name'], volume['uid']])

        return self._print_info_cmd(rows=rows, **kwargs)

    # List information about vdisk->host mappings
    def _cmd_lsvdiskhostmap(self, **kwargs):
        mappings_found = 0
        vdisk_name = kwargs['obj']

        if vdisk_name not in self._volumes_list:
            return self._errors['CMMVC5753E']

        rows = []
        rows.append(['id name', 'SCSI_id', 'host_id', 'host_name', 'vdisk_UID',
                     'IO_group_id', 'IO_group_name'])

        for k, mapping in self._mappings_list.iteritems():
            if (mapping['vol'] == vdisk_name):
                mappings_found += 1
                volume = self._volumes_list[mapping['vol']]
                host = self._hosts_list[mapping['host']]
                rows.append([volume['id'], volume['name'], host['id'],
                            host['host_name'], volume['uid'],
                            volume['IO_group_id'], volume['IO_group_name']])

        if mappings_found:
            return self._print_info_cmd(rows=rows, **kwargs)
        else:
            return ('', '')

    # Create a FlashCopy mapping
    def _cmd_mkfcmap(self, **kwargs):
        source = ''
        target = ''
        copyrate = kwargs['copyrate'] if 'copyrate' in kwargs else '50'

        if 'source' not in kwargs:
            return self._errors['CMMVC5707E']
        source = kwargs['source'].strip('\'\'')
        if source not in self._volumes_list:
            return self._errors['CMMVC5754E']

        if 'target' not in kwargs:
            return self._errors['CMMVC5707E']
        target = kwargs['target'].strip('\'\'')
        if target not in self._volumes_list:
            return self._errors['CMMVC5754E']

        if source == target:
            return self._errors['CMMVC6303E']

        if (self._volumes_list[source]['capacity'] !=
                self._volumes_list[target]['capacity']):
            return self._errors['CMMVC5924E']

        fcmap_info = {}
        fcmap_info['source'] = source
        fcmap_info['target'] = target
        fcmap_info['id'] = self._find_unused_id(self._fcmappings_list)
        fcmap_info['name'] = 'fcmap' + fcmap_info['id']
        fcmap_info['copyrate'] = copyrate
        fcmap_info['progress'] = '0'
        fcmap_info['autodelete'] = True if 'autodelete' in kwargs else False
        fcmap_info['status'] = 'idle_or_copied'
        self._fcmappings_list[fcmap_info['id']] = fcmap_info

        return('FlashCopy Mapping, id [' + fcmap_info['id'] +
               '], successfully created', '')

    def _cmd_gen_prestartfcmap(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        id_num = kwargs['obj']

        if self._next_cmd_error['prestartfcmap'] == 'bad_id':
            id_num = -1
            self._next_cmd_error['prestartfcmap'] = ''

        try:
            fcmap = self._fcmappings_list[id_num]
        except KeyError:
            return self._errors['CMMVC5753E']

        return self._state_transition('prepare', fcmap)

    def _cmd_gen_startfcmap(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        id_num = kwargs['obj']

        if self._next_cmd_error['startfcmap'] == 'bad_id':
            id_num = -1
            self._next_cmd_error['startfcmap'] = ''

        try:
            fcmap = self._fcmappings_list[id_num]
        except KeyError:
            return self._errors['CMMVC5753E']

        return self._state_transition('start', fcmap)

    def _cmd_stopfcmap(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        id_num = kwargs['obj']

        try:
            fcmap = self._fcmappings_list[id_num]
        except KeyError:
            return self._errors['CMMVC5753E']

        return self._state_transition('stop', fcmap)

    def _cmd_rmfcmap(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        id_num = kwargs['obj']
        force = True if 'force' in kwargs else False

        if self._next_cmd_error['rmfcmap'] == 'bad_id':
            id_num = -1
            self._next_cmd_error['rmfcmap'] = ''

        try:
            fcmap = self._fcmappings_list[id_num]
        except KeyError:
            return self._errors['CMMVC5753E']

        function = 'delete_force' if force else 'delete'
        ret = self._state_transition(function, fcmap)
        if fcmap['status'] == 'end':
            del self._fcmappings_list[id_num]
        return ret

    def _cmd_lsvdiskfcmappings(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5707E']
        vdisk = kwargs['obj']
        rows = []
        rows.append(['id', 'name'])
        for k, v in self._fcmappings_list.iteritems():
            if v['source'] == vdisk or v['target'] == vdisk:
                rows.append([v['id'], v['name']])
        return self._print_info_cmd(rows=rows, **kwargs)

    def _cmd_chfcmap(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5707E']
        id_num = kwargs['obj']

        try:
            fcmap = self._fcmappings_list[id_num]
        except KeyError:
            return self._errors['CMMVC5753E']

        for key in ['name', 'copyrate', 'autodelete']:
            if key in kwargs:
                fcmap[key] = kwargs[key]
        return ('', '')

    def _cmd_lsfcmap(self, **kwargs):
        rows = []
        rows.append(['id', 'name', 'source_vdisk_id', 'source_vdisk_name',
                     'target_vdisk_id', 'target_vdisk_name', 'group_id',
                     'group_name', 'status', 'progress', 'copy_rate',
                     'clean_progress', 'incremental', 'partner_FC_id',
                     'partner_FC_name', 'restoring', 'start_time',
                     'rc_controlled'])

        # Assume we always get a filtervalue argument
        filter_key = kwargs['filtervalue'].split('=')[0]
        filter_value = kwargs['filtervalue'].split('=')[1]
        to_delete = []
        for k, v in self._fcmappings_list.iteritems():
            if str(v[filter_key]) == filter_value:
                source = self._volumes_list[v['source']]
                target = self._volumes_list[v['target']]
                self._state_transition('wait', v)

                if self._next_cmd_error['lsfcmap'] == 'speed_up':
                    self._next_cmd_error['lsfcmap'] = ''
                    curr_state = v['status']
                    while self._state_transition('wait', v) == ("", ""):
                        if curr_state == v['status']:
                            break
                        curr_state = v['status']

                if ((v['status'] == 'idle_or_copied' and v['autodelete'] and
                     v['progress'] == '100') or (v['status'] == 'end')):
                    to_delete.append(k)
                else:
                    rows.append([v['id'], v['name'], source['id'],
                                source['name'], target['id'], target['name'],
                                '', '', v['status'], v['progress'],
                                v['copyrate'], '100', 'off', '', '', 'no', '',
                                'no'])

        for d in to_delete:
            del self._fcmappings_list[d]

        return self._print_info_cmd(rows=rows, **kwargs)

    def _cmd_migratevdisk(self, **kwargs):
        if 'mdiskgrp' not in kwargs or 'vdisk' not in kwargs:
            return self._errors['CMMVC5707E']
        mdiskgrp = kwargs['mdiskgrp'].strip('\'\'')
        vdisk = kwargs['vdisk'].strip('\'\'')

        if vdisk in self._volumes_list:
            curr_mdiskgrp = self._volumes_list
        else:
            for pool in self._other_pools:
                if vdisk in pool:
                    curr_mdiskgrp = pool
                    break
            else:
                return self._errors['CMMVC5754E']

        if mdiskgrp == self._flags['storwize_svc_volpool_name']:
            tgt_mdiskgrp = self._volumes_list
        elif mdiskgrp == 'openstack2':
            tgt_mdiskgrp = self._other_pools['openstack2']
        elif mdiskgrp == 'openstack3':
            tgt_mdiskgrp = self._other_pools['openstack3']
        else:
            return self._errors['CMMVC5754E']

        if curr_mdiskgrp == tgt_mdiskgrp:
            return self._errors['CMMVC6430E']

        vol = curr_mdiskgrp[vdisk]
        tgt_mdiskgrp[vdisk] = vol
        del curr_mdiskgrp[vdisk]
        return ('', '')

    def _cmd_addvdiskcopy(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        vol_name = kwargs['obj'].strip('\'\'')
        if vol_name not in self._volumes_list:
            return self._errors['CMMVC5753E']
        vol = self._volumes_list[vol_name]
        if 'mdiskgrp' not in kwargs:
            return self._errors['CMMVC5707E']
        mdiskgrp = kwargs['mdiskgrp'].strip('\'\'')

        copy_info = {}
        copy_info['id'] = self._find_unused_id(vol['copies'])
        copy_info['status'] = 'online'
        copy_info['sync'] = 'no'
        copy_info['primary'] = 'no'
        copy_info['mdisk_grp_name'] = mdiskgrp
        if mdiskgrp == self._flags['storwize_svc_volpool_name']:
            copy_info['mdisk_grp_id'] = '1'
        elif mdiskgrp == 'openstack2':
            copy_info['mdisk_grp_id'] = '2'
        elif mdiskgrp == 'openstack3':
            copy_info['mdisk_grp_id'] = '3'
        if 'easytier' in kwargs:
            if kwargs['easytier'] == 'on':
                copy_info['easy_tier'] = 'on'
            else:
                copy_info['easy_tier'] = 'off'
        if 'rsize' in kwargs:
            if 'compressed' in kwargs:
                copy_info['compressed_copy'] = 'yes'
            else:
                copy_info['compressed_copy'] = 'no'
        vol['copies'][copy_info['id']] = copy_info
        return ('Vdisk [%(vid)s] copy [%(cid)s] successfully created' %
                {'vid': vol['id'], 'cid': copy_info['id']}, '')

    def _cmd_lsvdiskcopy(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5804E']
        name = kwargs['obj']
        vol = self._volumes_list[name]
        rows = []
        rows.append(['vdisk_id', 'vdisk_name', 'copy_id', 'status', 'sync',
                     'primary', 'mdisk_grp_id', 'mdisk_grp_name', 'capacity',
                     'type', 'se_copy', 'easy_tier', 'easy_tier_status',
                     'compressed_copy'])
        for k, copy in vol['copies'].iteritems():
            rows.append([vol['id'], vol['name'], copy['id'],
                        copy['status'], copy['sync'], copy['primary'],
                        copy['mdisk_grp_id'], copy['mdisk_grp_name'],
                        vol['capacity'], 'striped', 'yes', copy['easy_tier'],
                        'inactive', copy['compressed_copy']])
        if 'copy' not in kwargs:
            return self._print_info_cmd(rows=rows, **kwargs)
        else:
            copy_id = kwargs['copy'].strip('\'\'')
            if copy_id not in vol['copies']:
                return self._errors['CMMVC6353E']
            copy = vol['copies'][copy_id]
            rows = []
            rows.append(['vdisk_id', vol['id']])
            rows.append(['vdisk_name', vol['name']])
            rows.append(['capacity', vol['capacity']])
            rows.append(['copy_id', copy['id']])
            rows.append(['status', copy['status']])
            rows.append(['sync', copy['sync']])
            copy['sync'] = 'yes'
            rows.append(['primary', copy['primary']])
            rows.append(['mdisk_grp_id', copy['mdisk_grp_id']])
            rows.append(['mdisk_grp_name', copy['mdisk_grp_name']])
            rows.append(['easy_tier', copy['easy_tier']])
            rows.append(['easy_tier_status', 'inactive'])
            rows.append(['compressed_copy', copy['compressed_copy']])

            if 'delim' in kwargs:
                for index in range(len(rows)):
                    rows[index] = kwargs['delim'].join(rows[index])

            return ('%s' % '\n'.join(rows), '')

    def _cmd_rmvdiskcopy(self, **kwargs):
        if 'obj' not in kwargs:
            return self._errors['CMMVC5701E']
        vol_name = kwargs['obj'].strip('\'\'')
        if 'copy' not in kwargs:
            return self._errors['CMMVC5707E']
        copy_id = kwargs['copy'].strip('\'\'')
        if vol_name not in self._volumes_list:
            return self._errors['CMMVC5753E']
        vol = self._volumes_list[vol_name]
        if copy_id not in vol['copies']:
            return self._errors['CMMVC6353E']
        del vol['copies'][copy_id]
        return ('', '')

    def _add_host_to_list(self, connector):
        host_info = {}
        host_info['id'] = self._find_unused_id(self._hosts_list)
        host_info['host_name'] = connector['host']
        host_info['iscsi_names'] = []
        host_info['wwpns'] = []
        if 'initiator' in connector:
            host_info['iscsi_names'].append(connector['initiator'])
        if 'wwpns' in connector:
            host_info['wwpns'] = host_info['wwpns'] + connector['wwpns']
        self._hosts_list[connector['host']] = host_info

    def _host_in_list(self, host_name):
        for k, v in self._hosts_list.iteritems():
            if k.startswith(host_name):
                return k
        return None

    # The main function to run commands on the management simulator
    def execute_command(self, cmd, check_exit_code=True):
        try:
            kwargs = self._cmd_to_dict(cmd)
        except IndexError:
            return self._errors['CMMVC5707E']

        command = kwargs['cmd']
        del kwargs['cmd']

        if command == 'lsmdiskgrp':
            out, err = self._cmd_lsmdiskgrp(**kwargs)
        elif command == 'lslicense':
            out, err = self._cmd_lslicense(**kwargs)
        elif command == 'lssystem':
            out, err = self._cmd_lssystem(**kwargs)
        elif command == 'lsnodecanister':
            out, err = self._cmd_lsnodecanister(**kwargs)
        elif command == 'lsnode':
            out, err = self._cmd_lsnode(**kwargs)
        elif command == 'lsportip':
            out, err = self._cmd_lsportip(**kwargs)
        elif command == 'lsfabric':
            out, err = self._cmd_lsfabric(**kwargs)
        elif command == 'mkvdisk':
            out, err = self._cmd_mkvdisk(**kwargs)
        elif command == 'rmvdisk':
            out, err = self._cmd_rmvdisk(**kwargs)
        elif command == 'expandvdisksize':
            out, err = self._cmd_expandvdisksize(**kwargs)
        elif command == 'lsvdisk':
            out, err = self._cmd_lsvdisk(**kwargs)
        elif command == 'lsiogrp':
            out, err = self._cmd_lsiogrp(**kwargs)
        elif command == 'mkhost':
            out, err = self._cmd_mkhost(**kwargs)
        elif command == 'addhostport':
            out, err = self._cmd_addhostport(**kwargs)
        elif command == 'chhost':
            out, err = self._cmd_chhost(**kwargs)
        elif command == 'rmhost':
            out, err = self._cmd_rmhost(**kwargs)
        elif command == 'lshost':
            out, err = self._cmd_lshost(**kwargs)
        elif command == 'lsiscsiauth':
            out, err = self._cmd_lsiscsiauth(**kwargs)
        elif command == 'mkvdiskhostmap':
            out, err = self._cmd_mkvdiskhostmap(**kwargs)
        elif command == 'rmvdiskhostmap':
            out, err = self._cmd_rmvdiskhostmap(**kwargs)
        elif command == 'lshostvdiskmap':
            out, err = self._cmd_lshostvdiskmap(**kwargs)
        elif command == 'lsvdiskhostmap':
            out, err = self._cmd_lsvdiskhostmap(**kwargs)
        elif command == 'mkfcmap':
            out, err = self._cmd_mkfcmap(**kwargs)
        elif command == 'prestartfcmap':
            out, err = self._cmd_gen_prestartfcmap(**kwargs)
        elif command == 'startfcmap':
            out, err = self._cmd_gen_startfcmap(**kwargs)
        elif command == 'stopfcmap':
            out, err = self._cmd_stopfcmap(**kwargs)
        elif command == 'rmfcmap':
            out, err = self._cmd_rmfcmap(**kwargs)
        elif command == 'chfcmap':
            out, err = self._cmd_chfcmap(**kwargs)
        elif command == 'lsfcmap':
            out, err = self._cmd_lsfcmap(**kwargs)
        elif command == 'lsvdiskfcmappings':
            out, err = self._cmd_lsvdiskfcmappings(**kwargs)
        elif command == 'migratevdisk':
            out, err = self._cmd_migratevdisk(**kwargs)
        elif command == 'addvdiskcopy':
            out, err = self._cmd_addvdiskcopy(**kwargs)
        elif command == 'lsvdiskcopy':
            out, err = self._cmd_lsvdiskcopy(**kwargs)
        elif command == 'rmvdiskcopy':
            out, err = self._cmd_rmvdiskcopy(**kwargs)
        else:
            out, err = ('', 'ERROR: Unsupported command')

        if (check_exit_code) and (len(err) != 0):
            raise processutils.ProcessExecutionError(exit_code=1,
                                                     stdout=out,
                                                     stderr=err,
                                                     cmd=' '.join(cmd))

        return (out, err)

    # After calling this function, the next call to the specified command will
    # result in in the error specified
    def error_injection(self, cmd, error):
        self._next_cmd_error[cmd] = error


class StorwizeSVCFakeDriver(storwize_svc.StorwizeSVCDriver):
    def __init__(self, *args, **kwargs):
        super(StorwizeSVCFakeDriver, self).__init__(*args, **kwargs)

    def set_fake_storage(self, fake):
        self.fake_storage = fake

    def _run_ssh(self, cmd, check_exit_code=True):
        try:
            LOG.debug(_('Run CLI command: %s') % cmd)
            ret = self.fake_storage.execute_command(cmd, check_exit_code)
            (stdout, stderr) = ret
            LOG.debug(_('CLI output:\n stdout: %(stdout)s\n stderr: '
                        '%(stderr)s') % {'stdout': stdout, 'stderr': stderr})

        except processutils.ProcessExecutionError as e:
            with excutils.save_and_reraise_exception():
                LOG.debug(_('CLI Exception output:\n stdout: %(out)s\n '
                            'stderr: %(err)s') % {'out': e.stdout,
                                                  'err': e.stderr})

        return ret


class StorwizeSVCFakeSock:
    def settimeout(self, time):
        return


class StorwizeSVCDriverTestCase(test.TestCase):
    def setUp(self):
        super(StorwizeSVCDriverTestCase, self).setUp()
        self.USESIM = True
        if self.USESIM:
            self.driver = StorwizeSVCFakeDriver(
                configuration=conf.Configuration(None))
            self._def_flags = {'san_ip': 'hostname',
                               'san_login': 'user',
                               'san_password': 'pass',
                               'storwize_svc_volpool_name': 'openstack',
                               'storwize_svc_flashcopy_timeout': 20,
                               # Test ignore capitalization
                               'storwize_svc_connection_protocol': 'iScSi',
                               'storwize_svc_multipath_enabled': False}
            wwpns = [str(random.randint(0, 9999999999999999)).zfill(16),
                     str(random.randint(0, 9999999999999999)).zfill(16)]
            initiator = 'test.initiator.%s' % str(random.randint(10000, 99999))
            self._connector = {'ip': '1.234.56.78',
                               'host': 'storwize-svc-test',
                               'wwpns': wwpns,
                               'initiator': initiator}
            self.sim = StorwizeSVCManagementSimulator('openstack')

            self.driver.set_fake_storage(self.sim)
        else:
            self.driver = storwize_svc.StorwizeSVCDriver(
                configuration=conf.Configuration(None))
            self._def_flags = {'san_ip': '1.111.11.11',
                               'san_login': 'user',
                               'san_password': 'password',
                               'storwize_svc_volpool_name': 'openstack',
                               # Test ignore capitalization
                               'storwize_svc_connection_protocol': 'iScSi',
                               'storwize_svc_multipath_enabled': False,
                               'ssh_conn_timeout': 0}
            config_group = self.driver.configuration.config_group
            self.driver.configuration.set_override('rootwrap_config',
                                                   '/etc/cinder/rootwrap.conf',
                                                   config_group)
            self._connector = utils.brick_get_connector_properties()

        self._reset_flags()
        self.driver.db = StorwizeSVCFakeDB()
        self.driver.do_setup(None)
        self.driver.check_for_setup_error()
        self.stubs.Set(storwize_svc.time, 'sleep', lambda s: None)
        self.stubs.Set(greenthread, 'sleep', lambda *x, **y: None)
        self.stubs.Set(storwize_svc, 'CHECK_FCMAPPING_INTERVAL', 0)

    def _set_flag(self, flag, value):
        group = self.driver.configuration.config_group
        self.driver.configuration.set_override(flag, value, group)

    def _reset_flags(self):
        self.driver.configuration.local_conf.reset()
        for k, v in self._def_flags.iteritems():
            self._set_flag(k, v)

    def _assert_vol_exists(self, name, exists):
        is_vol_defined = self.driver._is_vdisk_defined(name)
        self.assertEqual(is_vol_defined, exists)

    def test_storwize_svc_connectivity(self):
        # Make sure we detect if the pool doesn't exist
        no_exist_pool = 'i-dont-exist-%s' % random.randint(10000, 99999)
        self._set_flag('storwize_svc_volpool_name', no_exist_pool)
        self.assertRaises(exception.InvalidInput,
                          self.driver.do_setup, None)
        self._reset_flags()

        # Check the case where the user didn't configure IP addresses
        # as well as receiving unexpected results from the storage
        if self.USESIM:
            self.sim.error_injection('lsnodecanister', 'header_mismatch')
            self.assertRaises(exception.VolumeBackendAPIException,
                              self.driver.do_setup, None)
            self.sim.error_injection('lsnodecanister', 'remove_field')
            self.assertRaises(exception.VolumeBackendAPIException,
                              self.driver.do_setup, None)
            self.sim.error_injection('lsportip', 'header_mismatch')
            self.assertRaises(exception.VolumeBackendAPIException,
                              self.driver.do_setup, None)
            self.sim.error_injection('lsportip', 'remove_field')
            self.assertRaises(exception.VolumeBackendAPIException,
                              self.driver.do_setup, None)

        # Check with bad parameters
        self._set_flag('san_ip', '')
        self.assertRaises(exception.InvalidInput,
                          self.driver.check_for_setup_error)
        self._reset_flags()

        self._set_flag('san_password', None)
        self._set_flag('san_private_key', None)
        self.assertRaises(exception.InvalidInput,
                          self.driver.check_for_setup_error)
        self._reset_flags()

        self._set_flag('storwize_svc_vol_rsize', 101)
        self.assertRaises(exception.InvalidInput,
                          self.driver.check_for_setup_error)
        self._reset_flags()

        self._set_flag('storwize_svc_vol_warning', 101)
        self.assertRaises(exception.InvalidInput,
                          self.driver.check_for_setup_error)
        self._reset_flags()

        self._set_flag('storwize_svc_vol_grainsize', 42)
        self.assertRaises(exception.InvalidInput,
                          self.driver.check_for_setup_error)
        self._reset_flags()

        self._set_flag('storwize_svc_flashcopy_timeout', 601)
        self.assertRaises(exception.InvalidInput,
                          self.driver.check_for_setup_error)
        self._reset_flags()

        self._set_flag('storwize_svc_vol_compression', True)
        self._set_flag('storwize_svc_vol_rsize', -1)
        self.assertRaises(exception.InvalidInput,
                          self.driver.check_for_setup_error)
        self._reset_flags()

        self._set_flag('storwize_svc_connection_protocol', 'foo')
        self.assertRaises(exception.InvalidInput,
                          self.driver.check_for_setup_error)
        self._reset_flags()

        self._set_flag('storwize_svc_vol_iogrp', 5)
        self.assertRaises(exception.InvalidInput,
                          self.driver.check_for_setup_error)
        self._reset_flags()

        if self.USESIM:
            self.sim.error_injection('lslicense', 'no_compression')
            self._set_flag('storwize_svc_vol_compression', True)
            self.driver.do_setup(None)
            self.assertRaises(exception.InvalidInput,
                              self.driver.check_for_setup_error)
            self._reset_flags()

        # Finally, check with good parameters
        self.driver.do_setup(None)

    def _generate_vol_info(self, vol_name, vol_id):
        rand_id = str(random.randint(10000, 99999))
        if vol_name:
            return {'name': 'snap_volume%s' % rand_id,
                    'volume_name': vol_name,
                    'id': rand_id,
                    'volume_id': vol_id,
                    'volume_size': 10}
        else:
            return {'name': 'test_volume%s' % rand_id,
                    'size': 10,
                    'id': '%s' % rand_id,
                    'volume_type_id': None}

    def _create_test_vol(self, opts):
        ctxt = context.get_admin_context()
        type_ref = volume_types.create(ctxt, 'testtype', opts)
        volume = self._generate_vol_info(None, None)
        volume['volume_type_id'] = type_ref['id']
        self.driver.create_volume(volume)

        attrs = self.driver._get_vdisk_attributes(volume['name'])
        self.driver.delete_volume(volume)
        volume_types.destroy(ctxt, type_ref['id'])
        return attrs

    def _fail_prepare_fc_map(self, fc_map_id, source, target):
        raise processutils.ProcessExecutionError(exit_code=1,
                                                 stdout='',
                                                 stderr='unit-test-fail',
                                                 cmd='prestartfcmap id')

    def test_storwize_svc_snapshots(self):
        vol1 = self._generate_vol_info(None, None)
        self.driver.create_volume(vol1)
        self.driver.db.volume_set(vol1)
        snap1 = self._generate_vol_info(vol1['name'], vol1['id'])

        # Test timeout and volume cleanup
        self._set_flag('storwize_svc_flashcopy_timeout', 1)
        self.assertRaises(exception.InvalidSnapshot,
                          self.driver.create_snapshot, snap1)
        self._assert_vol_exists(snap1['name'], False)
        self._reset_flags()

        # Test prestartfcmap, startfcmap, and rmfcmap failing
        orig = self.driver._call_prepare_fc_map
        self.driver._call_prepare_fc_map = self._fail_prepare_fc_map
        self.assertRaises(processutils.ProcessExecutionError,
                          self.driver.create_snapshot, snap1)
        self.driver._call_prepare_fc_map = orig

        if self.USESIM:
            self.sim.error_injection('lsfcmap', 'speed_up')
            self.sim.error_injection('startfcmap', 'bad_id')
            self.assertRaises(processutils.ProcessExecutionError,
                              self.driver.create_snapshot, snap1)
            self._assert_vol_exists(snap1['name'], False)
            self.sim.error_injection('prestartfcmap', 'bad_id')
            self.assertRaises(processutils.ProcessExecutionError,
                              self.driver.create_snapshot, snap1)
            self._assert_vol_exists(snap1['name'], False)

        # Test successful snapshot
        self.driver.create_snapshot(snap1)
        self._assert_vol_exists(snap1['name'], True)

        # Try to create a snapshot from an non-existing volume - should fail
        snap_novol = self._generate_vol_info('undefined-vol', '12345')
        self.assertRaises(exception.VolumeNotFound,
                          self.driver.create_snapshot,
                          snap_novol)

        # We support deleting a volume that has snapshots, so delete the volume
        # first
        self.driver.delete_volume(vol1)
        self.driver.delete_snapshot(snap1)

    def test_storwize_svc_create_volfromsnap_clone(self):
        vol1 = self._generate_vol_info(None, None)
        self.driver.create_volume(vol1)
        self.driver.db.volume_set(vol1)
        snap1 = self._generate_vol_info(vol1['name'], vol1['id'])
        self.driver.create_snapshot(snap1)
        vol2 = self._generate_vol_info(None, None)
        vol3 = self._generate_vol_info(None, None)

        # Try to create a volume from a non-existing snapshot
        snap_novol = self._generate_vol_info('undefined-vol', '12345')
        vol_novol = self._generate_vol_info(None, None)
        self.assertRaises(exception.SnapshotNotFound,
                          self.driver.create_volume_from_snapshot,
                          vol_novol,
                          snap_novol)

        # Fail the snapshot
        orig = self.driver._call_prepare_fc_map
        self.driver._call_prepare_fc_map = self._fail_prepare_fc_map
        self.assertRaises(processutils.ProcessExecutionError,
                          self.driver.create_volume_from_snapshot,
                          vol2, snap1)
        self.driver._call_prepare_fc_map = orig
        self._assert_vol_exists(vol2['name'], False)

        # Try to create where source size != target size
        vol2['size'] += 1
        self.assertRaises(exception.VolumeBackendAPIException,
                          self.driver.create_volume_from_snapshot,
                          vol2, snap1)
        self._assert_vol_exists(vol2['name'], False)
        vol2['size'] -= 1

        # Succeed
        if self.USESIM:
            self.sim.error_injection('lsfcmap', 'speed_up')
        self.driver.create_volume_from_snapshot(vol2, snap1)
        self._assert_vol_exists(vol2['name'], True)

        # Try to clone where source size != target size
        vol3['size'] += 1
        self.assertRaises(exception.VolumeBackendAPIException,
                          self.driver.create_cloned_volume,
                          vol3, vol2)
        self._assert_vol_exists(vol3['name'], False)
        vol3['size'] -= 1

        if self.USESIM:
            self.sim.error_injection('lsfcmap', 'speed_up')
        self.driver.create_cloned_volume(vol3, vol2)
        self._assert_vol_exists(vol3['name'], True)

        # Delete in the 'opposite' order to make sure it works
        self.driver.delete_volume(vol3)
        self._assert_vol_exists(vol3['name'], False)
        self.driver.delete_volume(vol2)
        self._assert_vol_exists(vol2['name'], False)
        self.driver.delete_snapshot(snap1)
        self._assert_vol_exists(snap1['name'], False)
        self.driver.delete_volume(vol1)
        self._assert_vol_exists(vol1['name'], False)

    def test_storwize_svc_volumes(self):
        # Create a first volume
        volume = self._generate_vol_info(None, None)
        self.driver.create_volume(volume)

        self.driver.ensure_export(None, volume)

        # Do nothing
        self.driver.create_export(None, volume)
        self.driver.remove_export(None, volume)

        # Make sure volume attributes are as they should be
        attributes = self.driver._get_vdisk_attributes(volume['name'])
        attr_size = float(attributes['capacity']) / (1024 ** 3)  # bytes to GB
        self.assertEqual(attr_size, float(volume['size']))
        pool = self.driver.configuration.local_conf.storwize_svc_volpool_name
        self.assertEqual(attributes['mdisk_grp_name'], pool)

        # Try to create the volume again (should fail)
        self.assertRaises(processutils.ProcessExecutionError,
                          self.driver.create_volume,
                          volume)

        # Try to delete a volume that doesn't exist (should not fail)
        vol_no_exist = {'name': 'i_dont_exist'}
        self.driver.delete_volume(vol_no_exist)
        # Ensure export for volume that doesn't exist (should not fail)
        self.driver.ensure_export(None, vol_no_exist)

        # Delete the volume
        self.driver.delete_volume(volume)

    def test_storwize_svc_volume_params(self):
        # Option test matrix
        # Option        Value   Covered by test #
        # rsize         -1      1
        # rsize         2       2,3
        # warning       0       2
        # warning       80      3
        # autoexpand    True    2
        # autoexpand    False   3
        # grainsize     32      2
        # grainsize     256     3
        # compression   True    4
        # compression   False   2,3
        # easytier      True    1,3
        # easytier      False   2
        # iogrp         0       1
        # iogrp         1       2

        opts_list = []
        chck_list = []
        opts_list.append({'rsize': -1, 'easytier': True, 'iogrp': 0})
        chck_list.append({'free_capacity': '0', 'easy_tier': 'on',
                          'IO_group_id': '0'})
        test_iogrp = 1 if self.USESIM else 0
        opts_list.append({'rsize': 2, 'compression': False, 'warning': 0,
                          'autoexpand': True, 'grainsize': 32,
                          'easytier': False, 'iogrp': test_iogrp})
        chck_list.append({'-free_capacity': '0', 'compressed_copy': 'no',
                          'warning': '0', 'autoexpand': 'on',
                          'grainsize': '32', 'easy_tier': 'off',
                          'IO_group_id': str(test_iogrp)})
        opts_list.append({'rsize': 2, 'compression': False, 'warning': 80,
                          'autoexpand': False, 'grainsize': 256,
                          'easytier': True})
        chck_list.append({'-free_capacity': '0', 'compressed_copy': 'no',
                          'warning': '80', 'autoexpand': 'off',
                          'grainsize': '256', 'easy_tier': 'on'})
        opts_list.append({'rsize': 2, 'compression': True})
        chck_list.append({'-free_capacity': '0',
                          'compressed_copy': 'yes'})

        for idx in range(len(opts_list)):
            attrs = self._create_test_vol(opts_list[idx])
            for k, v in chck_list[idx].iteritems():
                try:
                    if k[0] == '-':
                        k = k[1:]
                        self.assertNotEqual(attrs[k], v)
                    else:
                        self.assertEqual(attrs[k], v)
                except processutils.ProcessExecutionError as e:
                    if 'CMMVC7050E' not in e.stderr:
                        raise

    def test_storwize_svc_unicode_host_and_volume_names(self):
        # We'll check with iSCSI only - nothing protocol-dependednt here
        self._set_flag('storwize_svc_connection_protocol', 'iSCSI')
        self.driver.do_setup(None)

        rand_id = random.randint(10000, 99999)
        volume1 = {'name': u'unicode1_volume%s' % rand_id,
                   'size': 2,
                   'id': 1,
                   'volume_type_id': None}
        self.driver.create_volume(volume1)
        self._assert_vol_exists(volume1['name'], True)

        self.assertRaises(exception.NoValidHost,
                          self.driver._connector_to_hostname_prefix,
                          {'host': 12345})

        # Add a a host first to make life interesting (this host and
        # conn['host'] should be translated to the same prefix, and the
        # initiator should differentiate
        tmpconn1 = {'initiator': u'unicode:initiator1.%s' % rand_id,
                    'ip': '10.10.10.10',
                    'host': u'unicode.foo}.bar{.baz-%s' % rand_id}
        self.driver._create_host(tmpconn1)

        # Add a host with a different prefix
        tmpconn2 = {'initiator': u'unicode:initiator2.%s' % rand_id,
                    'ip': '10.10.10.11',
                    'host': u'unicode.hello.world-%s' % rand_id}
        self.driver._create_host(tmpconn2)

        conn = {'initiator': u'unicode:initiator3.%s' % rand_id,
                'ip': '10.10.10.12',
                'host': u'unicode.foo}.bar}.baz-%s' % rand_id}
        self.driver.initialize_connection(volume1, conn)
        host_name = self.driver._get_host_from_connector(conn)
        self.assertNotEqual(host_name, None)
        self.driver.terminate_connection(volume1, conn)
        host_name = self.driver._get_host_from_connector(conn)
        self.assertEqual(host_name, None)
        self.driver.delete_volume(volume1)

        # Clean up temporary hosts
        for tmpconn in [tmpconn1, tmpconn2]:
            host_name = self.driver._get_host_from_connector(tmpconn)
            self.assertNotEqual(host_name, None)
            self.driver._delete_host(host_name)

    def test_storwize_svc_validate_connector(self):
        conn_neither = {'host': 'host'}
        conn_iscsi = {'host': 'host', 'initiator': 'foo'}
        conn_fc = {'host': 'host', 'wwpns': 'bar'}
        conn_both = {'host': 'host', 'initiator': 'foo', 'wwpns': 'bar'}

        self.driver._enabled_protocols = set(['iSCSI'])
        self.driver.validate_connector(conn_iscsi)
        self.driver.validate_connector(conn_both)
        self.assertRaises(exception.VolumeBackendAPIException,
                          self.driver.validate_connector, conn_fc)
        self.assertRaises(exception.VolumeBackendAPIException,
                          self.driver.validate_connector, conn_neither)

        self.driver._enabled_protocols = set(['FC'])
        self.driver.validate_connector(conn_fc)
        self.driver.validate_connector(conn_both)
        self.assertRaises(exception.VolumeBackendAPIException,
                          self.driver.validate_connector, conn_iscsi)
        self.assertRaises(exception.VolumeBackendAPIException,
                          self.driver.validate_connector, conn_neither)

        self.driver._enabled_protocols = set(['iSCSI', 'FC'])
        self.driver.validate_connector(conn_iscsi)
        self.driver.validate_connector(conn_fc)
        self.driver.validate_connector(conn_both)
        self.assertRaises(exception.VolumeBackendAPIException,
                          self.driver.validate_connector, conn_neither)

    def test_storwize_svc_host_maps(self):
        # Create two volumes to be used in mappings

        ctxt = context.get_admin_context()
        volume1 = self._generate_vol_info(None, None)
        self.driver.create_volume(volume1)
        volume2 = self._generate_vol_info(None, None)
        self.driver.create_volume(volume2)

        # Create volume types that we created
        types = {}
        for protocol in ['FC', 'iSCSI']:
            opts = {'storage_protocol': '<in> ' + protocol}
            types[protocol] = volume_types.create(ctxt, protocol, opts)

        for protocol in ['FC', 'iSCSI']:
            volume1['volume_type_id'] = types[protocol]['id']
            volume2['volume_type_id'] = types[protocol]['id']

            # Check case where no hosts exist
            if self.USESIM:
                ret = self.driver._get_host_from_connector(self._connector)
                self.assertEqual(ret, None)

            # Make sure that the volumes have been created
            self._assert_vol_exists(volume1['name'], True)
            self._assert_vol_exists(volume2['name'], True)

            # Initialize connection from the first volume to a host
            self.driver.initialize_connection(volume1, self._connector)

            # Initialize again, should notice it and do nothing
            self.driver.initialize_connection(volume1, self._connector)

            # Try to delete the 1st volume (should fail because it is mapped)
            self.assertRaises(processutils.ProcessExecutionError,
                              self.driver.delete_volume,
                              volume1)

            # Check bad output from lsfabric for the 2nd volume
            if protocol == 'FC' and self.USESIM:
                for error in ['remove_field', 'header_mismatch']:
                    self.sim.error_injection('lsfabric', error)
                    self.assertRaises(exception.VolumeBackendAPIException,
                                      self.driver.initialize_connection,
                                      volume2, self._connector)

            self.driver.terminate_connection(volume1, self._connector)
            if self.USESIM:
                ret = self.driver._get_host_from_connector(self._connector)
                self.assertEqual(ret, None)

        # Check cases with no auth set for host
        if self.USESIM:
            for auth_enabled in [True, False]:
                for host_exists in ['yes-auth', 'yes-noauth', 'no']:
                    self._set_flag('storwize_svc_iscsi_chap_enabled',
                                   auth_enabled)
                    case = 'en' + str(auth_enabled) + 'ex' + str(host_exists)
                    conn_na = {'initiator': 'test:init:%s' %
                                            random.randint(10000, 99999),
                               'ip': '11.11.11.11',
                               'host': 'host-%s' % case}
                    if host_exists.startswith('yes'):
                        self.sim._add_host_to_list(conn_na)
                        if host_exists == 'yes-auth':
                            kwargs = {'chapsecret': 'foo',
                                      'obj': conn_na['host']}
                            self.sim._cmd_chhost(**kwargs)
                    volume1['volume_type_id'] = types['iSCSI']['id']

                    init_ret = self.driver.initialize_connection(volume1,
                                                                 conn_na)
                    host_name = self.sim._host_in_list(conn_na['host'])
                    chap_ret = self.driver._get_chap_secret_for_host(host_name)
                    if auth_enabled or host_exists == 'yes-auth':
                        self.assertIn('auth_password', init_ret['data'])
                        self.assertNotEqual(chap_ret, None)
                    else:
                        self.assertNotIn('auth_password', init_ret['data'])
                        self.assertEqual(chap_ret, None)
                    self.driver.terminate_connection(volume1, conn_na)
        self._set_flag('storwize_svc_iscsi_chap_enabled', True)

        # Test no preferred node
        if self.USESIM:
            self.sim.error_injection('lsvdisk', 'no_pref_node')
            self.assertRaises(exception.VolumeBackendAPIException,
                              self.driver.initialize_connection,
                              volume1, self._connector)

        # Initialize connection from the second volume to the host with no
        # preferred node set if in simulation mode, otherwise, just
        # another initialize connection.
        if self.USESIM:
            self.sim.error_injection('lsvdisk', 'blank_pref_node')
        self.driver.initialize_connection(volume2, self._connector)

        # Try to remove connection from host that doesn't exist (should fail)
        conn_no_exist = self._connector.copy()
        conn_no_exist['initiator'] = 'i_dont_exist'
        conn_no_exist['wwpns'] = ['0000000000000000']
        self.assertRaises(exception.VolumeBackendAPIException,
                          self.driver.terminate_connection,
                          volume1,
                          conn_no_exist)

        # Try to remove connection from volume that isn't mapped (should print
        # message but NOT fail)
        unmapped_vol = self._generate_vol_info(None, None)
        self.driver.create_volume(unmapped_vol)
        self.driver.terminate_connection(unmapped_vol, self._connector)
        self.driver.delete_volume(unmapped_vol)

        # Remove the mapping from the 1st volume and delete it
        self.driver.terminate_connection(volume1, self._connector)
        self.driver.delete_volume(volume1)
        self._assert_vol_exists(volume1['name'], False)

        # Make sure our host still exists
        host_name = self.driver._get_host_from_connector(self._connector)
        self.assertNotEqual(host_name, None)

        # Remove the mapping from the 2nd volume. The host should
        # be automatically removed because there are no more mappings.
        self.driver.terminate_connection(volume2, self._connector)

        # Check if we successfully terminate connections when the host is not
        # specified (see bug #1244257)
        fake_conn = {'ip': '127.0.0.1', 'initiator': 'iqn.fake'}
        self.driver.initialize_connection(volume2, self._connector)
        host_name = self.driver._get_host_from_connector(self._connector)
        self.assertIsNotNone(host_name)
        self.driver.terminate_connection(volume2, fake_conn)
        host_name = self.driver._get_host_from_connector(self._connector)
        self.assertIsNone(host_name)
        self.driver.delete_volume(volume2)
        self._assert_vol_exists(volume2['name'], False)

        # Delete volume types that we created
        for protocol in ['FC', 'iSCSI']:
            volume_types.destroy(ctxt, types[protocol]['id'])

        # Check if our host still exists (it should not)
        if self.USESIM:
            ret = self.driver._get_host_from_connector(self._connector)
            self.assertEqual(ret, None)

    def test_storwize_svc_multi_host_maps(self):
        # We can't test connecting to multiple hosts from a single host when
        # using real storage
        if not self.USESIM:
            return

        # Create a volume to be used in mappings
        ctxt = context.get_admin_context()
        volume = self._generate_vol_info(None, None)
        self.driver.create_volume(volume)

        # Create volume types for protocols
        types = {}
        for protocol in ['FC', 'iSCSI']:
            opts = {'storage_protocol': '<in> ' + protocol}
            types[protocol] = volume_types.create(ctxt, protocol, opts)

        # Create a connector for the second 'host'
        wwpns = [str(random.randint(0, 9999999999999999)).zfill(16),
                 str(random.randint(0, 9999999999999999)).zfill(16)]
        initiator = 'test.initiator.%s' % str(random.randint(10000, 99999))
        conn2 = {'ip': '1.234.56.79',
                 'host': 'storwize-svc-test2',
                 'wwpns': wwpns,
                 'initiator': initiator}

        for protocol in ['FC', 'iSCSI']:
            volume['volume_type_id'] = types[protocol]['id']

            # Make sure that the volume has been created
            self._assert_vol_exists(volume['name'], True)

            self.driver.initialize_connection(volume, self._connector)

            self._set_flag('storwize_svc_multihostmap_enabled', False)
            self.assertRaises(exception.CinderException,
                              self.driver.initialize_connection, volume, conn2)

            self._set_flag('storwize_svc_multihostmap_enabled', True)
            self.driver.initialize_connection(volume, conn2)

            self.driver.terminate_connection(volume, conn2)
            self.driver.terminate_connection(volume, self._connector)

    def test_storwize_svc_delete_volume_snapshots(self):
        # Create a volume with two snapshots
        master = self._generate_vol_info(None, None)
        self.driver.create_volume(master)
        self.driver.db.volume_set(master)

        # Fail creating a snapshot - will force delete the snapshot
        if self.USESIM and False:
            snap = self._generate_vol_info(master['name'], master['id'])
            self.sim.error_injection('startfcmap', 'bad_id')
            self.assertRaises(processutils.ProcessExecutionError,
                              self.driver.create_snapshot, snap)
            self._assert_vol_exists(snap['name'], False)

        # Delete a snapshot
        snap = self._generate_vol_info(master['name'], master['id'])
        self.driver.create_snapshot(snap)
        self._assert_vol_exists(snap['name'], True)
        self.driver.delete_snapshot(snap)
        self._assert_vol_exists(snap['name'], False)

        # Delete a volume with snapshots (regular)
        snap = self._generate_vol_info(master['name'], master['id'])
        self.driver.create_snapshot(snap)
        self._assert_vol_exists(snap['name'], True)
        self.driver.delete_volume(master)
        self._assert_vol_exists(master['name'], False)

        # Fail create volume from snapshot - will force delete the volume
        if self.USESIM:
            volfs = self._generate_vol_info(None, None)
            self.sim.error_injection('startfcmap', 'bad_id')
            self.sim.error_injection('lsfcmap', 'speed_up')
            self.assertRaises(processutils.ProcessExecutionError,
                              self.driver.create_volume_from_snapshot,
                              volfs, snap)
            self._assert_vol_exists(volfs['name'], False)

        # Create volume from snapshot and delete it
        volfs = self._generate_vol_info(None, None)
        if self.USESIM:
            self.sim.error_injection('lsfcmap', 'speed_up')
        self.driver.create_volume_from_snapshot(volfs, snap)
        self._assert_vol_exists(volfs['name'], True)
        self.driver.delete_volume(volfs)
        self._assert_vol_exists(volfs['name'], False)

        # Create volume from snapshot and delete the snapshot
        volfs = self._generate_vol_info(None, None)
        if self.USESIM:
            self.sim.error_injection('lsfcmap', 'speed_up')
        self.driver.create_volume_from_snapshot(volfs, snap)
        self.driver.delete_snapshot(snap)
        self._assert_vol_exists(snap['name'], False)

        # Fail create clone - will force delete the target volume
        if self.USESIM:
            clone = self._generate_vol_info(None, None)
            self.sim.error_injection('startfcmap', 'bad_id')
            self.sim.error_injection('lsfcmap', 'speed_up')
            self.assertRaises(processutils.ProcessExecutionError,
                              self.driver.create_cloned_volume,
                              clone, volfs)
            self._assert_vol_exists(clone['name'], False)

        # Create the clone, delete the source and target
        clone = self._generate_vol_info(None, None)
        if self.USESIM:
            self.sim.error_injection('lsfcmap', 'speed_up')
        self.driver.create_cloned_volume(clone, volfs)
        self._assert_vol_exists(clone['name'], True)
        self.driver.delete_volume(volfs)
        self._assert_vol_exists(volfs['name'], False)
        self.driver.delete_volume(clone)
        self._assert_vol_exists(clone['name'], False)

    # Note defined in python 2.6, so define here...
    def assertLessEqual(self, a, b, msg=None):
        if not a <= b:
            self.fail('%s not less than or equal to %s' % (repr(a), repr(b)))

    def test_storwize_svc_get_volume_stats(self):
        self._set_flag('reserved_percentage', 25)
        stats = self.driver.get_volume_stats()
        self.assertLessEqual(stats['free_capacity_gb'],
                             stats['total_capacity_gb'])
        self.assertEqual(stats['reserved_percentage'], 25)
        pool = self.driver.configuration.local_conf.storwize_svc_volpool_name
        if self.USESIM:
            expected = 'storwize-svc-sim_' + pool
            self.assertEqual(stats['volume_backend_name'], expected)
            self.assertAlmostEqual(stats['total_capacity_gb'], 3328.0)
            self.assertAlmostEqual(stats['free_capacity_gb'], 3287.5)

    def test_storwize_svc_extend_volume(self):
        volume = self._generate_vol_info(None, None)
        self.driver.db.volume_set(volume)
        self.driver.create_volume(volume)
        stats = self.driver.extend_volume(volume, '13')
        attrs = self.driver._get_vdisk_attributes(volume['name'])
        vol_size = int(attrs['capacity']) / units.GiB
        self.assertAlmostEqual(vol_size, 13)

        snap = self._generate_vol_info(volume['name'], volume['id'])
        self.driver.create_snapshot(snap)
        self._assert_vol_exists(snap['name'], True)
        self.assertRaises(exception.VolumeBackendAPIException,
                          self.driver.extend_volume, volume, '16')

        self.driver.delete_snapshot(snap)
        self.driver.delete_volume(volume)

    def _check_loc_info(self, capabilities, expected):
        host = {'host': 'foo', 'capabilities': capabilities}
        vol = {'name': 'test', 'id': 1, 'size': 1}
        ctxt = context.get_admin_context()
        moved, model_update = self.driver.migrate_volume(ctxt, vol, host)
        self.assertEqual(moved, expected['moved'])
        self.assertEqual(model_update, expected['model_update'])

    def test_storwize_svc_migrate_bad_loc_info(self):
        self._check_loc_info({}, {'moved': False, 'model_update': None})
        cap = {'location_info': 'foo'}
        self._check_loc_info(cap, {'moved': False, 'model_update': None})
        cap = {'location_info': 'FooDriver:foo:bar'}
        self._check_loc_info(cap, {'moved': False, 'model_update': None})
        cap = {'location_info': 'StorwizeSVCDriver:foo:bar'}
        self._check_loc_info(cap, {'moved': False, 'model_update': None})

    def test_storwize_svc_migrate_same_extent_size(self):
        def _copy_info_exc(self, name):
            raise Exception('should not be called')

        self.stubs.Set(self.driver, '_get_vdisk_copy_info', _copy_info_exc)
        self.driver.do_setup(None)
        loc = 'StorwizeSVCDriver:' + self.driver._system_id + ':openstack2'
        cap = {'location_info': loc, 'extent_size': '256'}
        host = {'host': 'foo', 'capabilities': cap}
        ctxt = context.get_admin_context()
        volume = self._generate_vol_info(None, None)
        volume['volume_type_id'] = None
        self.driver.create_volume(volume)
        self.driver.migrate_volume(ctxt, volume, host)
        self.driver.delete_volume(volume)

    def test_storwize_svc_migrate_diff_extent_size(self):
        self.driver.do_setup(None)
        loc = 'StorwizeSVCDriver:' + self.driver._system_id + ':openstack3'
        cap = {'location_info': loc, 'extent_size': '128'}
        host = {'host': 'foo', 'capabilities': cap}
        ctxt = context.get_admin_context()
        volume = self._generate_vol_info(None, None)
        volume['volume_type_id'] = None
        self.driver.create_volume(volume)
        self.assertNotEquals(cap['extent_size'], self.driver._extent_size)
        self.driver.migrate_volume(ctxt, volume, host)
        self.driver.delete_volume(volume)


class CLIResponseTestCase(test.TestCase):
    def test_empty(self):
        self.assertEqual(0, len(storwize_svc.CLIResponse('')))
        self.assertEqual(0, len(storwize_svc.CLIResponse(('', 'stderr'))))

    def test_header(self):
        raw = r'''id!name
1!node1
2!node2
'''
        resp = storwize_svc.CLIResponse(raw, with_header=True)
        self.assertEqual(2, len(resp))
        self.assertEqual('1', resp[0]['id'])
        self.assertEqual('2', resp[1]['id'])

    def test_select(self):
        raw = r'''id!123
name!Bill
name!Bill2
age!30
home address!s1
home address!s2

id! 7
name!John
name!John2
age!40
home address!s3
home address!s4
'''
        resp = storwize_svc.CLIResponse(raw, with_header=False)
        self.assertEqual(list(resp.select('home address', 'name',
                                          'home address')),
                         [('s1', 'Bill', 's1'), ('s2', 'Bill2', 's2'),
                          ('s3', 'John', 's3'), ('s4', 'John2', 's4')])

    def test_lsnode_all(self):
        raw = r'''id!name!UPS_serial_number!WWNN!status
1!node1!!500507680200C744!online
2!node2!!500507680200C745!online
'''
        resp = storwize_svc.CLIResponse(raw)
        self.assertEqual(2, len(resp))
        self.assertEqual('1', resp[0]['id'])
        self.assertEqual('500507680200C744', resp[0]['WWNN'])
        self.assertEqual('2', resp[1]['id'])
        self.assertEqual('500507680200C745', resp[1]['WWNN'])

    def test_lsnode_single(self):
        raw = r'''id!1
port_id!500507680210C744
port_status!active
port_speed!8Gb
port_id!500507680240C744
port_status!inactive
port_speed!8Gb
'''
        resp = storwize_svc.CLIResponse(raw, with_header=False)
        self.assertEqual(1, len(resp))
        self.assertEqual('1', resp[0]['id'])
        self.assertEqual(list(resp.select('port_id', 'port_status')),
                         [('500507680210C744', 'active'),
                          ('500507680240C744', 'inactive')])

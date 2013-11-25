# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2012 NetApp, Inc.
# Copyright (c) 2012 OpenStack Foundation
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
"""
Utilities for NetApp drivers.

This module contains common utilities to be used by one or more
NetApp drivers to achieve the desired functionality.
"""

import base64
import binascii
import socket
import uuid

from cinder.openstack.common import log as logging
from cinder.openstack.common import lockutils


LOG = logging.getLogger(__name__)


@lockutils.synchronized("safe_set_attr", "cinder-")
def set_safe_attr(instance, attr, val):
    """Sets the attribute in a thread safe manner.

    Returns if new val was set on attribute.
    If attr already had the value then False.
    """

    if not instance or not attr:
        return False
    old_val = getattr(instance, attr, None)
    if val is None and old_val is None:
        return False
    elif val == old_val:
        return False
    else:
        setattr(instance, attr, val)
        return True


def resolve_hostname(hostname):
    """Resolves host name to IP address."""
    res = socket.getaddrinfo(hostname, None)[0]
    family, socktype, proto, canonname, sockaddr = res
    return sockaddr[0]


def encode_hex_to_base32(hex_string):
    """Encodes hex to base32 bit as per RFC4648."""
    bin_form = binascii.unhexlify(hex_string)
    return base64.b32encode(bin_form)


def decode_base32_to_hex(base32_string):
    """Decodes base32 string to hex string."""
    bin_form = base64.b32decode(base32_string)
    return binascii.hexlify(bin_form)


def convert_uuid_to_es_fmt(uuid_str):
    """Converts uuid to e-series compatible name format."""
    uuid_base32 = encode_hex_to_base32(uuid.UUID(str(uuid_str)).hex)
    return uuid_base32.strip('=')


def convert_es_fmt_to_uuid(es_label):
    """Converts e-series name format to uuid."""
    es_label_b32 = es_label.ljust(32, '=')
    return uuid.UUID(binascii.hexlify(base64.b32decode(es_label_b32)))

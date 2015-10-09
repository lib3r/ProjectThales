# Copyright 2015 Google Inc. All Rights Reserved.
"""instance-groups unmanaged describe command.

It's an alias for the instance-groups describe command.
"""
from googlecloudsdk.compute.lib import instance_groups_utils


class Describe(instance_groups_utils.InstanceGroupDescribe):
  pass

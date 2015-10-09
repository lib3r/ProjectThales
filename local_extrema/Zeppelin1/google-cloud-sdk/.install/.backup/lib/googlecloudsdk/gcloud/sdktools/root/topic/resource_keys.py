# Copyright 2014 Google Inc. All Rights Reserved.

"""Resource keys supplementary help."""

import textwrap

from googlecloudsdk.calliope import base


class Keys(base.Command):
  """Resource keys supplementary help."""

  def Run(self, args):
    self.cli.Execute(args.command_path[1:] + ['--document=style=topic'])
    return None

  detailed_help = {

      'DESCRIPTION': textwrap.dedent("""\
          A *resource* is a JSON-serializable object, protobuf, or other
          reflective data object. Resource data is organized as a tree,
          where each node in the tree may be a scalar, indexed array, or
          dictionary (associative array).  Programming language structs,
          classes and methods are modeled as dictionaries indexed by
          their member names.

          Each node, except the root, has a name, and is reachable by a
          unique path of node names from the root. The ordered list of
          node names, separated by '.', from the root to a node is the
          node data *key*.

          The resource keys and data values for any _gcloud_ _list_
          command can be printed by running _gcloud_ ... _list_
          *--format=flattened*. See the command specific documentation for
          details on specific resource keys.
          """),

      'EXAMPLES': textwrap.dedent("""\
          This command lists the keys and values for the *regions* resource:

            $ gcloud compute regions list --format=flattened

          and here is sample output for the command:

            ---
            creationTimestamp: 2013-05-23T07:02:09.522-07:00
            description:       us-central1
            id:                22115839677829654
            kind:              compute#region
            name:              us-central1
            quotas[0].limit:   24.0
            quotas[0].metric:  CPUS
            quotas[0].usage:   15.0
            quotas[1].limit:   5120.0
            quotas[1].metric:  DISKS_TOTAL_GB
            quotas[1].usage:   1416.0
            quotas[2].limit:   7.0
            quotas[2].metric:  STATIC_ADDRESSES
            quotas[2].usage:   1.0
            quotas[3].limit:   23.0
            quotas[3].metric:  IN_USE_ADDRESSES
            quotas[3].usage:   16.0
            quotas[4].limit:   1024.0
            quotas[4].metric:  SSD_TOTAL_GB
            quotas[4].usage:   0.0
            quotas[5].limit:   1500.0
            quotas[5].metric:  LOCAL_SSD_TOTAL_GB
            quotas[5].usage:   750.0
            selfLink:          https://www.googleapis.com/.../us-central1
            status:            UP
            zones[0]:          us-central1-a
            zones[1]:          us-central1-b
            zones[2]:          us-central1-f

          The *list* command produces a resource list. The keys are to the
          left of ':' and the values are to the right.
          """),
      }

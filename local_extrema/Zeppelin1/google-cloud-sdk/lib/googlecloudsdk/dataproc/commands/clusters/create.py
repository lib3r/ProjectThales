# Copyright 2015 Google Inc. All Rights Reserved.

"""Create cluster command."""

import argparse

from googlecloudsdk.calliope import actions
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.core import log
from googlecloudsdk.core import properties

from googlecloudsdk.dataproc.lib import compute_helpers
from googlecloudsdk.dataproc.lib import util


class Create(base.Command):
  """Create a cluster."""

  detailed_help = {
      'DESCRIPTION': '{description}',
      'EXAMPLES': """\
          To create a cluster, run:

            $ {command} my_cluster
      """
  }

  @staticmethod
  def Args(parser):
    parser.add_argument('name', help='The name of this cluster.')
    parser.add_argument(
        '--num-workers',
        type=int,
        help='The number of worker nodes in the cluster. Defaults to '
        'server-specified.')
    parser.add_argument(
        '--num-preemptible-workers',
        type=int,
        help='The number of preemptible worker nodes in the cluster.')
    parser.add_argument(
        '--master-machine-type',
        help='The type of machine to use for the master. Defaults to '
        'server-specified.')
    parser.add_argument(
        '--worker-machine-type',
        help='The type of machine to use for workers. Defaults to '
        'server-specified.')
    parser.add_argument('--image', help=argparse.SUPPRESS)
    parser.add_argument(
        '--bucket',
        help='The GCS bucket to use with the GCS connector. A bucket is auto '
        'created when this parameter is not specified.')
    parser.add_argument(
        '--network',
        help='The Compute Engine network that the cluster will connect to. '
        'Google Cloud Dataproc will use this network when creating routes '
        'and firewalls for the clusters. Defaults to the \'default\' network.')
    parser.add_argument(
        '--zone', '-z',
        help='The compute zone (e.g. us-central1-a) for the cluster.',
        action=actions.StoreProperty(properties.VALUES.compute.zone))
    parser.add_argument(
        '--num-worker-local-ssds',
        type=int,
        help='The number of local SSDs to attach to each worker in a cluster.')
    parser.add_argument(
        '--num-master-local-ssds',
        type=int,
        help='The number of local SSDs to attach to the master in a cluster.')
    parser.add_argument(
        '--worker-boot-disk-size-gb',
        type=int,
        help='The size in GB of the boot disk of each worker in a cluster.')
    parser.add_argument(
        '--master-boot-disk-size-gb',
        type=int,
        help='The size in GB of the boot disk of the master in a cluster.')
    parser.add_argument(
        '--initialization-actions',
        type=arg_parsers.ArgList(),
        metavar='GCS_URI',
        help=('A list of Google Cloud Storage URIs of '
              'executables to run on each node in the cluster.'))
    parser.add_argument(
        '--initialization-action-timeout',
        type=arg_parsers.Duration(),
        metavar='TIMEOUT',
        default='10m',
        help='The maximum duration of each initialization action.')
    # TODO(user): Add scope flag.

  @util.HandleHttpError
  def Run(self, args):
    client = self.context['dataproc_client']
    messages = self.context['dataproc_messages']

    cluster_ref = util.ParseCluster(args.name, self.context)

    config_helper = compute_helpers.ConfigurationHelper.FromContext(
        self.context)
    compute_uris = config_helper.ResolveGceUris(
        args.name,
        args.image,
        args.master_machine_type,
        args.worker_machine_type,
        args.network)

    init_actions = []
    timeout_str = str(args.initialization_action_timeout) + 's'
    if args.initialization_actions:
      init_actions = [messages.NodeInitializationAction(
          executableFile=exe, executionTimeout=timeout_str)
                      for exe in args.initialization_actions]

    cluster_config = messages.ClusterConfiguration(
        configurationBucket=args.bucket,
        gceClusterConfiguration=messages.GceClusterConfiguration(
            networkUri=compute_uris['network'],
            zoneUri=compute_uris['zone'],
        ),
        masterConfiguration=messages.InstanceGroupConfiguration(
            imageUri=compute_uris['image'],
            machineTypeUri=compute_uris['master_machine_type'],
            diskConfiguration=messages.DiskConfiguration(
                bootDiskSizeGb=args.master_boot_disk_size_gb,
                numLocalSsds=args.num_master_local_ssds,
            ),
        ),
        workerConfiguration=messages.InstanceGroupConfiguration(
            numInstances=args.num_workers,
            imageUri=compute_uris['image'],
            machineTypeUri=compute_uris['worker_machine_type'],
            diskConfiguration=messages.DiskConfiguration(
                bootDiskSizeGb=args.worker_boot_disk_size_gb,
                numLocalSsds=args.num_worker_local_ssds,
            ),
        ),
        initializationActions=init_actions,
    )

    # Secondary worker group is optional.
    if args.num_preemptible_workers is not None:
      cluster_config.secondaryWorkerConfiguration = (
          messages.InstanceGroupConfiguration(
              numInstances=args.num_preemptible_workers))

    cluster = messages.Cluster(
        configuration=cluster_config,
        clusterName=cluster_ref.clusterName,
        projectId=cluster_ref.projectId)

    operation = client.projects_clusters.Create(cluster)
    operation = util.WaitForOperation(
        operation, self.context, 'Waiting for cluster creation operation')

    cluster = client.projects_clusters.Get(cluster_ref.Request())
    if cluster.status.state == (
        messages.ClusterStatus.StateValueValuesEnum.RUNNING):
      log.CreatedResource(cluster_ref)
    else:
      log.error('Create cluster failed!')
      if operation.details:
        log.error('Details:\n' + operation.details)
    return cluster

  def Display(self, args, result):
    self.format(result)

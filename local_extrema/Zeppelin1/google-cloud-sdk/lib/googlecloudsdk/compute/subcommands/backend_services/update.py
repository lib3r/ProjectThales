# Copyright 2014 Google Inc. All Rights Reserved.
"""Commands for updating backend services.

   There are separate alpha, beta, and GA command classes in this file.  The
   key differences are that each track passes different message modules for
   inferring options to --balancing-mode, and to enable or disable support for
   https load balancing.
"""

import copy

from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.third_party.apis.compute.alpha import compute_alpha_messages
from googlecloudsdk.third_party.apis.compute.beta import compute_beta_messages
from googlecloudsdk.third_party.apis.compute.v1 import compute_v1_messages

from googlecloudsdk.compute.lib import backend_services_utils
from googlecloudsdk.compute.lib import base_classes


def _Args(parser, messages, include_https_health_checks):
  """Common arguments to create commands for each release track."""
  backend_services_utils.AddUpdatableArgs(
      parser,
      messages,
      include_https_health_checks,
      default_protocol=None,
      default_timeout=None)

  parser.add_argument(
      'name',
      help='The name of the backend service to update.')


@base.ReleaseTracks(base.ReleaseTrack.GA)
class UpdateGA(base_classes.ReadWriteCommand):
  """Update a backend service."""

  @staticmethod
  def Args(parser):
    _Args(parser, compute_v1_messages, include_https_health_checks=False)

  @property
  def service(self):
    return self.compute.backendServices

  @property
  def resource_type(self):
    return 'backendServices'

  def CreateReference(self, args):
    return self.CreateGlobalReference(args.name)

  def GetGetRequest(self, args):
    return (
        self.service,
        'Get',
        self.messages.ComputeBackendServicesGetRequest(
            project=self.project,
            backendService=self.ref.Name()))

  def GetSetRequest(self, args, replacement, _):
    return (
        self.service,
        'Update',
        self.messages.ComputeBackendServicesUpdateRequest(
            project=self.project,
            backendService=self.ref.Name(),
            backendServiceResource=replacement))

  def Modify(self, args, existing):
    replacement = copy.deepcopy(existing)

    if args.description:
      replacement.description = args.description
    elif args.description is not None:
      replacement.description = None

    health_checks = backend_services_utils.GetHealthChecks(args, self)
    if health_checks:
      replacement.healthChecks = health_checks

    if args.timeout:
      replacement.timeoutSec = args.timeout

    if args.port:
      replacement.port = args.port

    if args.port_name:
      replacement.portName = args.port_name

    if args.protocol:
      replacement.protocol = (self.messages.BackendService
                              .ProtocolValueValuesEnum(args.protocol))

    return replacement

  def Run(self, args):
    if not any([
        args.protocol,
        args.description is not None,
        args.http_health_checks,
        getattr(args, 'https_health_checks', None),
        args.timeout is not None,
        args.port,
        args.port_name,
    ]):
      raise exceptions.ToolException('At least one property must be modified.')

    return super(UpdateGA, self).Run(args)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class UpdateAlpha(UpdateGA):
  """Update a backend service."""

  @staticmethod
  def Args(parser):
    _Args(parser, compute_alpha_messages, include_https_health_checks=True)

    enable_caching = parser.add_argument(
        '--enable-caching',
        action='store_true',
        default=None,  # Tri-valued, None => don't change the setting.
        help='Cache GET request responses.')
    enable_caching.detailed_help = """\
        Cache GET request responses, subject to space availability and to the
        control of any cache-control headers in the response, as specified in
        RFC 7234.
        """

  def Modify(self, args, existing):
    replacement = super(UpdateAlpha, self).Modify(args, existing)

    if args.enable_caching is not None:
      replacement.enableCaching = args.enable_caching

    return replacement

  def Run(self, args):
    if not any([
        args.protocol,
        args.description is not None,
        args.http_health_checks,
        getattr(args, 'https_health_checks', None),
        args.timeout is not None,
        args.port,
        args.port_name,
        args.enable_caching is not None,
    ]):
      raise exceptions.ToolException('At least one property must be modified.')

    return super(UpdateGA, self).Run(args)


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class UpdateBeta(UpdateGA):
  """Update a backend service."""

  @staticmethod
  def Args(parser):
    _Args(parser, compute_beta_messages, include_https_health_checks=True)


UpdateGA.detailed_help = {
    'brief': 'Update a backend service',
    'DESCRIPTION': """
        *{command}* is used to update backend services.
        """,
}
UpdateAlpha.detailed_help = UpdateGA.detailed_help
UpdateBeta.detailed_help = UpdateGA.detailed_help

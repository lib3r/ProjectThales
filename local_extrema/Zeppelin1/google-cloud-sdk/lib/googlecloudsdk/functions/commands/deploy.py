# Copyright 2015 Google Inc. All Rights Reserved.

"""'functions deploy' command."""

import httplib
import os
import random
import string

from googlecloudsdk.calliope import base
from googlecloudsdk.core import properties
from googlecloudsdk.core.util import archive
from googlecloudsdk.core.util import files as file_utils
from googlecloudsdk.third_party.apitools.base import py as apitools_base

from googlecloudsdk.functions.lib import cloud_storage as storage
from googlecloudsdk.functions.lib import exceptions
from googlecloudsdk.functions.lib import operations
from googlecloudsdk.functions.lib import util


class Deploy(base.Command):
  """Creates a new function or updates an existing one."""

  @staticmethod
  def Args(parser):
    """Register flags for this command."""
    parser.add_argument(
        'name', help='Intended name of the new function.',
        type=util.ParseFunctionName)
    parser.add_argument(
        '--source', default='.',
        help='Path to directory with source code.',
        type=util.ParseDirectory)
    parser.add_argument(
        '--bucket', required=True,
        help='Name of GCS bucket in which source code will be stored.',
        type=util.ParseBucketUri)
    parser.add_argument(
        '--entry-point',
        help=('The name of the function (as defined in source code) that will '
              'be executed.'),
        type=util.ParseEntryPointName)
    trigger_group = parser.add_mutually_exclusive_group(required=True)
    trigger_group.add_argument(
        '--trigger-topic',
        help=('Name of Pub/Sub topic. Every message published in this topic '
              'will trigger function execution with message contents passed as '
              'input data.'),
        type=util.ParsePubsubTopicName)
    trigger_group.add_argument(
        '--trigger-gs-uri',
        help=('GCS bucket name. Every change in files in this bucket will '
              'trigger function execution.'),
        type=util.ParseBucketUri)

  @util.CatchHTTPErrorRaiseHTTPException
  def _GetExistingFunction(self, name):
    client = self.context['functions_client']
    messages = self.context['functions_messages']
    try:
      # TODO(user): Use resources.py here after b/21908671 is fixed.
      # We got response for a get request so a function exists.
      return client.projects_regions_functions.Get(
          messages.CloudfunctionsProjectsRegionsFunctionsGetRequest(name=name))
    except apitools_base.HttpError as error:
      if error.status_code == httplib.NOT_FOUND:
        # The function has not been found.
        return None
      raise

  def _GenerateBucketName(self, args):
    return args.bucket

  def _GenerateFileName(self, args):
    sufix = ''.join(random.choice(string.ascii_lowercase) for _ in range(12))
    return '{0}-{1}-{2}'.format(args.region, args.name, sufix)

  def _UploadFile(self, source, target):
    return storage.Upload(source, target)

  def _CreateZipFile(self, tmp_dir, args):
    zip_file_name = os.path.join(tmp_dir, 'fun.zip')
    archive.MakeZipFromDir(zip_file_name, args.source)
    return zip_file_name

  def _GenerateFunction(self, name, gcs_url, args):
    """Creates a function object.

    Args:
      name: funciton name
      gcs_url: the location of the code
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      The specified function with its description and configured filter.
    """
    messages = self.context['functions_messages']
    trigger = messages.FunctionTrigger()
    if args.trigger_topic:
      trigger.pubsubTopic = args.trigger_topic
    if args.trigger_gs_uri:
      trigger.gsUri = args.trigger_gs_uri
    function = messages.HostedFunction()
    function.name = name
    if args.entry_point:
      function.entryPoint = args.entry_point
    function.gcsUrl = gcs_url
    function.triggers = [trigger]
    return function

  def _DeployFunction(self, name, location, args, deploy_method):
    remote_zip_file = self._GenerateFileName(args)
    bucket_name = self._GenerateBucketName(args)
    gcs_url = storage.BuildRemoteDestination(bucket_name, remote_zip_file)
    function = self._GenerateFunction(name, gcs_url, args)
    with file_utils.TemporaryDirectory() as tmp_dir:
      zip_file = self._CreateZipFile(tmp_dir, args)
      if self._UploadFile(zip_file, gcs_url) != 0:
        raise exceptions.FunctionsError('Function upload failed.')
    return deploy_method(location, function)

  @util.CatchHTTPErrorRaiseHTTPException
  def _CreateFunction(self, location, function):
    client = self.context['functions_client']
    messages = self.context['functions_messages']
    # TODO(user): Use resources.py here after b/21908671 is fixed.
    op = client.projects_regions_functions.Create(
        messages.CloudfunctionsProjectsRegionsFunctionsCreateRequest(
            location=location, hostedFunction=function))
    operations.Wait(op, messages, client)
    return self._GetExistingFunction(function.name)

  @util.CatchHTTPErrorRaiseHTTPException
  def _UpdateFunction(self, unused_location, function):
    client = self.context['functions_client']
    messages = self.context['functions_messages']
    # TODO(user): Use resources.py here after b/21908671 is fixed.
    op = client.projects_regions_functions.Update(function)
    operations.Wait(op, messages, client)
    return self._GetExistingFunction(function.name)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      The specified function with its description and configured filter.
    """
    project = properties.VALUES.core.project.Get(required=True)
    location = 'projects/{0}/regions/{1}'.format(project, args.region)
    name = 'projects/{0}/regions/{1}/functions/{2}'.format(
        project, args.region, args.name)

    function = self._GetExistingFunction(name)
    if function is None:
      return self._DeployFunction(name, location, args, self._CreateFunction)
    else:
      return self._DeployFunction(name, location, args, self._UpdateFunction)

  def Display(self, unused_args, result):
    """This method is called to print the result of the Run() method.

    Args:
      unused_args: The arguments that command was run with.
      result: The value returned from the Run() method.
    """
    self.format(result)

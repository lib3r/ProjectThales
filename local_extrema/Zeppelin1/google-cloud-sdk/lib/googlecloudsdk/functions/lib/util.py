# Copyright 2015 Google Inc. All Rights Reserved.

"""A library that is used to support Functions commands."""

import functools
import json
import os
import re
import sys

from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import exceptions as base_exceptions
from googlecloudsdk.core import properties
from googlecloudsdk.third_party.apitools.base import py as apitools_base

_ENTRY_POINT_NAME_RE = re.compile(r'^[_a-zA-Z0-9]{1,128}$')
_FUNCTION_NAME_RE = re.compile(r'^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$')
_TOPIC_NAME_RE = re.compile(r'^[a-zA-Z][\-\._~%\+a-zA-Z0-9]{2,254}$')
_BUCKET_URI_RE = re.compile(
    r'^gs://[a-z\d][a-z\d\._-]{1,230}[a-z\d]/$')


def GetHttpErrorMessage(error):
  """Returns a human readable string representation from the http response.

  Args:
    error: HttpException representing the error response.

  Returns:
    A human readable string representation of the error.
  """
  status = error.response.status
  code = error.response.reason
  message = ''
  try:
    data = json.loads(error.content)
  except ValueError:
    data = error.content

  if 'error' in data:
    try:
      error_info = data['error']
      if 'message' in error_info:
        message = error_info['message']
    except (ValueError, TypeError):
      message = data
    violations = _GetViolationsFromError(error_info)
    if violations:
      message += '\nProblems:\n' + violations
  else:
    message = data
  return 'ResponseError: status=[{0}], code=[{1}], message=[{2}]'.format(
      status, code, message)


def GetOperationError(error):
  """Returns a human readable string representation from the operation.

  Args:
    error: A string representing the raw json of the operation error.

  Returns:
    A human readable string representation of the error.
  """
  return 'OperationError: code={0}, message={1}'.format(
      error.code, error.message)


def ParseFunctionName(name):
  """Checks if a function name provided by user is valid.

  Args:
    name: Function name provided by user.
  Returns:
    Function name.
  Raises:
    ArgumentTypeError: If the name provided by user is not valid.
  """
  match = _FUNCTION_NAME_RE.match(name)
  if not match:
    raise arg_parsers.ArgumentTypeError(
        'Function name must match [{0}]'.format(_FUNCTION_NAME_RE.pattern))
  return name


def ParseEntryPointName(entry_point):
  """Checks if a entry point name provided by user is valid.

  Args:
    entry_point: Entry point name provided by user.
  Returns:
    Entry point name.
  Raises:
    ArgumentTypeError: If the entry point name provided by user is not valid.
  """
  match = _ENTRY_POINT_NAME_RE.match(entry_point)
  if not match:
    raise arg_parsers.ArgumentTypeError(
        'Entry point name must match [{0}]'.format(
            _ENTRY_POINT_NAME_RE.pattern))
  return entry_point


def ParseBucketUri(bucket):
  """Checks if a bucket uri provided by user is valid.

  Args:
    bucket: Bucket uri provided by user.
  Returns:
    Sanitized bucket uri.
  Raises:
    ArgumentTypeError: If the name provided by user is not valid.
  """
  if not bucket.endswith('/'):
    bucket += '/'
  if not bucket.startswith('gs://'):
    bucket = 'gs://' + bucket
  match = _BUCKET_URI_RE.match(bucket)
  if not match:
    raise arg_parsers.ArgumentTypeError(
        'Bucket must match [{0}]'.format(_BUCKET_URI_RE.pattern))
  return bucket


def ParsePubsubTopicName(topic):
  """Checks if a Pub/Sub topic name provided by user is valid.

  Args:
    topic: Pub/Sub topic name provided by user.
  Returns:
    Topic name.
  Raises:
    ArgumentTypeError: If the name provided by user is not valid.
  """
  match = _TOPIC_NAME_RE.match(topic)
  if not match:
    raise arg_parsers.ArgumentTypeError(
        'Topic must match [{0}]'.format(_TOPIC_NAME_RE.pattern))
  project = properties.VALUES.core.project.Get(required=True)
  return 'projects/{0}/topics/{1}'.format(project, topic)


def ParseDirectory(directory):
  """Checks if a source directory provided by user is valid.

  Args:
    directory: Path do directory provided by user.
  Returns:
    Path to directory.
  Raises:
    ArgumentTypeError: If the directory provided by user is not valid.
  """
  if not os.path.exists(directory):
    raise arg_parsers.ArgumentTypeError('Provided directory does not exist.')
  if not os.path.isdir(directory):
    raise arg_parsers.ArgumentTypeError(
        'Provided path does not point to a directory.')
  return directory


def _GetViolationsFromError(error_info):
  """Looks for violations descriptions in error message.

  Args:
    error_info: json containing error information.
  Returns:
    List of violations descriptions.
  """
  result = ''
  details = None
  try:
    if 'details' in error_info:
      details = error_info['details']
    for field in details:
      if 'fieldViolations' in field:
        violations = field['fieldViolations']
        for violation in violations:
          if 'description' in violation:
            result += violation['description'] + '\n'
  except (ValueError, TypeError):
    pass
  return result


def CatchHTTPErrorRaiseHTTPException(func):
# TODO(user): merge this function with HandleHttpError defined elsewhere:
# * projects/lib/util.py
# * dns/lib/util.py
# (obstacle: GetHttpErrorMessage function may be project-specific)
  """Decorator that catches HttpError and raises corresponding exception."""

  @functools.wraps(func)
  def CatchHTTPErrorRaiseHTTPExceptionFn(*args, **kwargs):
    try:
      return func(*args, **kwargs)
    except apitools_base.HttpError as error:
      msg = GetHttpErrorMessage(error)
      unused_type, unused_value, traceback = sys.exc_info()
      raise base_exceptions.HttpException, msg, traceback

  return CatchHTTPErrorRaiseHTTPExceptionFn

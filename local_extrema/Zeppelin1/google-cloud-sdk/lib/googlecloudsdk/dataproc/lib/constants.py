# Copyright 2015 Google Inc. All Rights Reserved.

"""Constants for the dataproc tool."""

# TODO(user): Move defaults to the server
from googlecloudsdk.third_party.apis.dataproc.v1beta1 import dataproc_v1beta1_messages as messages

# Job Status states that do not change.
TERMINAL_JOB_STATES = [
    messages.JobStatus.StateValueValuesEnum.CANCELLED,
    messages.JobStatus.StateValueValuesEnum.DONE,
    messages.JobStatus.StateValueValuesEnum.ERROR,
]

# Path inside of GCS bucket to stage files.
GCS_STAGING_PREFIX = 'google-cloud-dataproc-staging'

# Beginning of driver output files.
JOB_OUTPUT_PREFIX = 'driveroutput'


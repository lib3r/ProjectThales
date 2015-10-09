# Copyright 2014 Google Inc. All Rights Reserved.

"""The 'gcloud test android devices' command group."""

from googlecloudsdk.calliope import base


class Devices(base.Group):
  """Explore Android testing device environments and characteristics."""

  detailed_help = {
      'DESCRIPTION': '{description}',
      'EXAMPLES': """\
          To list all Android devices available for running tests, run:

            $ {command} list

          To display detailed information about a specific Android device, run:

            $ {command} describe DEVICE_ID
          """,
  }

  @staticmethod
  def Args(parser):
    """Method called by Calliope to register flags common to this sub-group.

    Args:
      parser: An argparse parser used to add arguments that immediately follow
          this group in the CLI. Positional arguments are allowed.
    """
    pass

#!/usr/bin/env python
#
# Copyright 2013 Google Inc. All Rights Reserved.
#

"""Do initial setup for the Cloud SDK."""

import bootstrapping

# pylint:disable=g-bad-import-order
import argparse
import os
import sys

from googlecloudsdk.calliope import exceptions
from googlecloudsdk.core import config
from googlecloudsdk.core import platforms_install
from googlecloudsdk.core import properties
from googlecloudsdk.core.console import console_io
from googlecloudsdk.gcloud import gcloud

# pylint:disable=superfluous-parens


def ParseArgs():
  """Parse args for the installer, so interactive prompts can be avoided."""

  def Bool(s):
    return s.lower() in ['true', '1']

  parser = argparse.ArgumentParser()

  parser.add_argument('--usage-reporting',
                      default=None, type=Bool,
                      help='(true/false) Disable anonymous usage reporting.')
  parser.add_argument('--rc-path',
                      help='Profile to update with PATH and completion.')
  parser.add_argument('--command-completion', '--bash-completion',
                      default=None, type=Bool,
                      help=('(true/false) Add a line for command completion in'
                            ' the profile.'))
  parser.add_argument('--path-update',
                      default=None, type=Bool,
                      help=('(true/false) Add a line for path updating in the'
                            ' profile.'))
  parser.add_argument('--disable-installation-options', action='store_true',
                      help='DEPRECATED.  This flag is no longer used.')
  parser.add_argument('--override-components', nargs='*',
                      help='Override the components that would be installed by '
                      'default and install these instead.')
  parser.add_argument('--additional-components', nargs='+',
                      help='Additional components to install by default.  These'
                      ' components will either be added to the default install '
                      'list, or to the override-components (if provided).')

  return parser.parse_args()


def Prompts(usage_reporting):
  """Display prompts to opt out of usage reporting.

  Args:
    usage_reporting: bool, If True, enable usage reporting. If None, ask.
  """

  if config.InstallationConfig.Load().IsAlternateReleaseChannel():
    usage_reporting = True
    print("""
Usage reporting is always on for alternate release channels.
""")
    return

  if usage_reporting is None:
    print("""
To help improve the quality of this product, we collect anonymized data on how
the SDK is used. You may choose to opt out of this collection now (by choosing
'N' at the below prompt), or at any time in the future by running the following
command:
    gcloud config set --scope=user disable_usage_reporting true
""")

    usage_reporting = console_io.PromptContinue(
        prompt_string='Do you want to help improve the Google Cloud SDK')
  properties.PersistProperty(
      properties.VALUES.core.disable_usage_reporting, not usage_reporting,
      scope=properties.Scope.INSTALLATION)


def Install(override_components, additional_components):
  """Do the normal installation of the Cloud SDK."""
  # Install the OS specific wrapper scripts for gcloud and any pre-configured
  # components for the SDK.
  to_install = (override_components if override_components is not None
                else bootstrapping.GetDefaultInstalledComponents())
  if additional_components:
    to_install.extend(additional_components)

  print("""
This will install all the core command line tools necessary for working with
the Google Cloud Platform.
""")
  InstallComponents(to_install)

  # Show the list of components if there were no pre-configured ones.
  if not to_install:
    # pylint: disable=protected-access
    gcloud._cli.Execute(['--quiet', 'components', 'list'])


def ReInstall(component_ids):
  """Do a forced reinstallation of the Cloud SDK.

  Args:
    component_ids: [str], The components that should be automatically installed.
  """
  to_install = bootstrapping.GetDefaultInstalledComponents()
  to_install.extend(component_ids)
  InstallComponents(component_ids)


def InstallComponents(component_ids):
  # Installs the selected configuration or the wrappers for core at a minimum.
  # pylint: disable=protected-access
  gcloud._cli.Execute(
      ['--quiet', 'components', 'update', '--allow-no-backup'] + component_ids)


def main():
  pargs = ParseArgs()
  reinstall_components = os.environ.get('CLOUDSDK_REINSTALL_COMPONENTS')
  try:
    if reinstall_components:
      ReInstall(reinstall_components.split(','))
    else:
      Prompts(pargs.usage_reporting)
      bootstrapping.CommandStart('INSTALL', component_id='core')
      if not config.INSTALLATION_CONFIG.disable_updater:
        Install(pargs.override_components, pargs.additional_components)

      platforms_install.UpdateRC(
          command_completion=pargs.command_completion,
          path_update=pargs.path_update,
          rc_path=pargs.rc_path,
          bin_path=bootstrapping.BIN_DIR,
          sdk_root=bootstrapping.SDK_ROOT,
      )

      print("""\

For more information on how to get started, please visit:
  https://developers.google.com/cloud/sdk/gettingstarted

""")
  except exceptions.ToolException as e:
    print(e)
    sys.exit(1)


if __name__ == '__main__':
  main()

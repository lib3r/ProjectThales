# Copyright 2015 Google Inc. All Rights Reserved.

"""Clone GCP git repository.
"""

import textwrap

from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions as c_exc
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.credentials import store as c_store

from googlecloudsdk.source.lib import git


class Clone(base.Command):
  """Clone project git repository in the current directory."""

  detailed_help = {
      'DESCRIPTION': """\
          This command clones git repository for the currently active
          Google Cloud Platform project into specified folder in current
          directory.

          If you have enabled push-to-deploy in the Cloud Console,
          this `{command}` will clone the Google-hosted git repository
          associated with PROJECT. This repository will automatically be
          connected to Google, and it will use the credentials indicated as
          _active_ by `gcloud auth list`. Pushing to the origin's _master_
          branch will trigger an App Engine deployment using the contents
          of that branch.
      """,
      'EXAMPLES': textwrap.dedent("""\
          To perform a simple `"Hello, world!"` App Engine deployment with this
          command, run the following command lines with MYPROJECT replaced by
          a project you own and can use for this experiment.

            $ gcloud source repos clone REPOSITORY_NAME
            $ cd REPOSITORY_NAME
            $ git pull
              https://github.com/GoogleCloudPlatform/appengine-helloworld-python
            $ git push origin master
      """),
  }

  @staticmethod
  def Args(parser):
    parser.add_argument(
        'src',
        metavar='REPOSITORY_NAME',
        help=('Name of the repository. '
              'Note: GCP projects generally have (if created) repository '
              'named "default"'))
    parser.add_argument(
        'dst',
        metavar='DIRECTORY_NAME',
        nargs='?',
        help='Directory name for the cloned repo. Defaults repo name.')

  @c_exc.RaiseToolExceptionInsteadOf(git.Error, c_store.Error)
  def Run(self, args):
    """Clone GCP repo to current directory.

    Args:
      args: argparse.Namespace, the arguments this command is run with.

    Raises:
      ToolException: on project initialization errors.

    Returns:
      The path to the new git repo.
    """
    # Ensure that we're logged in.
    c_store.Load()

    project_id = properties.VALUES.core.project.Get(required=True)
    project_repo = git.Git(project_id, args.src)
    path = project_repo.Clone(destination_path=args.dst or args.src)
    if path:
      log.status.write('Project [{prj}] repository [{repo}] was cloned to '
                       '[{path}].\n'.format(prj=project_id, path=path,
                                            repo=project_repo.GetName()))


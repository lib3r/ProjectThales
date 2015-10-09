# Copyright 2014 Google Inc. All Rights Reserved.

"""Resource formats supplementary help."""

import textwrap

from googlecloudsdk.calliope import base
from googlecloudsdk.core.resource import resource_topics


class Formats(base.Command):
  """Resource formats supplementary help."""

  def Run(self, args):
    self.cli.Execute(args.command_path[1:] + ['--document=style=topic'])
    return None

  detailed_help = {

      'DESCRIPTION': textwrap.dedent("""\
          {description}

          === Format Expressions ===

          A format expression has 3 parts:

          *name*:: _name_
          *attributes*:: *[* [no-]_attribute-name_[=_value_] [, ... ] *]*
          *projection*:: *(* _projection-key_ [, ...] *)*

          *name* is required, *attributes* are optional, and *projection*
          may be required for some formats. Unknown attribute names are
          silently ignored.

          Format expressions may be composed. The expressions are scanned from
          left to right. Projections to the left provide default key attribute
          values, but only the rightmost projection specifies the keys to be
          projected. For example, if a default projection provides alias names
          for some keys then those alias names may be used in subsequent
          projections.

          {format_registry}
          """).format(
              description=resource_topics.ResourceDescription('format'),
              format_registry=resource_topics.FormatRegistryDescriptions()),

      'EXAMPLES': """\
          List a table of compute instance resources sorted by name:

            $ gcloud compute instances list --format='table[title=Instances,box](name:1, zone:zone, status)'

          List the disk interfaces for all compute instances as a compact
          comma separated list:

            $ gcloud compute instances list --format='value(disks[].interface.list())'

          List the URIs for all compute instances:

            $ gcloud compute instances list --format='value(uri())'

          List the project authenticated user email address:

            $ gcloud info --format='value(config.account)'
          """,
      }

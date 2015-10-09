# Copyright 2013 Google Inc. All Rights Reserved.

"""Resource projections supplementary help."""

import textwrap

from googlecloudsdk.calliope import base
from googlecloudsdk.core.resource import resource_topics


class Projections(base.Command):
  """Resource projections supplementary help."""

  def Run(self, args):
    self.cli.Execute(args.command_path[1:] + ['--document=style=topic'])
    return None

  detailed_help = {

      # pylint: disable=protected-access, need transform dicts.
      'DESCRIPTION': textwrap.dedent("""\
          {description}

          === Projection Expressions ===

          A *projection* is a function that transforms a resource to
          another resource. Projections typically shrink the resource
          by selecting a subset of the resource data values.

          A projection expression is an ordered tuple of keys that are to
          be retained in the projection. When combined with a format name
          it resembles a function call:

            table(name, network.ip.internal, network.ip.external, uri())

          A *transform* is a function that applies data conversion
          functions to resource data values. In *gcloud* a projection can
          both project and transform. A transform is denoted by appending one
          or more ._function_(...) calls to a resource key. This example
          applies the *iso*() transform to the *status.time* resource key:

            (name, status.time.iso())

          The built-in transform functions are listed below.

          In *gcloud* projections only appear in *--format* flag expressions:
          --format='_NAME_(_KEY_, ...)[_ATTR_, ...]'.

          === Key Attributes ===

          Key attributes provide per-key control of formatted output. Each
          key may have zero or more attributes, specified by appending
          :-separated attribute values to keys in a projection epression:

            _key_:_attribute_=_value_[:...]

          Attribute values may appear in any order. The attributes are:

          *alias*=_ALIAS-NAME_::
          Sets _ALIAS-NAME_ as an alias for the projection key.

          *align*=_ALIGNMENT_::
          Specifies the output column data alignment. Used by the *table*
          format. The alignment values are:

          *left*:::
          Left (default).

          *center*:::
          Center.

          *right*:::
          Right.

          *label*=_LABEL_::
          A string value used to label output. Use :label="" or :label=''
          for no label. The *table* format uses _LABEL_ values as column
          headings. Also sets _LABEL_ as an alias for the projection key.
          The default label is the the disambiguated right hand parts of the
          column key name in ANGRY_SNAKE_CASE.

          *sort*=_SORT-ORDER_::
          An integer counting from 1. Keys with lower sort-order are sorted
          first. Keys with same sort order are sorted left to right.

          === Transform Functions ===

          A transform function transforms a resource data item to a printable
          string. In the descriptions below the argument *r* is the implicit
          resource that the transform operates on. *r* is not specified in
          transform calls.

          {transform_registry}
          """).format(
              description=resource_topics.ResourceDescription('projection'),
              transform_registry=
              resource_topics.TransformRegistryDescriptions()),

      'EXAMPLES': """\
          List a table of instance *name* (sorted by *name* and centered with
          column heading *INSTANCE*) and *creationTimestamp* (listed as an
          ISO date/time):

            $ gcloud compute instances list --format='table(name:sort=1:align=center:label=INSTANCE, creationTimestamp.iso():label=START)'

          List only the *name*, *status* and *zone* instance resource keys in
          YAML format:

            $ gcloud compute instances list --format='yaml(name, status, zone)'

          List only the *config.account* key value(s) in the *info* resource:

            $ gcloud info --format='value(config.account)'
          """,
      }

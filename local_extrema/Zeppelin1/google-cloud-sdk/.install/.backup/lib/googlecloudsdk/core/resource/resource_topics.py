# Copyright 2014 Google Inc. All Rights Reserved.

"""Common resource topic text."""

import cStringIO
import textwrap

from googlecloudsdk.core.resource import resource_printer
from googlecloudsdk.core.resource import resource_transform


def ResourceDescription(name):
  """Generates resource help DESCRIPTION help text for name.

  This puts common text for the key, formats and projections topics in one
  place.

  Args:
    name: One of ['format', 'key', 'projection'].

  Raises:
    ValueError: If name is not one of the expected topic names.

  Returns:
    A detailed_help DESCRIPTION markdown string.
  """
  description = """\
  All *gcloud* commands return a list of resources on success (there are a few
  legacy exceptions, but these are being addressed command by command). By
  default the resources are pretty-printed on the standard output.

  There are two ways *gcloud* resource output is controlled, applied in this
  order:

  *resource projections*::
  A resource _projection_, specified by _PROJECTION_ in the
  --format=_NAME_[_DOC_ATTRIBUTES_](_PROJECTION_) flag, controls the amount of
  output produced for each resource in the output resource list. _PROJECTION_
  selects the resource keys listed for each resource. {see_projection}

  *resource formats*::
  A resource _format_, specified by _NAME_ and optional [_DOC_ATTRIBUTES_] in the
  --format=_NAME_[_DOC_ATTRIBUTES_](_PROJECTION_) flag, controls the output type of
  projected resources. {see_format}

  Projections and formats use *resource keys* to name resource data
  values. {see_key}
  """
  topics = ['format', 'key', 'projection']
  if name not in topics:
    raise ValueError('Expected one of [{topics}], got [{name}].'.format(
        topics=','.join(topics), name=name))
  see = {}
  for topic in ['format', 'key', 'projection']:
    if topic == name:
      see[topic] = 'Resource {0}s are described in detail below.'.format(topic)
    else:
      topic_command = {
          'format': 'formats',
          'key': 'resource-keys',
          'projection': 'projections',
          }
      see[topic] = 'For details run $ gcloud topic {0}.'.format(
          topic_command[topic])
  return textwrap.dedent(description).format(see_format=see['format'],
                                             see_key=see['key'],
                                             see_projection=see['projection'])


_DOC_MAIN, _DOC_ARGS, _DOC_ATTRIBUTES, _DOC_EXAMPLE, _DOC_SKIP = range(5)


def _ParseFormatDocString(printer):
  """Parses the doc string for printer.

  Args:
    printer: The doc string will be parsed from this resource format printer.

  Returns:
    A (description, attributes) tuple:
      description - The format description.
      attributes - A list of (name, description) tuples, one tuple for each
        format-specific attribute.

  Example resource printer docstring parsed by this method:
    '''This line is skipped. Printer attributes and Example sections optional.

    These lines describe the format.
    Another description line.

    Printer attributes:
      attribute-1-name: The description for attribute-1-name.
      attribute-N-name: The description for attribute-N-name.

    Example:
      One or more example lines for the 'For example:' section.
    '''
  """
  descriptions = []
  attributes = []
  example = []
  if not printer.__doc__:
    return '', '', ''
  _, _, doc = printer.__doc__.partition('\n')
  collect = _DOC_MAIN
  attribute = None
  attribute_description = []
  for line in textwrap.dedent(doc).split('\n'):
    line = line.replace('{', '{{')
    line = line.replace('}', '}}')

    if not line.startswith(' ') and line.endswith(':'):
      # The start of a new section.
      if attribute:
        # The current attribute description is done.
        attributes.append((attribute, ' '.join(attribute_description)))
        attribute = None
      if line == 'Printer attributes:':
        # Now collecting Printer attributes: section lines.
        collect = _DOC_ATTRIBUTES
      elif line == 'Example:':
        # Now collecting Example: section lines.
        collect = _DOC_EXAMPLE
      else:
        collect = _DOC_SKIP
      continue

    if not line or collect == _DOC_SKIP:
      # Only interested in the description body and the Printer args: section.
      continue
    elif collect == _DOC_MAIN:
      # The main description line.
      descriptions.append(line.strip())
    elif line.startswith('    '):
      if collect == _DOC_ATTRIBUTES:
        # An attribute description line.
        attribute_description.append(line.strip())
    elif collect == _DOC_EXAMPLE and line.startswith('  '):
      # An example section line.
      example.append(line.strip())
    else:
      # The current attribute description is done.
      if attribute:
        attributes.append((attribute, ' '.join(attribute_description)))
      # A new attribute description.
      attribute, _, text = line.partition(':')
      attribute = attribute.strip()
      attribute = attribute.lstrip('*')
      attribute_description = [text.strip()]
  return ' '.join(descriptions), attributes, example


def FormatRegistryDescriptions():
  """Returns help markdown for all registered resource printer formats."""
  descriptions = ['The supported formats and format specific attributes are:']
  for name, printer in sorted(resource_printer.GetFormatRegistry().iteritems()):
    description, attributes, example = _ParseFormatDocString(printer)
    descriptions.append('\n*{name}*::\n{description}'.format(
        name=name, description=description))
    if attributes:
      descriptions.append('+\nThe format attributes are:\n')
      for attribute, description in attributes:
        descriptions.append('*{attribute}*:::\n{description}'.format(
            attribute=attribute, description=description))
    if example:
      descriptions.append('+\nFor example`:`:::\n')
      descriptions.append(' '.join(example))
  descriptions.append('')
  return '\n'.join(descriptions)


def _ParseTransformDocString(func):
  """Parses the doc string for func.

  Args:
    func: The doc string will be parsed from this function.

  Returns:
    A (description, prototype, args) tuple:
      description - The function description.
      prototype - The function prototype string.
      args - A list of (name, description) tuples, one tuple for each arg.

  Example transform function docstring parsed by this method:
    '''Transform description. Example sections optional.

    These lines are skipped.
    Another skipped line.

    Args:
      r: The resource arg is always sepcified.
      arg-2-name[=default-2]: The description for arg-1-name.
      arg-N-name[=default-N]: The description for arg-N-name.
      kwargs: Omitted from the description.

    Example:
      One or more example lines for the 'For example:' section.
    '''
  """
  defaults = func.__defaults__
  if not func.__doc__:
    return '', '', '', ''
  description, _, doc = func.__doc__.partition('\n')
  collect = _DOC_MAIN
  arg = None
  descriptions = [description]
  example = []
  args = []
  arg_description = []
  formals = []
  for line in textwrap.dedent(doc).split('\n'):
    line = line.replace('{', '{{')
    line = line.replace('}', '}}')
    if line == 'Args:':
      # Now collecting Args: section lines.
      collect = _DOC_ARGS
    elif line == 'Example:':
      # Now collecting Example: section lines.
      collect = _DOC_EXAMPLE
    elif not line or collect == _DOC_SKIP:
      # Only interested in the main description and the Args: section.
      continue
    elif collect == _DOC_MAIN:
      # The main description line.
      descriptions.append(line.strip())
    elif line.startswith('    '):
      # An arg description line.
      arg_description.append(line.strip())
    elif collect == _DOC_EXAMPLE and line.startswith('  '):
      # An example description line.
      example.append(line.strip())
    else:
      # The current arg description is done.
      if arg and arg != 'kwargs':
        # Add arg to the function formal formal arguments.
        # Internal *args and **kwargs are skipped.
        if args and defaults and len(defaults) >= len(args):
          default = defaults[len(args) - 1]
        else:
          default = None
        if default is not None:
          formals.append('{arg}={default}'.format(
              arg=arg, default=repr(default)).replace("'", '"'))
        else:
          formals.append(arg)
        args.append((arg, ' '.join(arg_description)))
      if not line.startswith(' ') and line.endswith(':'):
        # The start of a new section.
        collect = _DOC_SKIP
        continue
      # A new arg description.
      arg, _, text = line.partition(':')
      arg = arg.strip()
      arg = arg.lstrip('*')
      arg_description = [text.strip()]
  prototype = '({formals})'.format(formals=', '.join(formals))
  return ' '.join(descriptions), prototype, args, example


def _TransformsDescriptions(transforms):
  """Generates resource transform help text markdown for transforms.

  Args:
    transforms: The transform name=>method symbol table.

  Returns:
    The resource transform help text markdown for transforms.
  """
  buf = cStringIO.StringIO()
  for name, transform in sorted(transforms.iteritems()):
    description, prototype, args, example = _ParseTransformDocString(transform)
    if not description:
      continue
    buf.write('\n*%s*%s::\n%s\n' % (name, prototype, description))
    if args:
      buf.write('+\n+\nThe arguments are:\n\n')
      for arg, description in args:
        buf.write('*```%s```*:::\n%s\n' % (arg, description))
    if example:
      buf.write('+\nFor example`:`:::\n\n{example}\n'.format(
          example=' '.join(example)))
  return buf.getvalue()


def TransformRegistryDescriptions():
  """Returns help markdown for all registered resource transforms."""
  descriptions = []
  for component, transforms in sorted(
      resource_transform.GetTransformRegistry().iteritems()):
    descriptions.append(
        '\nThe {component} transform functions are:\n{desc}\n'.format(
            component=component,
            desc=_TransformsDescriptions(transforms)))
  return ''.join(descriptions)

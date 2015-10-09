# Copyright 2015 Google Inc. All Rights Reserved.

"""Cloud resource list filter expression evaluator backend."""

import re

from googlecloudsdk.core.resource import resource_property


def _IsIn(matcher, value):
  """Applies matcher to determine if the expression operand is in value.

  Args:
    matcher: Boolean match function that takes value as an argument and
      returns True if the expression operand is in value.
    value: The value to match against.

  Returns:
    True if the expression operand is in value.
  """
  if matcher(value):
    return True
  try:
    for index in value:
      if matcher(index):
        return True
  except TypeError:
    pass
  return False


class Backend(object):
  """Cloud resource list filter expression evaluator backend.

  This is a backend for resource_filter.Parser(). The generated "evaluator" is a
  parsed resource expression tree with branching factor 2 for binary operator
  nodes, 1 for NOT and function nodes, and 0 for TRUE nodes. Evaluation for a
  resource object starts with expression_tree_root.Evaluate(obj) which
  recursively evaluates child nodes. The logic operators use left-right shortcut
  pruning, so an evaluation may not visit every node in the expression tree.
  """

  class Expr(object):
    """Expression base class."""
    pass

  class ExprTRUE(Expr):
    """TRUE node.

    Always evaluates True.
    """

    def Evaluate(self, unused_obj):
      return True

  class ExprConnective(Expr):
    """Base logic node.

    Attributes:
      left: Left Expr operand.
      right: Right Expr operand.
    """

    def __init__(self, left=None, right=None):
      super(Backend.ExprConnective, self).__init__()
      self._left = left
      self._right = right

  class ExprAND(ExprConnective):
    """AND node.

    AND with left-to-right shortcut pruning.
    """

    def __init__(self, *args, **kwargs):
      super(Backend.ExprAND, self).__init__(*args, **kwargs)

    def Evaluate(self, obj):
      if not self._left.Evaluate(obj):
        return False
      if not self._right.Evaluate(obj):
        return False
      return True

  class ExprOR(ExprConnective):
    """OR node.

    OR with left-to-right shortcut pruning.
    """

    def __init__(self, *args, **kwargs):
      super(Backend.ExprOR, self).__init__(*args, **kwargs)

    def Evaluate(self, obj):
      if self._left.Evaluate(obj):
        return True
      if self._right.Evaluate(obj):
        return True
      return False

  class ExprNOT(ExprConnective):
    """NOT node."""

    def __init__(self, expr=None):
      super(Backend.ExprNOT, self).__init__()
      self._expr = expr

    def Evaluate(self, obj):
      return not self._expr.Evaluate(obj)

  class ExprGlobal(Expr):
    """Global restriction function call node.

    Attributes:
      func: The function implementation Expr. Must match this description:
            func(obj, args)

            Args:
              obj: The current resource object.
              args: The possibly empty list of arguments.

            Returns:
              True on success.
      args: List of function call actual arguments.
    """

    def __init__(self, func=None, args=None):
      super(Backend.ExprGlobal, self).__init__()
      self._func = func
      self._args = args

    def Evaluate(self, unused_obj):
      return self._func(*self._args)

  class ExprOperand(Expr):
    """Operand node.

    Operand values are strings that represent string or numeric constants. The
    numeric value, if any, is precomputed by the constructor. If an operand has
    a numeric value then the actual key values are converted to numbers at
    Evaluate() time if possible for Apply(); if the conversion fails then the
    key and operand string values are passed to Apply().
    """

    def __init__(self, value):
      """Initializes an operand from value.

      Sets self.string_value to the string representation of value and
      self.numeric_value to the numeric value of value or None if it is not a
      number.

      Args:
        value: An int, float or string constant. If it is a string then
          self.numeric_value will be set to the int() or float() conversion of
          the string, or None if the string does not represent a number.
      """
      super(Backend.ExprOperand, self).__init__()
      if isinstance(value, basestring):
        self.string_value = value
        try:
          self.numeric_value = int(value)
        except ValueError:
          try:
            self.numeric_value = float(value)
          except ValueError:
            self.numeric_value = None
      else:
        self.string_value = str(value)
        self.numeric_value = value

  class ExprOperator(Expr):
    """Base term (<key operator operand>) node.

    ExprOperator subclasses must define the function Apply(self, value, operand)
    that returns the result of <value> <op> <operand>.

    Attributes:
      _key: Resource object key (list of str, int and/or None values).
      _operand: The term ExprOperand operand.
      _transform: Optional key value transform function.
      _args: Optional list of transform actual args.
      _backend: The backend object.
    """

    def __init__(self, key=None, operand=None, transform=None, args=None,
                 backend=None):
      super(Backend.ExprOperator, self).__init__()
      self._key = key
      self._operand = operand
      self._transform = transform
      self._args = args
      self._backend = backend

    def Evaluate(self, obj):
      """Evaluate a term node.

      Args:
        obj: The resource object to evaluate.
      Returns:
        The value of the operator applied to the key value and operand.
      """
      value = resource_property.Get(obj, self._key)
      if self._transform:
        try:
          value = (self._transform(value, *self._args) if self._key
                   else self._transform(*self._args))
        except (AttributeError, TypeError, ValueError):
          value = None
      # Each try/except attempts a different combination of value/operand
      # numeric and string conversions
      if self._operand.numeric_value is not None:
        try:
          return self.Apply(float(value), self._operand.numeric_value)
        except (TypeError, ValueError):
          pass
      try:
        return self.Apply(value, self._operand.string_value)
      except (AttributeError, ValueError):
        return False
      except TypeError:
        if isinstance(value, basestring):
          return False
      try:
        return self.Apply(str(value), self._operand.string_value)
      except TypeError:
        return False

  class ExprLT(ExprOperator):
    """LT node."""

    def __init__(self, *args, **kwargs):
      super(Backend.ExprLT, self).__init__(*args, **kwargs)

    def Apply(self, value, operand):
      return value < operand

  class ExprLE(ExprOperator):
    """LE node."""

    def __init__(self, *args, **kwargs):
      super(Backend.ExprLE, self).__init__(*args, **kwargs)

    def Apply(self, value, operand):
      return value <= operand

  class ExprInMatch(ExprOperator):
    """Membership and anchored prefix*suffix match node."""

    def __init__(self, prefix=None, suffix=None, *args, **kwargs):
      """Initializes the anchored prefix and suffix patterns.

      Args:
        prefix: The anchored prefix pattern string.
        suffix: The anchored suffix pattern string.
        *args: Super class positional args.
        **kwargs: Super class keyword args.
      """
      super(Backend.ExprInMatch, self).__init__(*args, **kwargs)
      self._prefix = prefix
      self._suffix = suffix

    def Apply(self, value, unused_operand):
      """Applies the : anchored case insensitive match operation."""

      def _InMatch(value):
        """Applies case insensitive string prefix/suffix match to value."""
        if value is None:
          return False
        v = str(value).lower()
        return ((not self._prefix or v.startswith(self._prefix)) and
                (not self._suffix or v.endswith(self._suffix)))

      return _IsIn(_InMatch, value)

  class ExprIn(ExprOperator):
    """Membership case-insensitive match node."""

    def __init__(self, *args, **kwargs):
      super(Backend.ExprIn, self).__init__(*args, **kwargs)
      self._operand.string_value = self._operand.string_value.lower()

    def Apply(self, value, operand):
      """Checks if operand is a member of value ignoring case differences.

      Args:
        value: The number, string, dict or list object value.
        operand: Number or string operand.

      Returns:
        True if operand is a member of value ignoring case differences.
      """

      def _InEq(subject):
        """Applies case insensitive string contains check to subject."""
        if operand == subject:
          return True
        try:
          if operand == subject.lower():
            return True
        except AttributeError:
          pass
        try:
          if operand in subject:
            return True
        except TypeError:
          pass
        try:
          if operand in subject.lower():
            return True
        except AttributeError:
          pass
        try:
          if int(operand) in subject:
            return True
        except ValueError:
          pass
        try:
          if float(operand) in subject:
            return True
        except ValueError:
          pass
        return False

      return _IsIn(_InEq, value)

  class ExprHAS(ExprOperator):
    """Case insensitive membership node.

    This is the pre-compile Expr for the ':' operator. It compiles into either
    an ExprInMatch node for prefix*suffix matching or an ExprIn node for
    membership.
    """

    def __new__(cls, key=None, operand=None, transform=None, args=None,
                **kwargs):
      """Checks for prefix*suffix operand.

      The * operator splits the operand into prefix and suffix matching strings.

      Args:
        key: Resource object key (list of str, int and/or None values).
        operand: The term ExprOperand operand.
        transform: Optional key value transform function.
        args: Optional list of transform actual args.
        **kwargs: Super class keyword args.

      Returns:
        ExprInMatch if operand is an anchored pattern, ExprIn otherwise.
      """
      if '*' not in operand.string_value:
        return Backend.ExprIn(key=key, operand=operand, transform=transform,
                              args=args, **kwargs)
      pattern = operand.string_value.lower()
      i = pattern.find('*')
      prefix = pattern[:i]
      suffix = pattern[i + 1:]
      return Backend.ExprInMatch(key=key, operand=operand, transform=transform,
                                 args=args, prefix=prefix, suffix=suffix,
                                 **kwargs)

  class ExprMatch(ExprOperator):
    """Anchored prefix*suffix match node."""

    def __init__(self, prefix=None, suffix=None, *args, **kwargs):
      """Initializes the anchored prefix and suffix patterns.

      Args:
        prefix: The anchored prefix pattern string.
        suffix: The anchored suffix pattern string.
        *args: Super class positional args.
        **kwargs: Super class keyword args.
      """
      super(Backend.ExprMatch, self).__init__(*args, **kwargs)
      self._prefix = prefix
      self._suffix = suffix

    def Apply(self, value, unused_operand):
      return ((not self._prefix or value.startswith(self._prefix)) and
              (not self._suffix or value.endswith(self._suffix)))

  class ExprEqual(ExprOperator):
    """Case sensitive EQ node with no match optimization."""

    def __init__(self, *args, **kwargs):
      super(Backend.ExprEqual, self).__init__(*args, **kwargs)

    def Apply(self, value, operand):
      return operand == value

  class ExprEQ(ExprOperator):
    """Case sensitive EQ node."""

    def __new__(cls, key=None, operand=None, transform=None, args=None,
                **kwargs):
      """Checks for prefix*suffix operand.

      The * operator splits the operand into prefix and suffix matching strings.

      Args:
        key: Resource object key (list of str, int and/or None values).
        operand: The term ExprOperand operand.
        transform: Optional key value transform function.
        args: Optional list of transform actual args.
        **kwargs: Super class keyword args.

      Returns:
        ExprMatch if operand is an anchored pattern, ExprEqual otherwise.
      """
      if '*' not in operand.string_value:
        return Backend.ExprEqual(key=key, operand=operand, transform=transform,
                                 args=args, **kwargs)
      pattern = operand.string_value
      i = pattern.find('*')
      prefix = pattern[:i]
      suffix = pattern[i + 1:]
      return Backend.ExprMatch(key=key, operand=operand, transform=transform,
                               args=args, prefix=prefix, suffix=suffix,
                               **kwargs)

  class ExprNE(ExprOperator):
    """NE node."""

    def __init__(self, *args, **kwargs):
      super(Backend.ExprNE, self).__init__(*args, **kwargs)

    def Apply(self, value, operand):
      try:
        return operand != value.lower()
      except AttributeError:
        return operand != value

  class ExprGE(ExprOperator):
    """GE node."""

    def __init__(self, *args, **kwargs):
      super(Backend.ExprGE, self).__init__(*args, **kwargs)

    def Apply(self, value, operand):
      return value >= operand

  class ExprGT(ExprOperator):
    """GT node."""

    def __init__(self, *args, **kwargs):
      super(Backend.ExprGT, self).__init__(*args, **kwargs)

    def Apply(self, value, operand):
      return value > operand

  class ExprRE(ExprOperator):
    """Unanchored RE match node."""

    def __init__(self, *args, **kwargs):
      super(Backend.ExprRE, self).__init__(*args, **kwargs)
      self.pattern = re.compile(self._operand.string_value)

    def Apply(self, value, unused_operand):
      if not isinstance(value, basestring):
        # This exception is caught by Evaluate().
        raise TypeError('RE match subject value must be a string.')
      return self.pattern.search(value) is not None

  class ExprNotRE(ExprOperator):
    """Unanchored RE not match node."""

    def __init__(self, *args, **kwargs):
      super(Backend.ExprNotRE, self).__init__(*args, **kwargs)
      self.pattern = re.compile(self._operand.string_value)

    def Apply(self, value, unused_operand):
      if not isinstance(value, basestring):
        # This exception is caught by Evaluate().
        raise TypeError('RE match subject value must be a string.')
      return self.pattern.search(value) is None

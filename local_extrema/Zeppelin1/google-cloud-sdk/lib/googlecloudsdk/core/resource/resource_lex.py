# Copyright 2015 Google Inc. All Rights Reserved.

r"""Resource expression lexer.

This class is used to parse resource keys, quoted tokens, and operator strings
and characters from resource filter and projection expression strings. Tokens
are defined by isspace() and caller specified per-token terminator characters.
" or ' quotes are supported, with these literal escapes: \\ => \, \' => ',
\" => ", and \<any-other-character> => \<any-other-character>.

Typical resource usage:

  # Initialize a lexer with the expression string.
  lex = resource_lex.Lexer(expression_string)
  # isspace() separated tokens. lex.SkipSpace() returns False at end of input.
  while lex.SkipSpace():
    # Save the expression string position for syntax error annotation.
    here = lex.GetPosition()
    # The next token must be a key.
    key = lex.Key()
    if not key:
      if lex.EndOfInput():
        # End of input is OK here.
        break
      # There were some characters in the input that did not form a valid key.
      raise resource_exceptions.ExpressionSyntaxError(
          'key expected [{0}].'.format(lex.Annotate(here)))
    # Check if the key is a function call.
    if lex.IsCharacter('('):
      # Collect the actual args and convert numeric args to float or int.
      args = lex.Args(convert=True)
    else:
      args = None
    # Skip an isspace() characters. End of input will fail with an
    # 'Operator expected [...]' resource_exceptions.ExpressionSyntaxError.
    lex.SkipSpace(token='Operator')
    # The next token must be one of these operators ...
    operator = lex.IsCharacter('+-*/&|')
    if not operator:
      # ... one of the operator names.
      if lex.IsString('AND'):
        operator = '&'
      elif lex.IsString('OR'):
        operator = '|'
      else:
        raise resource_exceptions.ExpressionSyntaxError(
            'Operator expected [{0}].'.format(lex.Annotate()))
    # The next token must be an operand. Convert to float or int if possible.
    # lex.Token() by default eats leading isspace().
    operand = lex.Token(convert=True)
    if not operand:
      raise resource_exceptions.ExpressionSyntaxErrorSyntaxError(
          'Operand expected [{0}].'.format(lex.Annotate()))
    # Process the key, args, operator and operand.
    Process(key, args, operator, operand)
"""

from googlecloudsdk.core.resource import resource_exceptions


class Lexer(object):
  """Resource expression lexer.

  This lexer handles simple and compound tokens. Compound tokens returned by
  Key() and Args() below are not strictly lexical items (i.e., they are parsed
  against simple grammars), but treating them as tokens here simplifies the
  resource expression parsers that use this class and avoids code replication.

  Attributes:
    _ESCAPE: The quote escape character.
    _QUOTES: The quote characters.
    _expr: The expression string.
    _position: The index of the next character in _expr to parse.
    _aliases: Parsed key alias dict indexed by the first key name.
  """
  _ESCAPE = '\\'
  _QUOTES = '\'"'

  def __init__(self, expression, aliases=None):
    """Initializes a resource lexer.

    Args:
      expression: The expression string.
      aliases: Parsed key alias dict indexed by the first key name.
    """
    self._expr = expression or ''
    self._position = 0
    self._aliases = aliases if aliases is not None else {}

  def EndOfInput(self, position=None):
    """Checks if the current expression string position is at the end of input.

    Args:
      position: Checks position instead of the current expression position.

    Returns:
      True if the expression string position is at the end of input.
    """
    if position is None:
      position = self._position
    return position >= len(self._expr)

  def GetPosition(self):
    """Returns the current expression position.

    Returns:
      The current expression position.
    """
    return self._position

  def SetPosition(self, position):
    """Sets the current expression position.

    Args:
      position: Sets the current position to position. Position should be 0 or a
        previous value returned by GetPosition().
    """
    self._position = position

  def Annotate(self, position=None):
    """Returns the expression string annotated for syntax error messages.

    Args:
      position: Uses position instead of the current expression position.

    Returns:
      The expression string with current position annotated.
    """
    here = position if position is not None else self._position
    cursor = '*HERE*'
    if here > 0 and not self._expr[here - 1].isspace():
      cursor = ' ' + cursor
    if here < len(self._expr) and not self._expr[here].isspace():
      cursor += ' '
    return '{0}{1}{2}'.format(self._expr[0:here], cursor, self._expr[here:])

  def SkipSpace(self, token=None):
    """Skips spaces in the expression string.

    Args:
      token: The expected token description, None if end of input is OK.

    Raises:
      ExpressionSyntaxError: End of input reached after skipping.

    Returns:
      True if the expression is not at end of input.
    """
    while not self.EndOfInput():
      if not self._expr[self._position].isspace():
        return True
      self._position += 1
    if token:
      raise resource_exceptions.ExpressionSyntaxError(
          '{0} expected [{1}].'.format(token, self.Annotate()))
    return False

  def IsCharacter(self, characters, peek=False, eoi_ok=False):
    """Checks if the next character is in characters and consumes it if it is.

    Args:
      characters: The characters to check for.
      peek: Does not consume a matching character if True.
      eoi_ok: True if end of input is OK. Returns None if at end of input.

    Raises:
      ExpressionSyntaxError: End of input reached and peek is False.

    Returns:
      The matching character or None if no match.
    """
    if self.EndOfInput():
      if peek or eoi_ok:
        return None
      raise resource_exceptions.ExpressionSyntaxError(
          'More tokens expected [{0}].'.format(self.Annotate()))
    c = self._expr[self._position]
    if c not in characters:
      return None
    if not peek:
      self._position += 1
    return c

  def IsString(self, name, peek=False):
    """Checks if the next space or ( separated token is name.

    Args:
      name: The token name to check.
      peek: Does not consume the string on match if True.

    Returns:
      True if the next space or ( separated token is name.
    """
    if not self.SkipSpace():
      return False
    i = self.GetPosition()
    if not self._expr[i:].startswith(name):
      return False
    i += len(name)
    if self.EndOfInput(i) or self._expr[i].isspace() or self._expr[i] == '(':
      if not peek:
        self.SetPosition(i)
      return True
    return False

  def Token(self, terminators='', space=True, convert=False):
    """Parses a possibliy quoted token from the current expression position.

    The quote characters are in _QUOTES. The _ESCAPE character can prefix
    an _ESCAPE or _QUOTE character to treat it as a normal character. If
    _ESCAPE is at end of input, or is followed by any other character, then it
    is treated as a normal character.

    Args:
      terminators: The characters that terminate the token. isspace() characters
        always terminate the token.
      space: True if space characters should be skipped after the token. Space
        characters are always skipped before the token.
      convert: Converts unquoted numeric string tokens to numbers if True.

    Raises:
      ExpressionSyntaxError: The expression has a syntax error.

    Returns:
      The token string, None if there is none.
    """
    quote = None  # The current quote character, None if not in quote.
    quoted = False  # True if the token is constructed from quoted parts.
    token = None  # The token char list, None for no token, [] for empty token.
    i = self.GetPosition()
    while True:
      if self.EndOfInput(i):
        break
      c = self._expr[i]
      if c == self._ESCAPE and not self.EndOfInput(i + 1):
        # Only _ESCAPE, the current quote or _QUOTES are escaped.
        c = self._expr[i + 1]
        if token is None:
          token = []
        if (c != self._ESCAPE and c != quote and
            (quote or c not in self._QUOTES)):
          token.append(self._ESCAPE)
        token.append(c)
        i += 1
      elif c == quote:
        # The end of the current quote.
        quote = None
      elif not quote and c in self._QUOTES:
        # The start of a new quote.
        quote = c
        quoted = True
        if token is None:
          token = []
      elif not quote and c in terminators:
        # Only unquoted terminators terminate the token.
        break
      elif quote or not c.isspace():
        # Append c to the token string.
        if token is None:
          token = []
        token.append(c)
      elif token:
        # A space after any token characters is a terminator.
        break
      i += 1
    if quote:
      raise resource_exceptions.ExpressionSyntaxError(
          'Unterminated [{0}] quote [{1}].'.format(quote, self.Annotate()))
    self.SetPosition(i)
    if space:
      self.SkipSpace()
    if token is not None:
      # Convert the list of token chars to a string.
      token = ''.join(token)
    if convert and token and not quoted:
      # Only unquoted tokens are converted.
      try:
        return int(token)
      except ValueError:
        try:
          return float(token)
        except ValueError:
          pass
    return token

  def Args(self, convert=False):
    """Parses a ,-separated, )-terminated arg list.

    The initial '(' has already been consumed by the caller.

    Args:
      convert: Converts unquoted numeric string args to numbers if True.

    Raises:
      ExpressionSyntaxError: The expression has a syntax error.

    Returns:
      [...]: The arg list.
    """
    required = False
    args = []
    while True:
      here = self.GetPosition()
      arg = self.Token(',)', convert=convert)
      end = self.IsCharacter(')')
      if arg is not None:
        args.append(arg)
      elif required or not end:
        raise resource_exceptions.ExpressionSyntaxError(
            'Argument expected [{0}].'.format(self.Annotate(here)))
      if end:
        break
      if not self.IsCharacter(','):
        raise resource_exceptions.ExpressionSyntaxError(
            'Closing ) expected in argument list [{0}].'.format(
                self.Annotate(here)))
      required = True
    return args

  def Key(self):
    """Parses a resource key from the expression.

    A resource key is a '.' separated list of names with optional [] slice or
    [NUMBER] array indices.

    Raises:
      ExpressionSyntaxError: The expression has a syntax error.

    Returns:
      The key which is a list of string, int and/or None elements.
    """
    key = []
    while not self.EndOfInput():
      here = self.GetPosition()
      # Key names may not contain any operator-ish characters. This prevents
      # keys from clashing with expressions that may contain keys. The excluded
      # characters are defined here for consistency.
      name = self.Token('[].(){},:=!<>+*/%&|^~@#;?', space=False)
      if name:
        # The first key name could be an alias except functions are not aliased.
        if (not key and not self.IsCharacter('(', peek=True, eoi_ok=True) and
            name in self._aliases):
          key.extend(self._aliases[name])
        else:
          key.append(name)
      elif not self.IsCharacter('[', peek=True):
        raise resource_exceptions.ExpressionSyntaxError(
            'Non-empty key name expected [{0}].'.format(self.Annotate(here)))
      if self.EndOfInput():
        break
      if self.IsCharacter(']'):
        raise resource_exceptions.ExpressionSyntaxError(
            'Unmatched ] in key [{0}].'.format(self.Annotate(here)))
      if self.IsCharacter('['):
        # [] slice or [NUMBER] array index.
        index = self.Token(']', convert=True)
        self.IsCharacter(']')
        key.append(index)
      if not self.IsCharacter('.'):
        break
      if self.EndOfInput():
        raise resource_exceptions.ExpressionSyntaxError(
            'Non-empty key name expected [{0}].'.format(self.Annotate()))
    return key

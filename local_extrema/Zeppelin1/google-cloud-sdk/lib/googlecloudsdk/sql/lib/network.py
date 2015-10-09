# Copyright 2015 Google Inc. All Rights Reserved.

"""Common utility functions for network operations."""

import socket

IP_VERSION_4 = 4
IP_VERSION_6 = 6
IP_VERSION_UNKNOWN = 0


def GetIpVersion(ip_address):
  """Given an ip address, try to open a socket and determine IP version.

  To check what IP version a client supports to connect to an IP address we
  try to open a socket forcing a specific Address Format. First try using
  AF_INET6. If we can open a socket, it means we have IPv6 connectivity to
  the IP address. If IPv6 is not established, then try to open an IPv4 socket
  and report IPv4 connectivity.

  Args:
    ip_address: string, IP address to test IP connectivity version to.

  Returns:
    int, IP_VERSION_6 if we can establish an IPv6 socket, else IP_VERSION_4 if
    we can establish an IPv4 socket. IP_VERSION_UNKNOWN otherwise.
  """
  try:
    socket.inet_pton(socket.AF_INET6, ip_address)
  except socket.error:
    try:
      # pylint:disable=g-socket-inet-aton
      # Possible Windows (and some Linux) problems with inet_pton.
      # Documentation says "Availability: Unix (maybe not all platforms)."
      socket.inet_aton(ip_address)
    except socket.error:
      return IP_VERSION_UNKNOWN
    else:
      return IP_VERSION_4
  else:
    return IP_VERSION_6


# AWS ORT Topology

Hardware topology and placement primitives for AWS ORT (Oxidized Runtime).

The topology component separates operating-system processor identifiers and physical
resource relationships from execution partition identities. Its boundary covers
resource discovery and placement without depending on an async runtime, HTTP, or
an AWS service client.

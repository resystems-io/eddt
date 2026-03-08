---
name: cobra-cli
description: Ensure that and cmd CLI implementations are carried out using the
Golang Cobra library.
triggers:
  - "create cli"
  - "build a command line tool"
  - "implement a go-lang CLI utilility"
---

# Golang Command Line Tools

## Architectural Constraints

1. When building and `cmd/...` tool in Golan use the Cobra library.
2. Structure logical functionality under a separate sub-command.
3. Configuration parameters for sub-commands must be grouped into a config
   struct.

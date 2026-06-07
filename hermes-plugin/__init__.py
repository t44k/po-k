"""hermes-pok — Drive Claude Code sessions via Xpo-k HTTP API.

Registers agent-facing tools for creating, prompting, and monitoring
CC sessions on remote po-k instances routed through Xpo-k.
"""

from .tools import register as register_tools


def register(ctx) -> None:
    """Plugin entry point — register all po-k tools."""
    register_tools(ctx)

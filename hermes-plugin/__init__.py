"""hermes-pok — Drive Claude Code sessions via Xpo-k HTTP API.

Registers agent-facing tools for creating, prompting, and monitoring
CC sessions on remote po-k instances routed through Xpo-k, plus a
``pre_llm_call`` hook that surfaces queued session notifications on the next
turn (see ``notifier.py``).
"""

import logging

from .tools import register as register_tools

logger = logging.getLogger(__name__)


def register(ctx) -> None:
    """Plugin entry point — register all po-k tools and the notifier hook."""
    register_tools(ctx)
    # Optional: a Hermes build without `pre_llm_call` in VALID_HOOKS still gets
    # the tools; only the automatic surfacing is skipped.
    try:
        from .notifier import on_pre_llm_call

        ctx.register_hook("pre_llm_call", on_pre_llm_call)
        logger.info("po-k plugin: registered pre_llm_call notification hook")
    except Exception as e:  # pragma: no cover — defensive
        logger.warning("po-k plugin: notification hook not registered: %s", e)
    # Origin capture: remembers which chat/topic/user a turn came from so a
    # webhook-woken turn can report back there. Returns None (allow) always.
    try:
        from .origin import on_pre_gateway_dispatch

        ctx.register_hook("pre_gateway_dispatch", on_pre_gateway_dispatch)
        logger.info("po-k plugin: registered pre_gateway_dispatch origin hook")
    except Exception as e:  # pragma: no cover — defensive
        logger.warning("po-k plugin: origin hook not registered: %s", e)

from __future__ import annotations

import logging
import os
import re
import subprocess
import threading
from typing import Optional, Tuple

log = logging.getLogger(__name__)

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "claude")
CLAUDE_TIMEOUT = int(os.environ.get("CLAUDE_TIMEOUT", "90"))
CLAUDE_CWD = os.environ.get("CLAUDE_CWD", "/Users/kongming")

# Status codes returned by generate_reply.
STATUS_OK = "ok"
STATUS_RATE_LIMIT = "rate_limit"
STATUS_ERROR = "error"
STATUS_TIMEOUT = "timeout"

# Substrings that suggest `claude -p` failed because of usage limits.
# Matched case-insensitively against combined stdout + stderr.
_RATE_LIMIT_PATTERNS: tuple[str, ...] = (
    "rate limit",
    "rate_limit",
    "ratelimit",
    "usage limit",
    "5-hour limit",
    "5 hour limit",
    "weekly limit",
    "you've used",
    "you have used",
    "quota",
    "out of credits",
    "out of tokens",
    "max_tokens has been exceeded",
)


def _looks_rate_limited(stdout: str, stderr: str) -> bool:
    blob = ((stdout or "") + "\n" + (stderr or "")).lower()
    return any(p in blob for p in _RATE_LIMIT_PATTERNS)

# Serialize claude -p invocations. Each call boots a fresh Claude process that
# loads MCP connectors, memory, and skills — running several in parallel on one
# machine creates contention that pushes individual calls past the timeout.
# Concurrency=1 queues bursts instead of dropping them; raise to 2 only if real
# backpressure shows up in logs.
_CLAUDE_SEMAPHORE = threading.BoundedSemaphore(1)

# Tools to deny. Anyone who can @mention you can feed text into Claude via
# this bot, so we block the most dangerous built-ins. MCP write tools are
# left allowed for now — tighten this list if a specific tool gets misused.
DISALLOWED_TOOLS = [
    "Bash",
    "Write",
    "Edit",
    "MultiEdit",
    "NotebookEdit",
]

SYSTEM_PROMPT = """You are replying on behalf of Kong Ming in Slack. Someone has @mentioned you in a channel or sent you a direct message.

You have access to Kong Ming's:
- Personal memory and notes (auto-loaded from his Claude memory).
- Connected tools / MCP connectors (ClickUp, Slack, calendar, Gmail, Notion, etc.).
- Skills installed on his Claude (including scrum-master-agile-coach for agile/Scrum questions, and skills for analytics, ops, finance, data, etc.).

Your job: use the available tools to look up the actual answer to the question, then reply concisely in first person as Kong Ming.

Knowing who you're talking to and what they point at:
- The prompt tells you the sender's name. If someone asks "who am I?", "do you know who's talking to you?", or similar, answer plainly with their name — never deflect or bounce the question back.
- When a message references something contextually ("the above message", "this plan", "what we discussed", "send that"), resolve the exact referent FIRST: read the prior messages in this conversation (provided above) and any linked doc/thread before replying. Only if you genuinely cannot locate it, say specifically what you looked at and couldn't find — do not answer an adjacent topic or ask them to re-paste without trying.

How to handle different kinds of messages:
- Knowledge / lookup ("what sprint are we in?", "where's the design doc?", "what's the Scrum process here?") → search ClickUp / memory / skills and answer with real information. Cite the source briefly when useful (e.g. "per the sprint board…").
- Process / methodology / Scrum / Agile questions → consult the scrum-master-agile-coach skill or relevant memory and answer substantively.
- Status / FYI / casual acknowledgment → reply naturally without over-engineering.
- Requires Kong Ming's personal judgment (approval, opinion on internal matters, scheduling decisions) → acknowledge briefly and say Kong Ming will follow up himself. Do NOT make commitments on his behalf.

Voice and style:
- First person as Kong Ming.
- Concise: 1-4 sentences typical; up to 5-6 if you're sharing a substantive lookup result.
- Match the thread tone (casual if casual, formal if formal).
- No emojis unless the thread already uses them. No sign-off, no name, no "As an AI", never reveal you are automated.
- If a lookup fails or returns nothing useful, say so honestly. Do NOT fabricate facts about Kong Ming's calendar, decisions, or private context.

Confidentiality — STRICT, do NOT share in your reply:
This Slack auto-reply must NEVER include revenue, profit, margin, or any financial figures, even when teammates ask. Specifically forbidden in the reply text:
- Total revenue, IAP revenue, ad revenue, subscription revenue
- ARPU, ARPDAU, ARPPU, LTV
- Profit, gross margin, contribution margin, EBITDA, net income
- Cost figures (CAC, COGS, OpEx, payback period)
- Unit economics, ROAS, RPI (revenue per install)
- Any other dollar/currency figures tied to financial performance

You MAY share (these are not financial):
- Engagement: DAU, MAU, WAU, sessions, avg session length, engagement time, retention, churn
- Acquisition (counts only): new users, first-open users, installs — but NOT cost or ROAS
- Gameplay: plays, level completion, song starts, fail rate, etc.
- Funnel conversion rates that are NOT tied to revenue (e.g., "% of users who started a song")
- Product / process / status info from ClickUp, memory, etc.

If a question explicitly asks for revenue/profit/financial figures, OR cannot be meaningfully answered without them, do NOT include them. Instead reply briefly with something like:
"Those numbers are confidential — I'll share them with you directly. DM me / catch me offline and I'll send them over."

Do NOT include partial, rounded, approximate, or ranged dollar figures as a workaround. A polite refusal is always safer than a leak. Err on the side of withholding any number you are unsure about.

Formatting — Slack mrkdwn, NOT standard Markdown:
Slack does NOT render standard Markdown. Use Slack's mrkdwn syntax so the reply renders correctly:
- Bold: *bold* (single asterisks). NEVER **bold** — that renders literally with the asterisks visible.
- Italic: _italic_ (underscores). NEVER *italic* in the Markdown sense — Slack reads single asterisks as bold.
- Strikethrough: ~strike~ (single tildes), not ~~strike~~.
- Inline code: `code` (backticks) — same as Markdown.
- Code block: triple backticks ``` on their own lines; no language tag (Slack ignores it).
- Block quote: lines starting with "> ".
- Links: <https://example.com|link text>. NEVER [link text](https://example.com).
- Bare URLs: paste the URL directly; Slack auto-links it.
- Bullets: start the line with "• " (bullet character) or "- ". NEVER use "*" for bullets (Slack reads it as bold).
- Numbered lists: "1. ", "2. ", etc.
- Headings: Slack has no Markdown headings. NEVER use "#", "##", or "###". For emphasis on a section title, use a bolded line: *Section title*
- Mentions: leave existing <@USERID> / <#CHANID|name> tokens intact; do not invent new ones.
Keep formatting minimal in chat — most replies need none. Reach for bold/bullets only when they genuinely aid scanning.

Output ONLY the reply text — no explanations, no preamble, no quotes around it."""


_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)\s]+)\)")
_BOLD_RE = re.compile(r"\*\*([^*\n]+?)\*\*")
_STRIKE_RE = re.compile(r"~~([^~\n]+?)~~")
_HEADING_RE = re.compile(r"^(#{1,6})[ \t]+(.*?)[ \t]*#*[ \t]*$", re.MULTILINE)
_BULLET_RE = re.compile(r"^([ \t]*)[-*][ \t]+", re.MULTILINE)


def _to_slack_mrkdwn(text: str) -> str:
    """Best-effort conversion of standard Markdown to Slack mrkdwn.

    Safety net for when the model slips and emits **bold**, [text](url),
    Markdown headings, or "- " bullets. Conservative: only rewrites patterns
    that clearly mean the same thing in both flavors.
    """
    if not text:
        return text

    # Protect fenced code blocks from rewrites.
    placeholders: list[str] = []

    def _stash(match: re.Match) -> str:
        placeholders.append(match.group(0))
        return f"\x00CODEBLOCK{len(placeholders) - 1}\x00"

    fenced = re.sub(r"```.*?```", _stash, text, flags=re.DOTALL)

    fenced = _MD_LINK_RE.sub(lambda m: f"<{m.group(2)}|{m.group(1)}>", fenced)
    fenced = _BOLD_RE.sub(r"*\1*", fenced)
    fenced = _STRIKE_RE.sub(r"~\1~", fenced)
    fenced = _HEADING_RE.sub(lambda m: f"*{m.group(2)}*", fenced)
    fenced = _BULLET_RE.sub(lambda m: f"{m.group(1)}• ", fenced)

    for i, original in enumerate(placeholders):
        fenced = fenced.replace(f"\x00CODEBLOCK{i}\x00", original)

    return fenced


def generate_reply(
    mention_text: str,
    thread_context: Optional[list[dict]] = None,
    sender_name: Optional[str] = None,
) -> Tuple[str, str]:
    """Generate a reply by shelling out to `claude -p` from Kong Ming's home dir.

    Inherits his Claude auth (Cowork session), auto-loaded memory, MCP
    connectors, and installed skills. No Anthropic API key required.

    Returns a (reply_text, status) tuple. status is one of:
      - STATUS_OK         — reply_text is the model output, ready to post
      - STATUS_RATE_LIMIT — claude -p failed because of a usage limit;
                            caller should NOT mark the event processed and
                            should let the catch-up loop retry later
      - STATUS_TIMEOUT    — claude -p ran past CLAUDE_TIMEOUT
      - STATUS_ERROR      — claude -p exited non-zero for some other reason
    """
    context_block = ""
    if thread_context:
        lines = [f"{m['user']}: {m['text']}" for m in thread_context]
        context_block = (
            "Prior messages in this conversation (oldest first):\n"
            + "\n".join(lines)
            + "\n\n"
        )

    sender_block = ""
    if sender_name:
        sender_block = (
            f"The person who just sent you this message is: {sender_name}. "
            f"If they ask who they are or who you're talking to, answer with "
            f"their name plainly; address them by name when it's natural.\n\n"
        )

    user_prompt = (
        f"{context_block}"
        f"{sender_block}"
        f"Message I was just sent / mentioned in:\n{mention_text}\n\n"
        f"Use any tools / connectors / memory needed to look up the answer, "
        f"then write the reply. Output only the reply text."
    )

    cmd = [
        CLAUDE_BIN,
        "-p",
        "--permission-mode",
        "bypassPermissions",
        "--append-system-prompt",
        SYSTEM_PROMPT,
        "--disallowed-tools",
        *DISALLOWED_TOOLS,
    ]

    log.info("invoking claude -p (cwd=%s, timeout=%ds)", CLAUDE_CWD, CLAUDE_TIMEOUT)

    with _CLAUDE_SEMAPHORE:
        try:
            result = subprocess.run(
                cmd,
                input=user_prompt,
                capture_output=True,
                text=True,
                timeout=CLAUDE_TIMEOUT,
                check=False,
                cwd=CLAUDE_CWD,
            )
        except subprocess.TimeoutExpired as e:
            err = e.stderr
            if isinstance(err, bytes):
                err = err.decode("utf-8", errors="replace")
            stderr_tail = (err or "")[-500:]
            log.error(
                "claude -p timed out after %ss; stderr tail: %s",
                CLAUDE_TIMEOUT, stderr_tail,
            )
            return "", STATUS_TIMEOUT
        except FileNotFoundError:
            log.error("claude binary not found at %r", CLAUDE_BIN)
            return "", STATUS_ERROR

    if result.returncode != 0:
        stdout_tail = (result.stdout or "")[-500:]
        stderr_tail = (result.stderr or "")[-500:]
        rate_limited = _looks_rate_limited(result.stdout or "", result.stderr or "")
        log.error(
            "claude -p exited %d (rate_limited=%s); stdout tail: %r ; stderr tail: %r",
            result.returncode, rate_limited, stdout_tail, stderr_tail,
        )
        return "", (STATUS_RATE_LIMIT if rate_limited else STATUS_ERROR)

    reply = result.stdout.strip()
    if reply.startswith('"') and reply.endswith('"') and len(reply) > 1:
        reply = reply[1:-1].strip()
    return _to_slack_mrkdwn(reply), STATUS_OK

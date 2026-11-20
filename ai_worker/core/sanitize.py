"""
Input sanitization, XML prompt-delimiter wrapping, and credential URL redaction helpers.
"""

import html
import re
from html.parser import HTMLParser
from typing import Any

MAX_TITLE_LEN = 300
MAX_DESCRIPTION_LEN = 6000
MAX_DRAFT_DESC_LEN = 2000
MAX_FEEDBACK_LEN = 300
MAX_TECH_LEN = 80
MAX_EVIDENCE_LEN = 500
MAX_LETTER_LEN = 4000
MAX_STACK_ITEMS = 25

_BLOCK_TAGS = frozenset({
    "p", "br", "li", "div", "h1", "h2", "h3", "h4", "h5", "h6",
    "tr", "section", "article", "header", "footer", "blockquote", "pre",
    "ul", "ol",
})
_IGNORED_TAGS = frozenset({"script", "style"})

_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_DELIMITER_TAGS_RE = re.compile(
    r"</?(?:untrusted_[a-z0-9_]*|learned_feedback|rag_evidence|audited_evidence|system)\b[^>]*>",
    re.IGNORECASE,
)
_CREDENTIAL_URL_RE = re.compile(r"\b([a-zA-Z][a-zA-Z0-9+.-]*://)([^\s/@]+)@")


class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self._parts: list[str] = []
        self._ignore_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        lower = tag.lower()
        if lower in _IGNORED_TAGS:
            self._ignore_depth += 1
            return
        if self._ignore_depth == 0:
            self._parts.append("\n" if lower in _BLOCK_TAGS else " ")

    def handle_startendtag(self, tag: str, attrs) -> None:
        lower = tag.lower()
        if self._ignore_depth == 0 and lower not in _IGNORED_TAGS:
            self._parts.append("\n" if lower in _BLOCK_TAGS else " ")

    def handle_endtag(self, tag: str) -> None:
        lower = tag.lower()
        if lower in _IGNORED_TAGS:
            if self._ignore_depth > 0:
                self._ignore_depth -= 1
            return
        if self._ignore_depth == 0:
            self._parts.append("\n" if lower in _BLOCK_TAGS else " ")

    def handle_data(self, data: str) -> None:
        if self._ignore_depth == 0 and data:
            self._parts.append(data)

    def handle_entityref(self, name: str) -> None:
        if self._ignore_depth == 0:
            self._parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if self._ignore_depth == 0:
            self._parts.append(f"&#{name};")

    def handle_comment(self, data: str) -> None:
        # Strip HTML comments completely
        return

    def get_text(self) -> str:
        return "".join(self._parts)


def sanitize_text(raw: Any, max_len: int = MAX_DESCRIPTION_LEN, *, single_line: bool = False) -> str:
    """Strips HTML, control chars, and XML delimiter breakouts, then normalizes whitespace and length."""
    if raw is None:
        return ""
    text = raw if isinstance(raw, str) else str(raw)
    if not text:
        return ""

    stripper = _HTMLStripper()
    stripper.feed(text)
    stripper.close()
    text = html.unescape(stripper.get_text())

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _CONTROL_CHARS_RE.sub("", text)
    text = _DELIMITER_TAGS_RE.sub("", text)

    if single_line:
        text = re.sub(r"\s+", " ", text).strip()
    else:
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"[ \t]*\n[ \t]*", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()

    if max_len > 0 and len(text) > max_len:
        if max_len > 3:
            text = text[: max_len - 3].rstrip() + "..."
        else:
            text = text[:max_len]

    return text


def wrap_xml(tag: str, content: str) -> str:
    """Wraps content in explicit XML delimiters after neutralizing embedded matching tags."""
    safe_tag = re.sub(r"[^a-zA-Z0-9_]", "", tag) or "data"
    return f"<{safe_tag}>\n{content}\n</{safe_tag}>"


def redact_url(text: Any) -> str:
    """Redacts user:password@ credentials from URLs inside strings or exception messages."""
    if text is None:
        return ""
    s = text if isinstance(text, str) else str(text)
    return _CREDENTIAL_URL_RE.sub(r"\1***:***@", s)

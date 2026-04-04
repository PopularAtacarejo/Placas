from __future__ import annotations

import re
from typing import Any, Callable, Mapping, Optional

# Aceita placeholders em ambos os formatos:
# - {{NOME}}
# - {NOME}
# com espacos opcionais ao redor do token.
PLACEHOLDER_TOKEN_RE = re.compile(
    r"(?:\{\{\s*(?P<token_double>[^{}]+?)\s*\}\}|\{\s*(?P<token_single>[^{}]+?)\s*\})"
)


def extract_placeholder_token(match: re.Match[str]) -> str:
    return (match.group("token_double") or match.group("token_single") or "").strip()


def resolve_placeholder_value(
    token_raw: str,
    replacements: Mapping[str, Any],
    key_normalizer: Callable[[str], str],
) -> tuple[str, Optional[Any]]:
    token_raw = (token_raw or "").strip()
    if not token_raw:
        return "", None

    token_key = token_raw.upper()
    value = replacements.get(token_key)
    if value is not None:
        return token_key, value

    normalized_key = key_normalizer(token_raw).upper()
    value = replacements.get(normalized_key)
    if value is not None:
        return normalized_key, value

    return "", None


def replace_placeholders_in_text(
    text: str,
    replacements: Mapping[str, Any],
    key_normalizer: Callable[[str], str],
    on_token_replaced: Optional[Callable[[str], None]] = None,
) -> str:
    def _replace_marker(match: re.Match[str]) -> str:
        token_raw = extract_placeholder_token(match)
        if not token_raw:
            return match.group(0)

        resolved_token, value = resolve_placeholder_value(token_raw, replacements, key_normalizer)
        if value is None:
            return match.group(0)

        if on_token_replaced and resolved_token:
            on_token_replaced(resolved_token)
        return str(value)

    return PLACEHOLDER_TOKEN_RE.sub(_replace_marker, text or "")

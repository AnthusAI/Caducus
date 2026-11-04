"""The operator alert email, as HTML and plain text.

No image of any kind, no tracking host, links shown whole and underlined. The
state is a word and a mark (never colour alone), because Ryan is colourblind and
some mail clients strip colour anyway. Everything is escaped. This template does
not need Markus; the themed renderer comes later.
"""

from __future__ import annotations

from dataclasses import dataclass
from html import escape
from typing import Any, List, Mapping, Sequence

MARKS = {"ALARM": "▲", "CLEARED": "✓", "NOTICE": "●"}
STATE_WORDS = {
    "ALARM": "ALARM",
    "CLEARED": "CLEARED",
    "NOTICE": "NOTICE",
}


@dataclass(frozen=True)
class RenderedEmail:
    subject: str
    text: str
    html: str


def describe_duration(seconds: int) -> str:
    """12 minutes, 1 hour 5 minutes, 2 days 3 hours: plain words, no cap."""
    if seconds < 60:
        return "less than a minute"
    units = (("day", 86400), ("hour", 3600), ("minute", 60))
    parts = []
    for name, size in units:
        count, seconds = divmod(seconds, size)
        if count:
            parts.append("%d %s%s" % (count, name, "" if count == 1 else "s"))
    return " ".join(parts[:2])


def _where(item: Mapping[str, Any]) -> str:
    place = item["product"] + ("/" + item["site"] if item.get("site") else "")
    bits = [place]
    if item.get("awsAccount"):
        bits.append("AWS account " + item["awsAccount"])
    if item.get("region"):
        bits.append(item["region"])
    return ", ".join(bits)


def _subject(items: Sequence[Mapping[str, Any]]) -> str:
    first = items[0]
    state = STATE_WORDS[first["state"]]
    place = first["product"] + ("/" + first["site"] if first.get("site") else "")
    if len(items) > 1:
        return "[%s] %s: %d alarms" % (state, place, len(items))
    if first["state"] == "CLEARED":
        return "[CLEARED] %s: %s is back to normal" % (place, first["alarmName"])
    return "[%s] %s: %s" % (state, place, first["what"].rstrip("."))


def _rows(item: Mapping[str, Any]) -> List[Any]:
    """(label, value) pairs in reading order, skipping what is empty."""
    rows = [("What broke", item["what"]), ("Where", _where(item))]
    if item["state"] == "CLEARED":
        rows.append(
            (
                "How long",
                "It lasted %s and flapped %d time%s."
                % (
                    describe_duration(item.get("durationSeconds", 0)),
                    item.get("flaps", 0),
                    "" if item.get("flaps", 0) == 1 else "s",
                ),
            )
        )
        rows.append(("Alarm", item["alarmName"]))
        return rows
    if item.get("hasNotes"):
        rows.append(("Impact", item.get("impact", "")))
        rows.append(("Next step", item.get("nextStep", "")))
    rows.append(("Alarm", item["alarmName"]))
    why = " ".join(p for p in (item.get("reason", ""), item.get("datapoints", "")) if p)
    if why:
        rows.append(("Why it fired", why))
    return [(label, value) for label, value in rows if value]


def _text_item(item: Mapping[str, Any]) -> str:
    state = STATE_WORDS[item["state"]]
    lines = ["%s %s" % (MARKS[item["state"]], state)]
    if item["state"] == "ALARM" and not item.get("hasNotes"):
        lines.append(item.get("notesLabel", "No triage notes yet"))
    lines.extend("%s: %s" % (label, value) for label, value in _rows(item))
    if item.get("link"):
        lines.append("Open the alarm: " + item["link"])
    return "\n".join(lines)


def _html_item(item: Mapping[str, Any]) -> str:
    state = STATE_WORDS[item["state"]]
    out = [
        '<div class="card" style="background:#ffffff;color:#1a1a1a;border-radius:8px;'
        'padding:16px 20px;margin:0 0 12px 0;">',
        '<p class="banner-%s" style="margin:0 0 12px 0;padding:8px 12px;border-radius:6px;'
        'font-weight:700;letter-spacing:0.04em;background:%s;">%s %s</p>'
        % (
            state.lower(),
            "#fbe3e1" if item["state"] == "ALARM" else "#dcefdc",
            escape(MARKS[item["state"]]),
            escape(state),
        ),
    ]
    if item["state"] == "ALARM" and not item.get("hasNotes"):
        out.append(
            '<p style="margin:0 0 12px 0;font-weight:700;">%s</p>'
            % escape(item.get("notesLabel", "No triage notes yet"))
        )
    for label, value in _rows(item):
        out.append(
            '<p style="margin:0 0 8px 0;line-height:1.45;">'
            '<strong>%s:</strong> %s</p>' % (escape(label), escape(value))
        )
    link = item.get("link", "")
    if link.startswith("https://"):
        out.append(
            '<p style="margin:12px 0 0 0;line-height:1.45;">'
            '<a class="link" href="%s" style="color:#0b57d0;text-decoration:underline;'
            'font-weight:700;">Open the alarm in CloudWatch</a></p>' % escape(link, quote=True)
        )
    out.append("</div>")
    return "\n".join(out)


_STYLE = """
:root { color-scheme: light dark; supported-color-schemes: light dark; }
@media (prefers-color-scheme: dark) {
  body, .page { background: #121212 !important; color: #ececec !important; }
  .card { background: #1e1e1e !important; color: #ececec !important; }
  .banner-alarm { background: #5a2420 !important; color: #ffffff !important; }
  .banner-cleared { background: #1f4a28 !important; color: #ffffff !important; }
  .banner-notice { background: #333333 !important; color: #ffffff !important; }
  .link { color: #8ab4f8 !important; }
  .foot { color: #b0b0b0 !important; }
}
"""


def render_operator_email(items: Sequence[Mapping[str, Any]]) -> RenderedEmail:
    """Render one email for one or more items of the same state."""
    if not items:
        raise ValueError("nothing to render")
    subject = " ".join(_subject(items).split())
    foot = "You get this because you are an Anthus operator. It sets no cookies and tracks nothing."
    text = "\n\n".join(_text_item(i) for i in items) + "\n\n-- \n" + foot + "\n"
    cards = "\n".join(_html_item(i) for i in items)
    html = (
        '<!doctype html>\n<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '<meta name="color-scheme" content="light dark">'
        '<meta name="supported-color-schemes" content="light dark">'
        "<title>%s</title><style>%s</style></head>"
        '<body style="margin:0;padding:0;background:#f4f4f2;color:#1a1a1a;'
        'font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Helvetica,Arial,sans-serif;font-size:16px;">'
        '<div class="page" style="background:#f4f4f2;padding:20px 12px;">'
        '<div style="max-width:620px;margin:0 auto;">%s'
        '<p class="foot" style="margin:16px 0 0 0;font-size:13px;color:#555555;">%s</p>'
        "</div></div></body></html>\n"
        % (escape(subject), _STYLE, cards, escape(foot))
    )
    return RenderedEmail(subject=subject, text=text, html=html)

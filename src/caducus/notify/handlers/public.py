"""The reader actions a host's routes send, mapped to the public audience.

Event: ``{"action": ..., ...}``; answer: ``{"statusCode": int, "body": {...}}``.

* ``subscribe``           {email, lists, consent, formId, page, origin, honeypot, ip, userAgent}
* ``confirm_page_state``  {token}           read only
* ``confirm``             {token, userAgent}
* ``preferences_state``   {token}           read only
* ``update_preferences``  {token, lists}
* ``unsubscribe``         {token, list?}    no list: everything
* ``delete_subscriber``   {token}

Admin actions (erase, export, import) are never reachable from here: a host calls them only
after checking the caller's role.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping

from ..audience.service import PublicAudience, SubscribeRequest, answer

READER_ACTIONS = frozenset(
    ["subscribe", "confirm", "confirm_page_state", "preferences_state", "update_preferences", "unsubscribe", "delete_subscriber"]
)


def dispatch(audience: PublicAudience, event: Mapping[str, Any]) -> Dict[str, Any]:
    action = event.get("action")
    token = event.get("token")
    if action == "subscribe":
        return audience.subscribe(
            SubscribeRequest(
                address=event.get("email"),
                lists=event.get("lists"),
                consent=event.get("consent"),
                form_id=event.get("formId", ""),
                page=event.get("page", ""),
                origin=event.get("origin", ""),
                honeypot=event.get("honeypot", ""),
                ip=event.get("ip", ""),
                user_agent=event.get("userAgent", ""),
            )
        )
    if action == "confirm_page_state":
        return audience.confirm_state(token)
    if action == "confirm":
        return audience.confirm(token, event.get("ip", ""), event.get("userAgent", ""))
    if action == "preferences_state":
        return audience.preferences_state(token)
    if action == "update_preferences":
        return audience.update_preferences(token, event.get("lists"))
    if action == "unsubscribe":
        list_id = event.get("list")
        return audience.unsubscribe(token, list_id if isinstance(list_id, str) and list_id else None)
    if action == "delete_subscriber":
        return audience.erase_self(token)
    return answer(400, status="error", error="unknown_action")

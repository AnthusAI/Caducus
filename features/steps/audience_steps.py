"""Steps for public audiences. Everything runs on the package's fakes: no SES, no DNS, no AWS."""

from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse

from behave import given, then, when

from caducus.notify.fakes import CONSENT, PublicWorld

A = "reader@example.com"


def _lists(text):
    return [part.strip().strip('"') for part in text.split('" and "')]


def _world(context):
    if not hasattr(context, "public") or context.public is None:
        context.public = PublicWorld()
    return context.public


def _confirmed(context, address, lists):
    w = _world(context)
    w.subscribe(address, lists)
    w.deliver()
    reply = w.act("confirm", token=w.confirm_token(address))
    assert reply["body"]["status"] == "confirmed", reply
    context.manage = reply["body"]["manageToken"]
    w.mailer.calls.clear()
    return context.manage


def _publish(context, lists, send=True):
    w = _world(context)
    context.stories = getattr(context, "stories", 0) + 1
    context.last_event = "story.published:lineage-%d" % context.stories
    w.publish(context.last_event, lists)
    if send:
        w.deliver()


def _story_mail(context, address):
    return [c for c in _world(context).mail_to(address) if "Confirm" not in c["subject"]]


def _no_row_holds(w, address, key):
    for (pk, sk), item in w.store.items.items():
        text = json.dumps(item)
        assert address not in text, (pk, sk)
        assert not pk.startswith("RCPT#" + key), (pk, sk)
        assert item.get("recipientKey") != key or pk.startswith("ALIAS#"), (pk, sk)


# -- setup ----------------------------------------------------------------------


@given('a host with the public lists "stories" and "weekly"')
def step_host(context):
    context.public = PublicWorld()


@given('"{address}" subscribed to "{lists}"')
def step_subscribed(context, address, lists):
    w = _world(context)
    w.subscribe(address, _lists(lists))
    w.deliver()
    context.confirm = w.confirm_token(address)


@given('"{address}" is a confirmed subscriber of "{lists}"')
def step_confirmed(context, address, lists):
    _confirmed(context, address, _lists(lists))


@given("the reader confirmed")
def step_reader_confirmed(context):
    context.reply = _world(context).act("confirm", token=context.confirm)


@given('the domain "{domain}" takes no mail')
def step_no_mail(context, domain):
    _world(context).domains.refused.add(domain)


@given("{count:d} readers confirmed within ten minutes")
def step_many_confirmed(context, count):
    w = _world(context)
    for n in range(count):
        address = "r%d@example.com" % n
        w.subscribe(address, ["stories"], ip="198.51.%d.%d" % (n // 250, n % 250))
        w.deliver()
        w.clock.advance(300)
        assert w.act("confirm", token=w.confirm_token(address))["body"]["status"] == "confirmed"


@given("readers on {count:d} networks each subscribed once")
def step_networks(context, count):
    w = _world(context)
    for n in range(count):
        w.subscribe("n%d@example.com" % n, ["stories"], ip="10.%d.%d.1" % (n // 250, n % 250))


@given("readers on {count:d} networks each subscribed once in earlier hours")
def step_networks_earlier(context, count):
    w = _world(context)
    for n in range(count):
        w.subscribe("e%d@example.com" % n, ["stories"], ip="10.%d.%d.1" % (n // 250, n % 250))
        w.clock.advance(3600)
    w.deliver()
    w.mailer.calls.clear()


@given("the reader unsubscribed from everything")
def step_unsubscribed_all(context):
    _world(context).act("unsubscribe", token=context.manage)


@given('SES reported a permanent bounce for "{address}"')
@when('SES reports a permanent bounce for a message to "{address}"')
def step_bounce(context, address):
    w = _world(context)
    context.provider = w.audience.provider_event(w.key(address), "bounced")


@given('a story published for "{lists}" reached "{address}"')
def step_story_reached(context, lists, address):
    _publish(context, _lists(lists))
    assert len(_story_mail(context, address)) == 1


@given('"{address}" was imported with the alias "{alias}" from "{source}" until tomorrow')
def step_alias(context, address, alias, source):
    w = _world(context)
    w.audience.import_subscriber(address, ["weekly"], {"confirmedAt": "2026-01-01T00:00:00Z"}, source)
    w.audience.add_alias(source, alias, address, int(w.clock().timestamp()) + 86400)
    context.alias_source = source


@given('{count:d} confirmed subscribers of "{list_id}"')
def step_n_confirmed(context, count, list_id):
    context.readers = ["s%d@example.com" % n for n in range(count)]
    for address in context.readers:
        _confirmed(context, address, [list_id])


@given("the list index answers {size:d} readers a page")
def step_page_size(context, size):
    _world(context).store.page_size = size


@given('a story event for "{list_id}" was accepted but its pages were never queued')
def step_crashed_fanout(context, list_id):
    w = _world(context)
    _publish(context, [list_id], send=False)
    w.queues.sent.clear()
    w.store.update("EVENT#" + context.last_event, "FANOUT", lambda row: dict(row, started=False))


# -- actions ----------------------------------------------------------------------


@when('"{address}" subscribes to "{lists}" with the form\'s own consent wording')
def step_subscribe(context, address, lists):
    w = _world(context)
    context.before = len(w.mail_to(address))
    context.reply = w.subscribe(address, _lists(lists))
    w.deliver()


@when('"{address}" subscribes to "{lists}" with the wording "{wording}"')
def step_subscribe_wording(context, address, lists, wording):
    w = _world(context)
    context.reply = w.subscribe(address, _lists(lists), consent=wording)
    w.deliver()


@when('a bot subscribes "{address}" and fills the hidden field')
def step_bot(context, address):
    w = _world(context)
    context.reply = w.subscribe(address, ["stories"], honeypot="http://spam.example")
    w.deliver()


@when("the confirm page reads the state of the confirmation link")
def step_confirm_state(context):
    context.state = _world(context).act("confirm_page_state", token=context.confirm)


@when("the reader presses the confirm button")
@when("the same confirmation link is pressed again")
def step_confirm(context):
    context.reply = _world(context).act("confirm", token=context.confirm, userAgent="Reader/1.0")
    if context.reply["body"].get("manageToken"):
        context.manage = context.reply["body"]["manageToken"]
    _world(context).mailer.calls.clear()


@when("the confirm window passes")
def step_window_passes(context):
    w = _world(context)
    w.clock.advance(w.settings.confirm_window_seconds + 1)


@when('"{address}" is submitted {count:d} more times from different networks')
def step_bomb(context, address, count):
    w = _world(context)
    for n in range(count):
        w.subscribe(address, ["stories"], ip="192.0.%d.9" % n)
        w.clock.advance(60)
    w.deliver()


@when("one network sends {count:d} signups for different addresses")
def step_one_network(context, count):
    w = _world(context)
    context.replies = [w.subscribe("flood%d@example.com" % n, ["stories"], ip="198.18.0.%d" % n) for n in range(count)]
    w.deliver()


@when("{count:d} readers on different networks subscribe within one hour")
def step_surge(context, count):
    w = _world(context)
    context.replies = [w.subscribe("v%d@example.com" % n, ["stories"], ip="172.16.%d.1" % n) for n in range(count)]
    w.deliver()


@when("the reader opens the manage link")
def step_manage(context):
    context.state = _world(context).act("preferences_state", token=context.manage)


@when('the reader turns "{list_id}" off')
def step_turn_off(context, list_id):
    w = _world(context)
    keep = [entry["id"] for entry in context.state["body"]["lists"] if entry["subscribed"] and entry["id"] != list_id]
    context.reply = w.act("update_preferences", token=context.manage, lists=keep)


@when('the reader turns "{list_id}" on from the manage link')
def step_turn_on(context, list_id):
    w = _world(context)
    w.mailer.calls.clear()
    context.reply = w.act("update_preferences", token=context.manage, lists=[list_id])
    w.deliver()


@when('a story is published for "{lists}"')
def step_publish(context, lists):
    _publish(context, _lists(lists))


@when('a story is published for "{lists}" and its delivery is queued but not sent')
def step_publish_unsent(context, lists):
    _publish(context, _lists(lists), send=False)
    _world(context).fan_out_only()


@when("the queue is sent")
def step_send_queue(context):
    _world(context).deliver()


@when("the mail provider posts the one-click unsubscribe from the message")
def step_one_click(context):
    w = _world(context)
    _publish(context, ["stories"])
    header = _story_mail(context, A)[-1]["headers"]["List-Unsubscribe"].strip("<>")
    query = parse_qs(urlparse(header).query)
    context.reply = w.act("unsubscribe", token=query["t"][0], list=None)  # the page sends no list for everything
    w.mailer.calls.clear()


@when("the reader unsubscribes from everything twice")
def step_unsubscribe_twice(context):
    w = _world(context)
    context.replies = [w.act("unsubscribe", token=context.manage) for _ in range(2)]


@when("the reader unsubscribes from everything")
def step_unsubscribe_all(context):
    _world(context).act("unsubscribe", token=context.manage)


@when('an admin erases "{address}"')
def step_erase(context, address):
    context.reply = _world(context).audience.erase(address)


@when("the reader deletes their data from the preferences page")
def step_delete_self(context):
    context.reply = _world(context).act("delete_subscriber", token=context.manage)


@when('an admin exports "{address}"')
def step_export(context, address):
    context.export = _world(context).audience.export(address)["body"]


@when('the host imports "{address}" for "{list_id}" with evidence from "{source}"')
def step_import(context, address, list_id, source):
    w = _world(context)
    context.evidence = {"confirmedAt": "2026-01-01T00:00:00Z", "formId": "newsletter", "wording": "Old wording."}
    context.import_args = (address, [list_id], context.evidence, source)
    context.outcome = w.audience.import_subscriber(*context.import_args)
    w.deliver()


@when('the host imports "{address}" again with the same evidence')
def step_import_again(context, address):
    context.outcome = _world(context).audience.import_subscriber(*context.import_args)


@when("two days pass")
def step_two_days(context):
    _world(context).clock.advance(2 * 86400)


@when("the same event arrives again")
def step_event_again(context):
    w = _world(context)
    w.publish(context.last_event, ["stories"])
    w.deliver()


# -- outcomes -------------------------------------------------------------------


@then('the answer is {code:d} "{status}"')
def step_answer(context, code, status):
    assert context.reply["statusCode"] == code and context.reply["body"]["status"] == status, context.reply


@then('the answer is {code:d} "{status}" with a manage token')
def step_answer_token(context, code, status):
    step_answer(context, code, status)
    assert context.reply["body"]["manageToken"].startswith("v1.")


@then('both answers are {code:d} "{status}"')
def step_both(context, code, status):
    assert all(r["statusCode"] == code and r["body"]["status"] == status for r in context.replies), context.replies


@then('exactly one confirmation is mailed to "{address}"')
def step_one_confirmation(context, address):
    mails = _world(context).mail_to(address)
    assert len(mails) == 1 and "Confirm" in mails[0]["subject"], mails


@then('no new confirmation is mailed to "{address}"')
def step_no_new(context, address):
    assert len(_world(context).mail_to(address)) == context.before


@then('nothing is mailed to "{address}"')
def step_nothing(context, address):
    assert not _world(context).mail_to(address)


@then('nothing else has reached "{address}"')
def step_nothing_else(context, address):
    assert not _story_mail(context, address)


@then('the page names the list "{name}"')
def step_names(context, name):
    assert [entry["name"] for entry in context.state["body"]["lists"]] == [name]


@then('"{address}" is not yet subscribed')
def step_not_yet(context, address):
    w = _world(context)
    assert w.audience.subscribed_lists(w.key(address)) == []


@then('a story published for "{lists}" reaches "{address}"')
def step_reaches(context, lists, address):
    before = len(_story_mail(context, address))
    _publish(context, _lists(lists))
    assert len(_story_mail(context, address)) == before + 1


@then('a story published for "{lists}" does not reach "{address}"')
def step_not_reaches(context, lists, address):
    before = len(_story_mail(context, address))
    _publish(context, _lists(lists))
    assert len(_story_mail(context, address)) == before


@then("the consent record holds the wording hash, the form, the page path, the origin and both times")
def step_consent(context):
    (record,) = _world(context).audience.export(A)["body"]["consent"]
    for field in ("wordingHash", "formId", "page", "origin", "requestedAt", "confirmedAt"):
        assert record.get(field), field
    context.record = record


@then("the stored wording is exactly the form's wording")
def step_wording(context):
    assert context.record["wording"] == CONSENT


@then("the page path carries no query string")
def step_page_path(context):
    assert "?" not in context.record["page"] and context.record["page"].endswith("/notifications/")


@then("the resend gap is the whole confirm window")
def step_gap_window(context):
    w = _world(context)
    assert w.audience._lag.resend_gap(w.settings.confirm_window_seconds) == w.settings.confirm_window_seconds


@then("the resend gap is at most {seconds:d} seconds")
def step_gap(context, seconds):
    w = _world(context)
    assert w.audience._lag.resend_gap(w.settings.confirm_window_seconds) <= seconds


@then('"{address}" can ask for a second confirmation once that gap has passed, and not before')
def step_second(context, address):
    w = _world(context)
    gap = w.audience._lag.resend_gap(w.settings.confirm_window_seconds)
    w.subscribe(address, ["stories"], ip="100.64.0.1")
    w.clock.advance(gap - 1)
    w.subscribe(address, ["stories"], ip="100.64.1.1")
    w.deliver()
    assert len(w.mail_to(address)) == 1
    w.clock.advance(1)
    w.subscribe(address, ["stories"], ip="100.64.2.1")
    w.deliver()
    assert len(w.mail_to(address)) == 2


@then('the later signups from it are answered {code:d} "{status}"')
def step_later_limited(context, code, status):
    codes = [r["statusCode"] for r in context.replies]
    assert codes[0] == 202 and code in codes, codes
    first = codes.index(code)
    assert all(c == code for c in codes[first:]), codes
    context.limited = ["flood%d@example.com" % n for n in range(first, len(codes))]


@then("no confirmation is mailed for them")
def step_none_for_limited(context):
    assert not any(_world(context).mail_to(a) for a in context.limited)


@then('a signup from another network is still answered {code:d} "{status}"')
def step_other_network(context, code, status):
    reply = _world(context).subscribe("other@example.com", ["stories"], ip="203.0.113.200")
    assert reply["statusCode"] == code and reply["body"]["status"] == status


@then('every one of them is answered {code:d} "{status}"')
def step_every(context, code, status):
    assert all(r["statusCode"] == code and r["body"]["status"] == status for r in context.replies)


@then("one surge notice is raised for the hour")
def step_one_surge(context):
    w = _world(context)
    surges = [pk for (pk, sk) in w.store.items if sk == "META" and pk.startswith("EVENT#audience.surge:")]
    assert len(surges) == 1, surges
    assert [c for c in w.mailer.calls if "Unusually many signups" in c["subject"]], "the operators are told"


@then('the page shows the address as "{masked}" with "stories" on and "weekly" on')
def step_page_shows(context, masked):
    body = context.state["body"]
    assert body["email"] == masked
    assert {e["id"]: e["subscribed"] for e in body["lists"]} == {"stories": True, "weekly": True}


@then("exactly one message for it reaches \"{address}\"")
def step_exactly_one(context, address):
    assert len(_story_mail(context, address)) == 1


@then("the message carries List-Unsubscribe with the one-click link and List-Unsubscribe-Post")
def step_headers(context):
    mail = _story_mail(context, A)[-1]
    headers = mail["headers"]
    assert headers["List-Unsubscribe-Post"] == "List-Unsubscribe=One-Click"
    assert headers["List-Unsubscribe"].startswith("<https://news.example.com/api/notifications/unsubscribe?t=v1.")
    context.mail = mail


@then("the footer links go to the preferences and unsubscribe pages")
def step_footer(context):
    w = _world(context)
    links = w.audience.links_for(w.key(A), ["stories"])
    assert "/notifications/preferences/?token=" in links["manage"]
    assert "/notifications/unsubscribe/?token=" in links["unsubscribe"] and "list=stories" in links["unsubscribe"]


@then("no link or header holds the address")
def step_no_address(context):
    assert A not in json.dumps(context.mail["headers"])
    assert A not in json.dumps(_world(context).audience.links_for(_world(context).key(A), ["stories"]))


@then('"{address}" is suppressed as "{reason}"')
def step_suppressed(context, address, reason):
    w = _world(context)
    assert w.audience.suppression(w.key(address)) == reason


@then('"{address}" is not suppressed')
def step_not_suppressed(context, address):
    w = _world(context)
    assert w.audience.suppression(w.key(address)) is None


@then('nothing reaches "{address}" and the delivery says "{status}"')
def step_nothing_status(context, address, status):
    w = _world(context)
    assert not _story_mail(context, address)
    rows = [r for r in w.log.for_event(context.last_event) if r["recipientKey"] == w.key(address)]
    assert rows and rows[0]["status"] == status, rows


@then("no confirmation is mailed again")
def step_no_confirmation_again(context):
    assert not [m for m in _world(context).mail_to(A) if "Confirm" in m["subject"]]


@then('a consent record of source "{source}" exists')
def step_consent_source(context, source):
    records = _world(context).audience.export(A)["body"]["consent"]
    assert any(r["source"] == source for r in records), records


@then('a new signup for "{address}" mails nothing')
def step_new_signup_nothing(context, address):
    w = _world(context)
    w.mailer.calls.clear()
    reply = w.subscribe(address, ["stories"], ip="198.51.100.77")
    w.deliver()
    assert reply["body"]["status"] == "check_email" and not w.mail_to(address)


@then("the host was asked to clear its provider's suppression for the address")
def step_unsuppress_called(context):
    assert _world(context).unsuppressed == [A]


@then("no row holds the address, its choices, its consent or its delivery history")
def step_no_row(context):
    w = _world(context)
    _no_row_holds(w, A, w.key(A))


@then('an import of "{address}" is refused')
def step_import_refused(context, address):
    assert _world(context).audience.import_subscriber(address, ["stories"], {"confirmedAt": "x"}, "old") == "refused"


@then("the old manage link no longer opens the preferences page")
def step_old_link(context):
    assert _world(context).act("preferences_state", token=context.manage)["body"]["status"] == "invalid"


@then('"{address}" may subscribe again through the form')
def step_again(context, address):
    w = _world(context)
    w.mailer.calls.clear()
    reply = w.subscribe(address, ["stories"], ip="198.51.100.88")
    w.deliver()
    assert reply["body"]["status"] == "check_email" and len(w.mail_to(address)) == 1
    assert w.act("confirm", token=w.confirm_token(address))["body"]["status"] == "confirmed"
    assert w.audience.suppression(w.key(address)) is None


@then("the export holds the address, the lists, the consent record with its wording and the delivery history")
def step_export_holds(context):
    export = context.export
    assert export["profile"]["address"] == A
    assert export["lists"] == {"stories": "subscribed", "weekly": "unsubscribed"} or export["lists"].get("stories") == "subscribed"
    assert export["consent"][0]["wording"] == CONSENT
    assert export["deliveries"] and export["deliveries"][0]["kind"] == "audience.confirmation"


@then('the answer is "{outcome}" and no mail is sent')
def step_outcome_no_mail(context, outcome):
    assert context.outcome == outcome and not _world(context).mailer.calls


@then('the answer is "{outcome}"')
def step_outcome(context, outcome):
    assert context.outcome == outcome


@then('the consent record is marked migrated from "{source}"')
def step_migrated(context, source):
    (record,) = _world(context).audience.export("old@example.com")["body"]["consent"]
    assert record["source"] == "migrated" and record["from"] == source and record["formId"] == "newsletter"


@then('the alias "{alias}" gives a manage token for "{address}"')
def step_alias_works(context, alias, address):
    w = _world(context)
    token = w.audience.manage_token_for_alias(context.alias_source, alias)
    assert token and w.act("preferences_state", token=token)["body"]["status"] == "ready"


@then('the alias "{alias}" gives nothing')
def step_alias_gone(context, alias):
    assert _world(context).audience.manage_token_for_alias(context.alias_source, alias) is None


@then("the fan-out took {count:d} page messages")
def step_pages(context, count):
    w = _world(context)
    assert w.fanout_pages == count, w.fanout_pages


@then("each of the {count:d} got exactly one message")
def step_each_one(context, count):
    for address in context.readers[:count]:
        assert len(_story_mail(context, address)) == 1, address

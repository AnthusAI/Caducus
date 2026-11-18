Feature: Cadence families in public audiences
  As a reader I choose how often I hear from a publication: every item as it
  appears, a daily summary, or a weekly issue. Every-item and daily are two
  cadences of the same thing, so I hold only one of them at a time, and when I
  switch, every item reaches me by exactly one route: nothing twice, nothing
  missed. The weekly issue is a different thing and stands alone.

  Background:
    Given a host whose lists "every-item" and "daily" form one cadence family and "weekly" stands alone

  Scenario: A reader cannot hold two lists of one family
    Given "reader@example.com" is a confirmed subscriber of "every-item"
    When the reader asks the manage link for "every-item" and "daily"
    And the reader opens the manage link
    Then the page shows "daily" on and "every-item" off

  Scenario: Two new lists of one family at once are refused
    When "reader@example.com" subscribes to "every-item" and "daily" with the form's own consent wording
    Then the answer is 400 "one_cadence_per_family"
    And nothing is mailed to "reader@example.com"

  Scenario: The refusal does not depend on what the address already holds
    Given "reader@example.com" is a confirmed subscriber of "every-item"
    When "reader@example.com" subscribes to "every-item" and "daily" with the form's own consent wording
    Then the answer is 400 "one_cadence_per_family"

  Scenario: Two new lists of one family from the manage link are refused
    Given "reader@example.com" is a confirmed subscriber of "weekly"
    When the reader asks the manage link for "weekly", "every-item" and "daily"
    Then the answer is 400 "one_cadence_per_family"
    And a story published for "every-item" does not reach "reader@example.com"
    And a story published for "daily" does not reach "reader@example.com"

  Scenario: Choosing another list of the family moves the reader
    Given "reader@example.com" is a confirmed subscriber of "every-item"
    When "reader@example.com" subscribes to "daily" with the form's own consent wording
    And the reader confirms the new subscription
    Then the answer is 200 "confirmed" with a manage token
    And a story published for "daily" reaches "reader@example.com"
    And a story published for "every-item" does not reach "reader@example.com"

  Scenario: A list with no family is unaffected
    Given "reader@example.com" is a confirmed subscriber of "every-item" and "weekly"
    When the reader asks the manage link for "weekly" and "daily"
    Then a story published for "weekly" reaches "reader@example.com"
    And a story published for "daily" reaches "reader@example.com"
    And a story published for "every-item" does not reach "reader@example.com"

  Scenario: Membership keeps its start while the reader stays
    Given "reader@example.com" is a confirmed subscriber of "daily"
    When two days pass
    And the reader asks the manage link for "daily" and "weekly"
    Then the reader's membership of "daily" began at "2026-11-04T12:00:00Z"
    And the reader's membership of "weekly" began at "2026-11-06T12:00:00Z"

  Scenario: Membership starts again when the reader rejoins
    Given "reader@example.com" is a confirmed subscriber of "daily" and "weekly"
    When the reader asks the manage link for "weekly"
    And two days pass
    And the reader asks the manage link for "daily" and "weekly"
    Then the reader's membership of "daily" began at "2026-11-06T12:00:00Z"
    And the reader's membership of "weekly" began at "2026-11-04T12:00:00Z"

  Scenario: Without a host rule a switch takes effect at once
    Given "reader@example.com" is a confirmed subscriber of "every-item"
    When the reader asks the manage link for "daily"
    Then the reader's membership of "every-item" ended at "2026-11-04T12:00:00Z"
    And the reader's membership of "daily" began at "2026-11-04T12:00:00Z"

  Scenario: A switch takes effect at the next period start
    Given a host whose lists "every-item" and "daily" form one cadence family and whose switches take effect at the next midnight
    And "reader@example.com" is a confirmed subscriber of "every-item"
    When the reader asks the manage link for "daily"
    Then the reader's membership of "every-item" ended at "2026-11-05T00:00:00Z"
    And the reader's membership of "daily" began at "2026-11-05T00:00:00Z"
    And a story published for "every-item" reaches "reader@example.com"
    And a story published for "daily" does not reach "reader@example.com"

  Scenario: During a switch the manage page shows the reader's choice
    Given a host whose lists "every-item" and "daily" form one cadence family and whose switches take effect at the next midnight
    And "reader@example.com" is a confirmed subscriber of "every-item"
    When the reader asks the manage link for "daily"
    And the reader opens the manage link
    Then the page shows "daily" on and "every-item" off

  Scenario: Every item reaches the reader by exactly one route through a switch
    Given a host whose lists "every-item" and "daily" form one cadence family and whose switches take effect at the next midnight
    And "reader@example.com" is a confirmed subscriber of "every-item"
    When the reader asks the manage link for "daily"
    And the time is "2026-11-04T15:00:00Z"
    And a story is published for "every-item"
    Then "reader@example.com" gets that publication
    When a summary is published for "daily" as of "2026-11-04T00:00:00Z"
    Then "reader@example.com" does not get that publication
    When the time is "2026-11-05T00:30:00Z"
    And a summary is published for "daily" as of "2026-11-05T00:00:00Z"
    Then "reader@example.com" gets that publication
    When a story is published for "every-item"
    Then "reader@example.com" does not get that publication
    And "reader@example.com" has been sent 2 story messages in all

  Scenario: Choosing the old list again before the switch begins cancels it
    Given a host whose lists "every-item" and "daily" form one cadence family and whose switches take effect at the next midnight
    And "reader@example.com" is a confirmed subscriber of "every-item"
    When the reader asks the manage link for "daily"
    And the reader asks the manage link for "every-item"
    And the time is "2026-11-05T00:30:00Z"
    Then a story published for "every-item" reaches "reader@example.com"
    And a story published for "daily" does not reach "reader@example.com"

  Scenario: A fan-out as of an instant pages only the members then
    Given "early@example.com" is a confirmed subscriber of "daily"
    And the time is "2026-11-05T12:00:00Z"
    And "late@example.com" is a confirmed subscriber of "daily"
    When a summary is published for "daily" as of "2026-11-04T18:00:00Z"
    Then "early@example.com" gets that publication
    And "late@example.com" does not get that publication
    When a summary is published for "daily" as of "2026-11-05T18:00:00Z"
    Then "early@example.com" gets that publication
    And "late@example.com" gets that publication

  Scenario: A summary of a past period still reaches a reader who has since moved off
    Given a host whose lists "every-item" and "daily" form one cadence family and whose switches take effect at the next midnight
    And "reader@example.com" is a confirmed subscriber of "daily"
    When the reader asks the manage link for "every-item"
    And the time is "2026-11-05T00:30:00Z"
    And a summary is published for "daily" as of "2026-11-04T18:00:00Z"
    Then "reader@example.com" gets that publication

  Scenario: A link from the list the reader moved off leaves the current list alone
    Given "reader@example.com" is a confirmed subscriber of "every-item" and "weekly"
    And a story published for "every-item" reached "reader@example.com"
    And the reader asks the manage link for "daily" and "weekly"
    When the mail provider posts the one-click unsubscribe from that earlier message
    Then the answer is 200 "unsubscribed"
    And a story published for "daily" reaches "reader@example.com"
    And a story published for "weekly" reaches "reader@example.com"
    And a story published for "every-item" does not reach "reader@example.com"

  Scenario: One-click unsubscribe leaves only the named list and never rejoins another
    Given "reader@example.com" is a confirmed subscriber of "daily" and "weekly"
    And a story published for "daily" reached "reader@example.com"
    When the mail provider posts the one-click unsubscribe from that earlier message
    Then a story published for "weekly" reaches "reader@example.com"
    And a story published for "daily" does not reach "reader@example.com"
    And a story published for "every-item" does not reach "reader@example.com"

  Scenario: An import of two lists of one family is refused
    When the host imports "reader@example.com" for "every-item" and "daily" with evidence from "old-system"
    Then the answer is "refused"
    And "reader@example.com" is not yet subscribed

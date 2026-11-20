Feature: Public audiences
  As an anonymous reader of a publication, I subscribe by email to the lists I
  choose, confirm from my inbox, change my choices and leave from any message,
  and the host keeps proof of what I agreed to. Nothing here reaches SES or DNS.

  Background:
    Given a host with the public lists "stories" and "weekly"

  Scenario: Double opt-in
    When "reader@example.com" subscribes to "stories" with the form's own consent wording
    Then the answer is 202 "check_email"
    And exactly one confirmation is mailed to "reader@example.com"
    When a story is published for "stories"
    Then nothing else has reached "reader@example.com"

  Scenario: Confirming subscribes, and a GET of the confirm page state changes nothing
    Given "reader@example.com" subscribed to "stories"
    When the confirm page reads the state of the confirmation link
    Then the page names the list "New stories"
    And "reader@example.com" is not yet subscribed
    When the reader presses the confirm button
    Then the answer is 200 "confirmed" with a manage token
    And a story published for "stories" reaches "reader@example.com"

  Scenario: A confirmation link works once
    Given "reader@example.com" subscribed to "stories"
    And the reader confirmed
    When the same confirmation link is pressed again
    Then the answer is 400 "invalid"

  Scenario: A confirmation link expires with the confirm window
    Given "reader@example.com" subscribed to "stories"
    When the confirm window passes
    And the reader presses the confirm button
    Then the answer is 400 "invalid"

  Scenario: The answer reveals nothing
    Given "reader@example.com" is a confirmed subscriber of "stories"
    When "reader@example.com" subscribes to "stories" with the form's own consent wording
    Then the answer is 202 "check_email"
    And no new confirmation is mailed to "reader@example.com"

  Scenario: Consent evidence matches the form
    Given "reader@example.com" is a confirmed subscriber of "stories"
    Then the consent record holds the wording hash, the form, the page path, the origin and both times
    And the stored wording is exactly the form's wording
    And the page path carries no query string

  Scenario: A wording the form does not show is refused
    When "reader@example.com" subscribes to "stories" with the wording "I agree to anything."
    Then the answer is 400 "invalid_consent"
    And nothing is mailed to "reader@example.com"

  Scenario: A filled hidden field is a bot
    When a bot subscribes "victim@example.com" and fills the hidden field
    Then the answer is 202 "check_email"
    And nothing is mailed to "victim@example.com"

  Scenario: A domain that takes no mail
    Given the domain "nomail.example" takes no mail
    When "reader@nomail.example" subscribes to "stories" with the form's own consent wording
    Then the answer is 400 "invalid_email"
    And nothing is mailed to "reader@nomail.example"

  Scenario: A confirmation bomb
    Given "victim@example.com" subscribed to "stories"
    When "victim@example.com" is submitted 20 more times from different networks
    Then exactly one confirmation is mailed to "victim@example.com"

  Scenario: The resend gap is the confirm window until enough readers have confirmed
    Given 58 readers confirmed within ten minutes
    Then the resend gap is the whole confirm window

  Scenario: The resend gap is learned from how fast readers confirm
    Given 200 readers confirmed within ten minutes
    Then the resend gap is at most 1024 seconds
    And "late@example.com" can ask for a second confirmation once that gap has passed, and not before

  Scenario: A network far above what readers do
    Given readers on 300 networks each subscribed once
    When one network sends 30 signups for different addresses
    Then the later signups from it are answered 429 "slow_down"
    And no confirmation is mailed for them
    And a signup from another network is still answered 202 "check_email"

  Scenario: One network cannot teach the limit to accept itself
    Given a new host where nobody has subscribed yet
    When one network sends 30 signups for different addresses
    Then the later signups from it are answered 429 "slow_down"

  Scenario: A surge raises a notice and refuses nothing
    Given readers on 300 networks each subscribed once in earlier hours
    When 60 readers on different networks subscribe within one hour
    Then every one of them is answered 202 "check_email"
    And one surge notice is raised for the hour

  Scenario: Preferences
    Given "reader@example.com" is a confirmed subscriber of "stories" and "weekly"
    When the reader opens the manage link
    Then the page shows the address as "r***@example.com" with "stories" on and "weekly" on
    When the reader turns "weekly" off
    Then a story published for "weekly" does not reach "reader@example.com"
    And a story published for "stories" reaches "reader@example.com"

  Scenario: A reader on two lists gets one message
    Given "reader@example.com" is a confirmed subscriber of "stories" and "weekly"
    When a story is published for "stories" and "weekly"
    Then exactly one message for it reaches "reader@example.com"

  Scenario: Every public message can be left in one click
    Given "reader@example.com" is a confirmed subscriber of "stories"
    When a story is published for "stories"
    Then the message carries List-Unsubscribe with the one-click link and List-Unsubscribe-Post
    And the footer links go to the preferences and unsubscribe pages
    And no link or header holds the address

  Scenario: One-click unsubscribe
    Given "reader@example.com" is a confirmed subscriber of "stories"
    When the mail provider posts the one-click unsubscribe from the message
    Then the answer is 200 "unsubscribed"
    And a story published for "stories" does not reach "reader@example.com"
    And "reader@example.com" is suppressed as "unsubscribed"

  Scenario: Unsubscribing twice, or after erasure, still succeeds
    Given "reader@example.com" is a confirmed subscriber of "stories"
    When the reader unsubscribes from everything twice
    Then both answers are 200 "unsubscribed"

  Scenario: Unsubscribed while a message waits in the queue
    Given "reader@example.com" is a confirmed subscriber of "stories"
    When a story is published for "stories" and its delivery is queued but not sent
    And the reader unsubscribes from everything
    And the queue is sent
    Then nothing reaches "reader@example.com" and the delivery says "suppressed"

  Scenario: Re-subscribing by manage link
    Given "reader@example.com" is a confirmed subscriber of "stories"
    And the reader unsubscribed from everything
    When the reader turns "stories" on from the manage link
    Then no confirmation is mailed again
    And a story published for "stories" reaches "reader@example.com"
    And a consent record of source "manage_link" exists

  Scenario: Bounces and complaints suppress
    Given "reader@example.com" is a confirmed subscriber of "stories"
    When SES reports a permanent bounce for a message to "reader@example.com"
    Then "reader@example.com" is suppressed as "bounce"
    And a story published for "stories" does not reach "reader@example.com"
    And a new signup for "reader@example.com" mails nothing

  Scenario: A deliberate opt-in lifts a bounce
    Given "reader@example.com" is a confirmed subscriber of "stories"
    And SES reported a permanent bounce for "reader@example.com"
    When the reader turns "stories" on from the manage link
    Then "reader@example.com" is not suppressed
    And the host was asked to clear its provider's suppression for the address

  Scenario: Erasure
    Given "reader@example.com" is a confirmed subscriber of "stories"
    And a story published for "stories" reached "reader@example.com"
    When an admin erases "reader@example.com"
    Then no row holds the address, its choices, its consent or its delivery history
    And an import of "reader@example.com" is refused
    And the old manage link no longer opens the preferences page
    But "reader@example.com" may subscribe again through the form

  Scenario: The reader deletes their own data
    Given "reader@example.com" is a confirmed subscriber of "stories"
    When the reader deletes their data from the preferences page
    Then the answer is 200 "deleted"
    And no row holds the address, its choices, its consent or its delivery history

  Scenario: Export
    Given "reader@example.com" is a confirmed subscriber of "stories"
    When an admin exports "reader@example.com"
    Then the export holds the address, the lists, the consent record with its wording and the delivery history

  Scenario: Import without re-confirming
    When the host imports "old@example.com" for "weekly" with evidence from "old-newsletter"
    Then the answer is "imported" and no mail is sent
    And the consent record is marked migrated from "old-newsletter"
    And a story published for "weekly" reaches "old@example.com"
    When the host imports "old@example.com" again with the same evidence
    Then the answer is "unchanged"

  Scenario: Old links keep working until the alias date
    Given "old@example.com" was imported with the alias "h123" from "old-newsletter" until tomorrow
    Then the alias "h123" gives a manage token for "old@example.com"
    When two days pass
    Then the alias "h123" gives nothing

  Scenario: Fan-out goes page by page
    Given 7 confirmed subscribers of "stories"
    And the list index answers 3 readers a page
    When a story is published for "stories"
    Then the fan-out took 3 page messages
    And each of the 7 got exactly one message

  Scenario: A crashed fan-out resumes on retry
    Given 2 confirmed subscribers of "stories"
    And a story event for "stories" was accepted but its pages were never queued
    When the same event arrives again
    Then each of the 2 got exactly one message

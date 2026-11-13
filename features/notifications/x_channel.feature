Feature: A host announces on a publication's X account through Caducus
  As a publication, I want each qualifying Story posted once to my own X
  account, so that readers who follow the account hear about it, and so that
  I can see the post and what it cost. X offers no idempotency key, so the
  channel never repeats a post when it cannot tell whether the first one landed.

  Background:
    Given a host that connected the X account "@example_pub" as a broadcast recipient
    And X's price list as of 2026-11-12: post with a link 0.200 USD, delete 0.010 USD, read 0.005 USD per post

  Scenario: A broadcast is posted once
    When the host sends the announcement "story.announce:acct1:lineage9" with a link twice
    And the X channel handles the queue
    And the X channel handles the queue again
    Then exactly one post is made at X
    And the delivery records the post id and a cost of 0.200 USD
    And the delivery notes the price list its cost came from

  Scenario: The text is posted exactly as drafted
    When the host sends the announcement "story.announce:acct1:lineage1" saying "A new Story @someone https://example.com/s/1?utm_source=x"
    And the X channel handles the queue
    Then the post at X says exactly "A new Story @someone https://example.com/s/1?utm_source=x"

  Scenario: A post with no link costs the plain post price
    When the host sends the announcement "story.announce:acct1:plain" saying "Just words"
    And the X channel handles the queue
    Then the delivery records a cost of 0.015 USD

  Scenario: An unknown outcome is checked before retrying
    Given X creates the post but does not answer in time
    When the host sends the announcement "story.announce:acct1:lineage2" with a link twice
    And the X channel handles the queue and the outcome is unknown
    And the X channel handles the queue again
    Then exactly one post is made at X
    And the account's recent posts were read before anything was posted again
    And the delivery status is "sent" with that post's id
    And the delivery records a cost of 0.005 USD

  Scenario: An unknown outcome with no post is retried after the check
    Given X does not answer the post request in time
    When the host sends the announcement "story.announce:acct1:lineage3" with a link twice
    And the X channel handles the queue and the outcome is unknown
    And the X channel handles the queue again
    Then exactly one post is made at X
    And the delivery status is "sent"

  Scenario: A server error is an unknown outcome too
    Given X answers the post request with a 503
    When the host sends the announcement "story.announce:acct1:lineage4" with a link twice
    And the X channel handles the queue and the outcome is unknown
    Then nothing was posted at X
    And the delivery status is still "queued"

  Scenario: X asks us to wait
    Given X answers the post request with 429 and a reset time 90 seconds away
    When the host sends the announcement "story.announce:acct1:lineage5" with a link twice
    And the X channel handles the queue
    Then the channel waited 90 seconds and then posted once

  Scenario: A refusal is final
    Given X answers the post request with 403 "You are not allowed to create a Tweet with this text"
    When the host sends the announcement "story.announce:acct1:lineage6" with a link twice
    And the X channel handles the queue
    Then the delivery failed and says why
    And nothing was posted at X

  Scenario: A duplicate-content refusal counts as posted only once the account shows the post
    Given the account already shows a post for the link
    And X answers the post request with 403 "You are not allowed to create a Tweet with duplicate content"
    When the host sends the announcement "story.announce:acct1:lineage7" with a link twice
    And the X channel handles the queue
    Then the delivery status is "sent" with that post's id
    And exactly one post is made at X

  Scenario: A duplicate-content refusal the account does not confirm fails
    Given X answers the post request with 403 "You are not allowed to create a Tweet with duplicate content"
    When the host sends the announcement "story.announce:acct1:lineage8" with a link twice
    And the X channel handles the queue
    Then the delivery failed and says why

  Scenario: Deleting a post records its cost
    Given a delivered announcement
    When the host withdraws the post as "editor@example.org"
    Then the post is deleted at X
    And the delivery was withdrawn by "editor@example.org" with the time
    And the delivery records a cost of 0.210 USD

  Scenario: A paused host posts nothing
    Given the host has paused announcements because "paused by the owner"
    When the host sends the announcement "story.announce:acct1:lineage10" with a link twice
    And the X channel handles the queue
    Then nothing was posted at X
    And the delivery was refused because "paused by the owner"

  Scenario: A budget that cannot cover the next post stops it
    Given the host's budget is 0.100 USD
    When the host sends the announcement "story.announce:acct1:lineage11" with a link twice
    And the X channel handles the queue
    Then nothing was posted at X
    And the delivery was refused because "budget reached"

  Scenario: The host is told what each request cost
    When the host sends the announcement "story.announce:acct1:lineage12" with a link twice
    And the X channel handles the queue
    Then the host's budget guard was told about 0.200 USD

  Scenario: Tokens never appear in the log
    When the host sends the announcement "story.announce:acct1:lineage13" with a link twice
    And the X channel handles the queue
    Then the access token was sent to X as a bearer token
    And no token appears in the delivery log or in the logs

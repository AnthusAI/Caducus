Feature: A broadcast goes to an audience account, not to people
  As a host, I want to address a notification to my publication's connected
  account, so that announcements need no subscriber list and ignore reader
  choices that do not apply to them.

  Background:
    Given a host that connected the X account "@example_pub" as a broadcast recipient

  Scenario: A broadcast makes one delivery to the named account
    When the host sends the announcement "story.announce:acct1:lineage20" with a link twice
    Then there is one delivery, to the account "@example_pub" on the channel "x"
    And the second send was reported as a duplicate

  Scenario: A broadcast ignores reader preferences
    Given every person has turned off every kind
    When the host sends the announcement "story.announce:acct1:lineage21" with a link
    Then no preference, consent or suppression was looked up
    And one delivery is queued

  Scenario: A broadcast goes only to the account the host names
    Given the host also connected the X account "@other_pub"
    When the host sends the announcement "story.announce:acct1:lineage22" with a link
    Then one delivery is queued, to "@example_pub" only

  Scenario: A broadcast to an account nobody connected is refused
    When the host sends an announcement to the account "CHANNELACCT#x#nobody"
    Then the event is refused with "no audience"

  Scenario: A broadcast with no text is refused
    When the host sends an announcement with no text
    Then the event is refused with "a broadcast needs facts.text"

  Scenario: A broadcast kind cannot be grouped
    When a host registers a broadcast kind that groups its events
    Then the registry refuses it

Feature: Storm grouping
  As an operator, a storm of related alarms reaches me as one or two messages
  with everything in them. Nothing is dropped: alarms are grouped and paced.

  Background:
    Given the notification service has one operator
    And the alarm grouping window is 120 seconds

  Scenario: A burst
    Given alarms for one site
    When ten fire within the grouping window
    Then the first sends at once
    And one further message lists the other nine, each with its link
    And all ten are in the delivery log

  Scenario: Different sites stay separate
    When alarms fire for two different sites
    Then each site has its own message

  Scenario: A large burst is still two messages
    Given alarms for one site
    When 50 fire within the grouping window
    Then 2 messages were queued in all
    And all 50 are in the delivery log

  Scenario: Clearing messages group the same way
    Given alarms for one site
    When ten fire and all clear within the grouping window
    Then the first clearing sends at once
    And one further message lists the other nine clearings

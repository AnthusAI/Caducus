Feature: Incidents
  As an operator, I get one message when an alarm starts and one when it ends,
  however often it flaps, because too many notifications is the same as none.

  Background:
    Given the notification service has one operator
    And the alarm hold-down is 300 seconds

  Scenario: A flapping alarm is one incident
    Given an alarm entered ALARM and a message went out
    When it goes OK and back to ALARM within the hold-down, three times
    Then no further message goes out

  Scenario: Clearing sends one message
    Given an alarm entered ALARM and a message went out
    And the alarm went OK 10 minutes later
    When the hold-down passes
    Then one message says it cleared, how long it lasted and how many times it flapped

  Scenario: Still flapping when the hold-down ends does not clear
    Given an alarm entered ALARM and a message went out
    And the alarm went OK
    And the alarm went back to ALARM
    When the hold-down passes
    Then no clearing message goes out

  Scenario: Missing data is not an incident
    When an alarm enters INSUFFICIENT_DATA
    Then no message goes out and the transition is logged

  Scenario: A muted alarm is recorded but silent
    When a muted alarm enters ALARM
    Then no message goes out and the delivery log says it was muted

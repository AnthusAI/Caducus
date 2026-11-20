Feature: One event format
  As any Anthus app, I emit one small JSON event and name a kind, never a
  recipient, so that the service decides who hears about it.

  Background:
    Given the notification service has one operator
    And the kind "cloudwatch.alarm" is registered

  Scenario: A valid event is accepted
    When an event of kind "cloudwatch.alarm" arrives with a title and the event id "e1"
    Then it is accepted for routing

  Scenario: An unknown kind is refused and recorded, not dropped silently
    When an event of kind "no.such.kind" arrives with a title and the event id "e2"
    Then no one is notified
    And the delivery log records the event as refused with the reason "unknown kind"

  Scenario: The same event twice is one event
    Given an event with id "e1" was accepted
    When an event with id "e1" arrives again
    Then nothing further is routed

  Scenario: A malformed event is refused and recorded
    When an event arrives with the event id "e3" and no title
    Then no one is notified
    And the delivery log records the event as refused with the reason "invalid event: title is required"

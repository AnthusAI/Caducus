Feature: Routing
  As the service, I turn one accepted event into one queued delivery per person
  and channel, so that each operator hears once.

  Scenario: An operator event reaches each operator once
    Given the notification service has 2 operators
    When an operator event arrives twice
    Then each operator has exactly one queued email delivery

  Scenario: A kind with no audience is visible
    Given the notification service has no operators
    When an operator event arrives
    Then the delivery log records "refused" with the reason "no audience"

  Scenario: A redelivered queue message makes no second delivery
    Given the notification service has one operator
    When the same queue message is handled twice
    Then the operator has exactly one queued email delivery

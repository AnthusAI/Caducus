Feature: Delivery log
  As an operator, I can answer "did this reach them, and if not why" from one
  record, and the record never holds an address.

  Scenario: A send is recorded from queue to delivery
    Given a notification is queued for a person by email
    When it is sent and the provider reports delivery
    Then the log shows the kind, the channel, a masked address, the provider id and "delivered"

  Scenario: A late delivery report does not hide a bounce
    Given a delivery is recorded as "bounced"
    When a "delivered" report for it arrives afterwards
    Then the delivery stays "bounced"

  Scenario: Choosing not to send is recorded too
    Given a person turned this kind off
    When an event of this kind is routed
    Then the log shows "skipped_preference" for that person

  Scenario: The log never holds an address
    Given a notification is queued for a person by email
    When it is sent and the provider reports delivery
    Then no stored row and no log line contains the address

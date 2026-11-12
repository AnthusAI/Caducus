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

  Scenario: A cost only ever grows
    Given a delivery on a channel that charges per request
    When a charge of 0.200 USD and then a charge of 0.010 USD are recorded
    Then the delivery shows a cost of 0.210 USD
    And no later report lowers it

  Scenario: One delivery is billed in one currency
    Given a delivery on a channel that charges per request
    When a charge of 0.200 USD and then a charge of 0.100 EUR are recorded
    Then the second charge is refused
    And the delivery still shows a cost of 0.200 USD

  Scenario: A withdrawal says who and when
    Given a delivery that was sent
    When it is withdrawn by "editor@example.org"
    Then the delivery shows "withdrawn", who withdrew it and when
    And a later report of "failed" does not change it

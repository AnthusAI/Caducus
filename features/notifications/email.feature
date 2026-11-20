Feature: Email channel
  As an operator, I get a readable HTML and plain-text email with no tracking
  pixel and no rewritten link.

  Background:
    Given the notification service has one operator

  Scenario: An email goes out once
    Given a queued email delivery
    When the sender handles it twice
    Then SES was called once
    And the log shows "sent" with the provider id

  Scenario: SES throttles
    Given a queued email delivery
    And SES answers with a throttling error twice
    When the sender handles it
    Then it retries with backoff of 1 and 2 seconds
    And SES accepted it once and the delivery is "sent"

  Scenario: A failed send can be retried
    Given a queued email delivery
    And SES refuses the first call
    When the sender handles it and fails
    And the sender handles it again
    Then SES accepted it once and the delivery is "sent"

  Scenario: Nothing in the mail tracks the reader
    When any notification email is rendered
    Then it has no tracking image and every link points at its real destination
    And it has a plain-text part

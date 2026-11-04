Feature: Triage content
  As an operator, each alarm message says what broke, where, the impact and the
  next step, with one link into the right AWS account, so that I do not have to
  log in to find out what happened.

  Background:
    Given the notification service has one operator

  Scenario: A known alarm
    Given the alarm "papyrus-monitoring-router-errors" is in the catalog
    When it fires
    Then the message names what broke, where, the impact and the next step
    And it links straight to that alarm in the right AWS account

  Scenario: The site is read from the alarm name
    Given the alarm "papyrus-newsletter-shipped-bounce-rate" is in the catalog
    When it fires
    Then the message says the site is "shipped"

  Scenario: An alarm with no notes still reaches me
    Given an alarm that is not in the catalog
    When it fires
    Then the message gives its name, its account and its reason
    And it is labelled "No triage notes yet"

  Scenario: The state is in words and a mark, never colour alone
    Given the alarm "papyrus-monitoring-router-errors" is in the catalog
    When it fires
    Then the subject starts with "[ALARM]"
    And the body says "ALARM" with a mark beside it

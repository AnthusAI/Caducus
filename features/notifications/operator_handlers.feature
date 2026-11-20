Feature: The operator listener's own health checks
  A host that runs the operator listener needs to know the listener itself still
  works. A heartbeat travels the real path to a mailbox nobody reads, and the host
  watches for the proof. A scheduled look at CloudWatch finds alarms that were
  already firing before the listener existed.

  Scenario: A heartbeat goes to the probe mailbox and never to an operator
    Given the notification service has one operator
    When a heartbeat is requested
    Then exactly one delivery is queued
    And that delivery goes to the probe mailbox and not to the operator

  Scenario: A delivered heartbeat is counted
    Given the notification service has one operator
    When SES reports a heartbeat delivered
    Then the metric "HeartbeatDelivered" is counted once

  Scenario: Ordinary mail is not counted as a heartbeat
    Given the notification service has one operator
    When SES reports an alarm email delivered
    Then no heartbeat metric is counted

  Scenario: A refused heartbeat send is counted
    Given the notification service has one operator
    When a heartbeat is requested and SES refuses to send it
    Then the metric "HeartbeatSendFailed" is counted

  Scenario: Reconcile announces an alarm that was already firing
    Given the notification service has one operator
    And CloudWatch says "papyrus-monitoring-router-errors" is in ALARM
    When the reconcile runs twice
    Then one email about "papyrus-monitoring-router-errors" is queued

  Scenario: Reconcile skips the listener's own alarms
    Given the notification service has one operator
    And CloudWatch says "papyrus-operator-deadman" is in ALARM
    When the reconcile runs ignoring the prefix "papyrus-operator-"
    Then no email is queued

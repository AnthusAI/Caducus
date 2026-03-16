Feature: Demo vertical slice
  As an on-call operator
  I want a story-first demo that turns noisy logs into ranked radar blips
  So that I can quickly decide what to investigate first

  Background:
    Given a local demo dataset file with HDFS-style log rows
    And a Caducus data directory is configured

  Scenario: Firehose logs become ranked radar output
    When I run the Caducus demo run command for group_id "hdfs-demo:DataNode"
    Then the command succeeds
    And Caducus writes one canonical event per log row
    And the radar output includes group "hdfs-demo:DataNode"
    And the radar output includes ranked blips with weight, temporal, and member count

  Scenario: Ingest-only mode stores canonical event shape
    When I run the Caducus demo ingest command with that input file
    Then the command succeeds
    And each stored event has id, timestamp, source, group_id, text, and metadata

  Scenario: Groups command reveals discoverable analysis targets
    When I run the Caducus demo ingest command with that input file
    And I run the Caducus groups command
    Then the command succeeds
    And the CLI lists at least the group IDs from the ingested file

  Scenario: Analyze uses previously stored events
    Given canonical event records already exist for group_id "hdfs-demo:DataNode" in the data directory
    When I run the Caducus analyze command for group_id "hdfs-demo:DataNode"
    Then the command succeeds
    And the radar output includes group "hdfs-demo:DataNode"
    And the radar output includes ranked blips with weight, temporal, and member count

  Scenario: Unknown group returns a clear operator message
    Given canonical event records already exist for group_id "hdfs-demo:DataNode" in the data directory
    When I run the Caducus analyze command for group_id "hdfs-demo:DoesNotExist"
    Then the command succeeds
    And the CLI says no events were found for group_id "hdfs-demo:DoesNotExist"

  Scenario: Source column controls grouping prefix
    Given a local demo dataset file with a custom source column
    When I run the Caducus demo ingest command with that input file
    And I run the Caducus groups command
    Then the command succeeds
    And the CLI includes group_id "bgl-demo:KERNEL"

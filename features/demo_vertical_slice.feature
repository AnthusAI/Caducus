Feature: Demo vertical slice
  As an operator
  I want to run Caducus on a local demo log dataset and see structured memory topics
  So that I can verify logs-in to memories-out without live AWS sources

  Scenario: Run demo analysis from a local demo dataset file
    Given a local demo dataset file with HDFS-style log rows
    And a Caducus data directory is configured
    And the biblicus config subtree specifies reinforcement_memory data_dir and vector_store
    When I run the Caducus demo run command with that input file and a group_id
    Then Caducus writes canonical event records for each log row
    And Caducus maps those events to Biblicus TimestampedText
    And Caducus invokes Biblicus ReinforcementMemory ingest and analyze for that group_id
    And the CLI shows structured memory topics with label, memory_tier, lifecycle_tier, member_count

  Scenario: Demo ingest only writes canonical events
    Given a local demo dataset file with HDFS-style log rows
    And a Caducus data directory is configured
    When I run the Caducus demo ingest command with that input file
    Then Caducus writes one canonical event per log row
    And each event has id, timestamp, source, group_id, text, and metadata

  Scenario: Analyze reads from stored canonical events
    Given canonical event records already exist for a group_id in the data directory
    And the biblicus config subtree is present
    When I run the Caducus analyze command for that group_id
    Then Caducus reads canonical events from storage
    And Caducus runs Biblicus reinforcement memory analysis for that group_id
    And the CLI outputs topic lines in the stable shape

  Scenario: Groups command lists group IDs after ingest
    Given a local demo dataset file with HDFS-style log rows
    And a Caducus data directory is configured
    When I run the Caducus demo ingest command with that input file
    And I run the Caducus groups command
    Then the CLI lists at least the group IDs from the ingested file

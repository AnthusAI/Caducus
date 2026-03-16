Feature: Configured demo story
  As an operator integrating Caducus
  I want behavior-driven config coverage for storage and analysis paths
  So that I can trust the CLI will honor YAML configuration in real usage

  Scenario: YAML configuration controls both event and analysis storage locations
    Given a local demo dataset file with HDFS-style log rows
    And a Caducus data directory is configured
    And a Caducus YAML config file sets data_dir and biblicus reinforcement-memory paths
    When I run the Caducus demo run command with config for group_id "hdfs-demo:DataNode"
    Then the command succeeds
    And the configured Caducus data_dir contains canonical events
    And the configured Biblicus analysis and vector directories exist

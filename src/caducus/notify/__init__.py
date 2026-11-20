"""Caducus notifications: decide what deserves attention and get it there.

This package is standard library plus boto3 (imported lazily where AWS is used);
It runs inside a host's own deployment; PyYAML is only needed if a host writes its
config in YAML. Hosts add kinds and an alarm catalog through `CADUCUS_CONFIG_DIR`.
It imports nothing from the log-radar side of Caducus.
"""

"""Caducus notifications: decide what deserves attention and get it there.

This package is standard library plus boto3 (imported lazily where AWS is used);
PyYAML is only needed to read the YAML config at development time, because the
deployed Lambdas read the JSON the CDK app builds from it. It imports nothing
from the log-radar side of Caducus.
"""

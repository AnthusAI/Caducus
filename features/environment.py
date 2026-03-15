"""Behave environment for Caducus features."""

import os
import tempfile
from pathlib import Path


def before_scenario(context, scenario):
    """Create a temp data dir for each scenario."""
    context.tmpdir = tempfile.mkdtemp(prefix="caducus_")
    context.data_dir = os.path.join(context.tmpdir, "data")


def after_scenario(context, scenario):
    """Clean up temp dir."""
    if getattr(context, "tmpdir", None):
        import shutil
        shutil.rmtree(context.tmpdir, ignore_errors=True)

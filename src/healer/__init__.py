"""Self-healing data pipeline.

A small, honest reference implementation of a pipeline that detects its own
data-quality failures, diagnoses the root cause and applies a remediation
playbook before giving up and escalating to a human.
"""

__version__ = "0.1.0"

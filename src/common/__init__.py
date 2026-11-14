"""Code shared by every service: configuration, message schemas, logging, Kafka/DB helpers.

Sub-modules are imported explicitly by callers. Nothing is imported here, so the light
``fraud-app`` image (no PySpark) can use ``src.common`` without pulling in Spark.
"""

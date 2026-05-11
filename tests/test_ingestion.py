"""Smoke tests for ingestion modules.

These tests verify module structure, constants, and configuration without
hitting GCP. They run fast and catch the most common breakage:
- Module imports fail (missing dependency, syntax error)
- Constants get accidentally renamed or removed

CLI behavior (--ingestion-date validation, GCS path conventions) is
covered by tests/test_ingestion_paths.py via subprocess invocation.
"""
import re

import pytest


class TestTaxiIngestion:
    """Tests for ingestion.batch.taxi_ingestion module."""

    def test_module_imports(self):
        """Module must import without error."""
        from ingestion.batch import taxi_ingestion
        assert taxi_ingestion is not None

    def test_required_constants_exist(self):
        """Module must define expected configuration constants."""
        from ingestion.batch import taxi_ingestion
        assert hasattr(taxi_ingestion, 'PROJECT_ID')
        assert hasattr(taxi_ingestion, 'RAW_BUCKET')
        assert hasattr(taxi_ingestion, 'PUBLIC_DATASET')

    def test_project_id_format(self):
        """PROJECT_ID matches GCP project naming rules."""
        from ingestion.batch.taxi_ingestion import PROJECT_ID
        assert isinstance(PROJECT_ID, str)
        assert re.match(r'^[a-z][a-z0-9\-]{4,29}$', PROJECT_ID), \
            f"Invalid GCP project ID format: {PROJECT_ID}"

    def test_parse_args_function_exists(self):
        """Module must expose parse_args for CLI arg handling."""
        from ingestion.batch import taxi_ingestion
        assert hasattr(taxi_ingestion, 'parse_args')
        assert callable(taxi_ingestion.parse_args)


class TestWeatherIngestion:
    """Tests for ingestion.batch.weather_ingestion module."""

    def test_module_imports(self):
        from ingestion.batch import weather_ingestion
        assert weather_ingestion is not None

    def test_required_constants_exist(self):
        from ingestion.batch import weather_ingestion
        assert hasattr(weather_ingestion, 'PROJECT_ID')
        assert hasattr(weather_ingestion, 'RAW_BUCKET')
        assert hasattr(weather_ingestion, 'API_URL')

    def test_api_url_is_open_meteo(self):
        """Verify we're hitting the expected weather API."""
        from ingestion.batch.weather_ingestion import API_URL
        assert 'open-meteo.com' in API_URL
        assert API_URL.startswith('https://')


class TestLoadToBigQuery:
    """Tests for ingestion.batch.load_to_bigquery module."""

    def test_module_imports(self):
        from ingestion.batch import load_to_bigquery
        assert load_to_bigquery is not None

    def test_required_constants_exist(self):
        from ingestion.batch import load_to_bigquery
        assert hasattr(load_to_bigquery, 'PROJECT_ID')
        assert hasattr(load_to_bigquery, 'RAW_DATASET')
        assert hasattr(load_to_bigquery, 'RAW_BUCKET')


class TestSparkTransform:
    """Tests for transform.spark_transform module."""

    def test_script_file_exists(self):
        """Spark script must exist at expected location."""
        from pathlib import Path
        script = Path('transform/spark_transform.py')
        assert script.exists(), f"Missing: {script.absolute()}"
        assert script.stat().st_size > 100, "Script suspiciously small"

    def test_script_has_main_block(self):
        """Spark script must have if __name__ == '__main__' for Dataproc."""
        with open('transform/spark_transform.py', encoding='utf-8') as f:
            content = f.read()
        assert "__main__" in content
        assert "argparse" in content, "Should use argparse for Dataproc args"
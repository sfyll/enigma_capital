"""Basic import tests to verify package structure."""


def test_import_infrastructure():
    """Test that infrastructure module can be imported."""
    from infrastructure import api_secret_getter, log_handler, runner_base

    assert api_secret_getter is not None
    assert log_handler is not None
    assert runner_base is not None


def test_import_utilities():
    """Test that utilities module can be imported."""
    from utilities import encryptor, get_process_name, request_handler, telegram_handler

    assert encryptor is not None
    assert get_process_name is not None
    assert request_handler is not None
    assert telegram_handler is not None


def test_import_account_data_fetcher():
    """Test that account_data_fetcher module can be imported."""
    from account_data_fetcher.data_aggregator import data_aggregator
    from account_data_fetcher.exchanges import exchange_base

    assert exchange_base is not None
    assert data_aggregator is not None


def test_import_monitor():
    """Test that monitor module can be imported."""
    from monitor import runner

    assert runner is not None


def test_core_dependencies():
    """Test that core dependencies are available."""
    import aiohttp
    import matplotlib
    import pandas
    import requests

    assert pandas is not None
    assert aiohttp is not None
    assert requests is not None
    assert matplotlib is not None

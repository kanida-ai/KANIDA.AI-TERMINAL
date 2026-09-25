"""Shared test isolation: no test may read the machine's real NSE bhavcopy archive (db/fo_bhavcopy.db).
Tests that need a verified calendar build their own small archive and point PILOT_FO_BHAVCOPY_DB at it."""
import os
import pytest


@pytest.fixture(autouse=True)
def _no_real_bhavcopy(tmp_path,monkeypatch):
 monkeypatch.setenv('PILOT_FO_BHAVCOPY_DB',str(tmp_path/'no-bhavcopy.db'))

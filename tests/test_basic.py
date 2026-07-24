# tests/test_basic.py
def test_pipeline_imports():
    """Verify core dependencies import cleanly."""
    import json
    assert json is not None
import pytest
import commonpy.singleton

@pytest.fixture(scope="function", autouse=True)
def reset_singleton(pytestconfig):
    try:
        commonpy.singleton.Singleton._instances.clear()
    except AttributeError:
        pass


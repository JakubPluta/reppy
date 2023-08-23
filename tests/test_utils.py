import pytest

from reppy.ext import NotSupportedFileFormat
from reppy.utils import check_extension


@pytest.mark.parametrize(
    "ext,expected, exc",
    [
        ("csv", "csv", False),
        ("json", "json", False),
        (".csv", "csv", False),
        (".json", "json", False),
        ("somefile.csv", "csv", False),
        ("somefile", None, True),
        ("somefile.casv", None, True),
    ],
)
def test_should_properly_check_extension(ext, expected, exc):
    if exc:
        with pytest.raises(NotSupportedFileFormat) as e:
            assert check_extension(ext) is None
    else:
        assert check_extension(ext) == expected

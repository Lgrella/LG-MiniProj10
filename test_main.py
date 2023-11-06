"""
Test goes here

"""

import os
from mylib.lib import (
    extract
)

def test_extract():
    file_path = extract()
    assert os.path.exists(file_path) is True

if __name__ == "__main__":
    test_extract()

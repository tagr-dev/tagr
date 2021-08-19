import unittest
import json
import numpy as np

from tagr.utils import NpEncoder

PYTHON_DATA = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
NUMPY_DATA = np.array([0, 1, np.int32(2), np.int64(3), np.float32(4), np.float64(5)])


class UtilsTest(unittest.TestCase):
    def test_np_encoder(self):
        expected_result = json.dumps(PYTHON_DATA, sort_keys=True)
        np_json = json.dumps(NUMPY_DATA, sort_keys=True, cls=NpEncoder)

        self.assertEqual(expected_result, np_json)

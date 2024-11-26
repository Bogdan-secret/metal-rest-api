import unittest
import os
import json
from job import save_data


class SaveData(unittest.TestCase):

    def test_idempotency(self):
        test_date = "2024-10-11"
        test_feature = "Gold"
        save_data(test_date, test_feature)

        storage_path = f"./path/to/my_dir/raw/{test_feature}/metal_data_{test_date}.json"
        self.assertTrue(os.path.isfile(storage_path))

        save_data(test_date, test_feature)

        files = os.listdir(os.path.dirname(storage_path))
        self.assertEqual(len(files), 2)

        expected_filename = f"metal_data_{test_date}.json"
        self.assertIn(expected_filename, files)

    def test_data_format(self):
        test_date = "2024-10-11"
        test_feature = "Gold"
        save_data(test_date, test_feature)

        file_path = f"./path/to/my_dir/raw/{test_feature}/metal_data_{test_date}.json"

        with open(file_path, 'r') as f:
            data = json.load(f)

        self.assertIsInstance(data, list)


unittest.main()

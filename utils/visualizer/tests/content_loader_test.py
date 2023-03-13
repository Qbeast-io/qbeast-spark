import os
import unittest

from qviz.content_loader import (
    extract_metadata_from_json_files,
    extract_metadata_from_checkpoint,
    addFiles_from_checkpoint_file,
    addFiles_from_json_log_files,
    extract_checkpoint_version,
    process_table_delta_log
)


class TestContentLoader(unittest.TestCase):
    def setUp(self) -> None:
        root = os.getcwd()
        self.table_path = os.path.join(root, "resources", "test_table")
        log_path = os.path.join(self.table_path, "_delta_log")
        # remove_log = os.path.join(root, "resources/table_with_remove", "_delta_log")

        self.first_log_file_path = os.path.join(log_path, "00000000000000000000.json")
        self.checkpoint_file_path = os.path.join(log_path, "00000000000000000001.checkpoint.parquet")
        self.json_log_files_with_remove = [
            os.path.join(log_path, "00000000000000000000.json"),
            os.path.join(log_path, "00000000000000000001.json")
        ]
        self.last_checkpoint = os.path.join(log_path, "_last_checkpoint")

    def check_empty_revision(self, transformers: list[str]) -> None:
        self.assertEqual(len(transformers), 2)
        self.assertEqual(len(set(transformers)), 1)
        self.assertEqual(set(transformers).pop(), 'io.qbeast.core.transform.EmptyTransformer')

    def test_metadata_extraction_from_json_files(self):
        metadata = extract_metadata_from_json_files([self.first_log_file_path], "qbeast.revision.0")
        transformers = list(map(lambda t: t['className'], metadata['columnTransformers']))
        self.check_empty_revision(transformers)

    def test_metadata_extraction_from_checkpoint_file(self):
        metadata = extract_metadata_from_checkpoint(self.checkpoint_file_path, "qbeast.revision.0")
        transformers = list(map(lambda t: t['className'], metadata['columnTransformers']))
        self.check_empty_revision(transformers)

    def test_version_extraction(self):
        version = extract_checkpoint_version(self.last_checkpoint)
        self.assertTrue(version == 1)

    def test_addFile_extraction_from_checkpoint_file(self):
        add_files = addFiles_from_checkpoint_file(self.checkpoint_file_path, "1")

        def is_valid_add_file(add_file: dict) -> bool:
            return 'path' in add_file and type(add_file['tags']) == dict

        self.assertTrue(all(is_valid_add_file(add_file) for add_file in add_files))

    def test_addFile_extraction_from_json_log_files(self):
        (add_files, remove_paths) = addFiles_from_json_log_files(self.json_log_files_with_remove, '1')
        self.assertTrue(len(add_files) > len(remove_paths))

        valid_add_files = list(filter(lambda add_file: add_file['path'] not in remove_paths, add_files))
        self.assertTrue(len(valid_add_files) == 8)

    def test_log_processing(self):
        (add_files, _) = process_table_delta_log(self.table_path, '1')
        self.assertTrue(all(['tags' in a for a in add_files]))

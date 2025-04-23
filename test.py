import traceback
import contextlib
import io
import os
import hashlib
import tempfile
import unittest
from pathlib import Path

from backup import backup, backup2, Results, _listdir

def hash_directory(root, ignore_empty_dirs=False):
	hasher = hashlib.sha256()
	for dir, dirnames, filenames in sorted(os.walk(root)):
		dirnames.sort()
		if ignore_empty_dirs and not filenames:
			continue
		dir_path = os.path.relpath(dir, root)
		hasher.update(dir_path.encode())
		for file in sorted(filenames):
			file_path = os.path.join(dir, file)
			try:
				with open(file_path, "rb") as f:
					while True:
						buf = f.read(4096)
						if not buf:
							break
						hasher.update(buf)
			except OSError as e:
				print(f"Error hashing {file_path}: {e}")
	return hasher.hexdigest()

def create_file_structure(root_dir : Path, structure : dict):
    """Recursively creates a directory structure with files."""
    root_dir.mkdir(parents=True, exist_ok=True)
    for name, content in structure.items():
        filepath = root_dir / name
        if isinstance(content, dict):
            create_file_structure(filepath, content)
        elif content is None:  # Create an empty file
            filepath.touch()
        else:  # Create a file with content
            filepath.write_text(content)

class TestBackup(unittest.TestCase):
	def test_listdir(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			test_root = Path(temp_root)
			file_structure = {
				"a": {
					"aa": {
						"aaa": {
							"1.txt": None,
						},
						"aab": {
							"12.txt": None,
						},
						"aac": {
							"21.txt": None,
						},

						"1.txt": None,
					},
					"ab": {
						"aba": {
							"1.jpg": None,
						},
						"abb": {
							"12.jpg": None,
						},
						"abc": {
							"21.jpg": None,
						},

						"1.jpg": None,
					},
					"ac": {
						"aca": {
							"1.html": None,
						},
						"acb": {
							"12.html": None,
						},
						"acc": {
							"21.html": None,
						},

						"1.html": None,
					},

					"1.txt": None,
					"1.jpg": None,
					"1.html": None,
				},
				"b": {
					"ba": {
					},
					"bb": {
					},
					"bc": {
					},
				},
				"c": {
					"ca": {},
					"cb": {},
					"cc": {},
				},
			}
			create_file_structure(test_root, file_structure)

			###################################################################################

			files = _listdir(
				root = test_root,
				include = "1.???".split(),
				exclude = "b/ c/".split(),
				ignore_missing = True,
			)
			files_expected = [
				"a/aa/1.txt",
				"a/aa/aaa/1.txt",
				"a/ab/1.jpg",
				"a/ab/aba/1.jpg",
				"a/1.txt",
				"a/1.jpg",
			]
			self.assertEqual(
				sorted(files.relpath_stats.keys()),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)
			self.assertEqual(files._scan_count, 14)

			###################################################################################

			files = _listdir(
				root = test_root,
				include = "./a/a?/a?b".split(),
				exclude = "b/ c/".split(),
				ignore_missing = True,
			)
			files_expected = [
				"a/aa/aab/12.txt",
				"a/ab/abb/12.jpg",
				"a/ac/acb/12.html",
			]
			self.assertEqual(
				sorted(files.relpath_stats.keys()),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)
			self.assertEqual(files._scan_count, 8)

			###################################################################################

			files = _listdir(
				root = test_root,
				include = "a?b/".split(),
				exclude = "b/ c/".split(),
				ignore_missing = True,
				all_dir_patterns_are_paths = False
			)
			files_expected = [
				"a/aa/aab/12.txt",
				"a/ab/abb/12.jpg",
				"a/ac/acb/12.html",
			]
			self.assertEqual(
				sorted(files.relpath_stats.keys()),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)
			self.assertEqual(files._scan_count, 14)

if __name__ == "__main__":
	unittest.main()

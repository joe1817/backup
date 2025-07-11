import traceback
import contextlib
import io
import os
import hashlib
import tempfile
import unittest
from pathlib import Path

from backup import backup, backup2, Results, _Filter, _listdir, _operations, _move

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
	def test_filter(self):
		f = _Filter("- **/.git/ **/__pycache__/ + **/*/ **/*")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("a/b"))
		self.assertTrue(f.filter("a/b/c"))
		self.assertFalse(f.filter(".git/"))
		self.assertFalse(f.filter("a/.git/"))
		self.assertFalse(f.filter("a/b/.git/"))
		self.assertFalse(f.filter("__pycache__/"))
		self.assertFalse(f.filter("a/__pycache__/"))
		self.assertFalse(f.filter("a/b/__pycache__/"))

		f = _Filter("+ places.sqlite key4.db logins.json cookies.sqlite prefs.js - **/*/ **/*")
		self.assertTrue(f.filter("places.sqlite"))
		self.assertTrue(f.filter("key4.db"))
		self.assertTrue(f.filter("logins.json"))
		self.assertTrue(f.filter("cookies.sqlite"))
		self.assertTrue(f.filter("prefs.js"))
		self.assertFalse(f.filter("storage.sqlite"))
		self.assertFalse(f.filter("storage/"))

		f = _Filter("+ audio/music/**/*.flac - **/*/ **/*")
		self.assertTrue(f.filter("audio/"))
		self.assertTrue(f.filter("audio/music/"))
		self.assertTrue(f.filter("audio/music/OST/"))
		self.assertTrue(f.filter("audio/music/OST/Star Wars/"))
		self.assertTrue(f.filter("audio/music/OST/Star Wars/Duel of the Fates.flac"))
		self.assertFalse(f.filter("video/"))
		self.assertFalse(f.filter("audio/audiobooks/"))
		self.assertFalse(f.filter("audio/music/OST/Star Wars/cover.jpg"))

		f = _Filter("- audio/music/**/*.wav + **/*/ **/*")
		self.assertTrue(f.filter("audio/"))
		self.assertTrue(f.filter("audio/music/"))
		self.assertTrue(f.filter("audio/music/OST/"))
		self.assertTrue(f.filter("audio/music/OST/Titanic/"))
		self.assertFalse(f.filter("audio/music/OST/Titanic/My Heart Will Go On (Recorder Cover).wav"))
		self.assertTrue(f.filter("video/"))
		self.assertTrue(f.filter("audio/audiobooks/"))
		self.assertTrue(f.filter("audio/music/OST/Titanic/cover.jpg"))

		f = _Filter("- ./**/foo/bar/ '**/eggs and spam/' \"Joe's Files/\" + **/*/ **/*")
		self.assertFalse(f.filter("foo/bar/"))
		self.assertFalse(f.filter("eggs and spam/"))
		self.assertFalse(f.filter("Joe's Files/"))

		f = _Filter("+ * - a/ b/a/ + b/*/ - **/x + ?/**/* - **/*")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("b/a"))
		self.assertTrue(f.filter("b/a/a"))
		self.assertTrue(f.filter("b/b/"))
		self.assertFalse(f.filter("a/"))
		self.assertFalse(f.filter("b/a/"))
		self.assertFalse(f.filter("b/b/x"))
		self.assertTrue(f.filter("b/y"))
		self.assertTrue(f.filter("b/b/y"))
		self.assertFalse(f.filter("aa/y"))

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
						"1.txt": None,
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
				filter = "- b/ c/ + **/*/ **/1.???"
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

			###################################################################################

			files = _listdir(
				root = test_root,
				filter = "+ a/a?/a?b/*",
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

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_operations(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			test_root = Path(temp_root)
			file_structure = {
				"a": {
					"a": {
						"1.txt": None
					}
				},
				"b": {
					"A": {
						"1.txt": None
					},
					"empty": {
						"empty2": {}
					}
				}
			}
			create_file_structure(test_root, file_structure)

			src_root = os.path.join(test_root, "a")
			dst_root = os.path.join(test_root, "b")
			src_files = _listdir(
				root = src_root
			)
			dst_files = _listdir(
				root = dst_root
			)
			trash_dir = "/"
			rename_threshold = 1000
			metadata_only = True

			actual = list(x[4] for x in _operations(
				src_files        = src_files,
				dst_files        = dst_files,
				src_root         = src_root,
				dst_root         = dst_root,
				trash_dir        = trash_dir,
				rename_threshold = rename_threshold,
				metadata_only    = metadata_only
			))
			expected = [
				f"- {os.path.join('A','1.txt')}",
				f"+ {os.path.join('a','1.txt')}",
				f"- {os.path.join('empty','empty2') + os.sep}"
			]
			self.assertTrue(actual == expected)

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_move(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			test_root = Path(temp_root)
			file_structure = {
				"a": {
					"b": {
						"1.txt": None
					}
				}
			}
			create_file_structure(test_root, file_structure)

			src = os.path.join(test_root, "a", "b", "1.txt")
			dst = os.path.join(test_root, "A", "B", "2.txt")
			_move(src, dst)

			self.assertEqual(os.listdir(test_root), ["A"])

			src = os.path.join(test_root, "A", "B", "2.txt")
			dst = os.path.join(test_root, "a", "b", "2.txt")
			_move(src, dst)

			self.assertEqual(os.listdir(test_root), ["a"])

			src = os.path.join(test_root, "a", "b", "2.txt")
			dst = os.path.join(test_root, "a", "B", "2.txt")
			_move(src, dst)

			self.assertEqual(os.listdir(test_root), ["a"])

if __name__ == "__main__":
	try:
		unittest.main()
	except SystemExit as e:
		pass

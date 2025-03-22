import io
import os
import time
import contextlib
import hashlib
import tempfile
import traceback
import unittest
import doctest
from pathlib import Path

import psync

def hash_directory(root:Path, *, follow_links:bool=False, ignore_empty_dirs:bool=False, verbose:bool=False):
	if verbose:
		print("--- Hash Start ---")
	hasher = hashlib.sha256()
	for dir, dirnames, filenames in os.walk(root, followlinks=follow_links):
		if ignore_empty_dirs and not filenames:
			continue
		dirnames.sort(key=lambda x: (os.path.normcase(x), x))
		filenames.sort(key=lambda x: (os.path.normcase(x), x))
		dir_relpath = os.path.normcase(os.path.relpath(dir, root))
		hasher.update(dir_relpath.encode())
		if verbose:
			print(dir_relpath)
		for file in filenames:
			file_path = os.path.join(dir, file)
			file_relpath = os.path.normcase(os.path.relpath(file_path, root))
			hasher.update(file_relpath.encode())
			if verbose:
				print(file_relpath)
			try:
				with open(file_path, "rb") as f:
					while True:
						buf = f.read(4096)
						if not buf:
							break
						hasher.update(buf)
						if verbose:
							print(buf)
			except OSError as e:
				print(f"Error hashing {file_path}: {e}")
	if verbose:
		print("--- Hash End ---")
	return hasher.hexdigest()

def create_file_structure(root_dir:Path, structure:dict, *, _delay:float = 0.01):
	'''Recursively creates a directory structure with files.'''
	root_dir.mkdir(parents=True, exist_ok=True)
	for name, content in structure.items():
		file_path = root_dir / name
		if isinstance(content, Path):
			# create symlink
			os.symlink(content, file_path)
		elif isinstance(content, dict):
			# create dir
			create_file_structure(file_path, content, _delay=0)
		elif isinstance(content, (tuple, list)):
			# Create file with modtime and content
			file_path.write_text(content[0] or "")
			mtime = float(content[1])
			os.utime(file_path, (mtime, mtime))
		elif content is None:
			# Create an empty file
			file_path.touch()
		else:
			# Create a file with content
			file_path.write_text(content)
	if _delay:
		# delay so filesystem cache can update (changes to modtimes), TODO not sure if this actually works
		time.sleep(_delay)

def load_tests(loader, tests, ignore):
	tests.addTests(doctest.DocTestSuite(psync))
	return tests

class TestBackup(unittest.TestCase):
	def test_filter(self):
		f = psync._Filter("+ **")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("a/b"))
		self.assertTrue(f.filter("a/b/c"))
		self.assertTrue(f.filter(".git/"))
		self.assertTrue(f.filter("a/.git/"))
		self.assertTrue(f.filter("a/b/.git/"))
		self.assertTrue(f.filter("__pycache__/"))
		self.assertTrue(f.filter("a/__pycache__/"))
		self.assertTrue(f.filter("a/b/__pycache__/"))

		f = psync._Filter("+ **/*")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("a/b"))
		self.assertTrue(f.filter("a/b/c"))
		self.assertTrue(f.filter(".git/"))
		self.assertTrue(f.filter("a/.git/"))
		self.assertTrue(f.filter("a/b/.git/"))
		self.assertTrue(f.filter("__pycache__/"))
		self.assertTrue(f.filter("a/__pycache__/"))
		self.assertTrue(f.filter("a/b/__pycache__/"))

		f = psync._Filter("- **/.*/ **/__pycache__/ + **/*/ **/*")
		self.assertTrue(f.filter("a"))
		self.assertTrue(f.filter("a/b"))
		self.assertTrue(f.filter("a/b/c"))
		self.assertFalse(f.filter(".git/"))
		self.assertFalse(f.filter("a/.git/"))
		self.assertFalse(f.filter("a/b/.git/"))
		self.assertFalse(f.filter("__pycache__/"))
		self.assertFalse(f.filter("a/__pycache__/"))
		self.assertFalse(f.filter("a/b/__pycache__/"))

		f = psync._Filter("+ places.sqlite key4.db logins.json cookies.sqlite prefs.js - **/*/ **/*")
		self.assertTrue(f.filter("places.sqlite"))
		self.assertTrue(f.filter("key4.db"))
		self.assertTrue(f.filter("logins.json"))
		self.assertTrue(f.filter("cookies.sqlite"))
		self.assertTrue(f.filter("prefs.js"))
		self.assertFalse(f.filter("storage.sqlite"))
		self.assertFalse(f.filter("storage/"))

		f = psync._Filter("+ audio/music/**/*.flac - **/*/ **/*")
		self.assertTrue(f.filter("audio/"))
		self.assertTrue(f.filter("audio/music/"))
		self.assertTrue(f.filter("audio/music/OST/"))
		self.assertTrue(f.filter("audio/music/OST/Star Wars/"))
		self.assertTrue(f.filter("audio/music/OST/Star Wars/Duel of the Fates.flac"))
		self.assertFalse(f.filter("video/"))
		self.assertFalse(f.filter("audio/audiobooks/"))
		self.assertFalse(f.filter("audio/music/OST/Star Wars/cover.jpg"))

		f = psync._Filter("- audio/music/**/*.wav + **/*/ **/*")
		self.assertTrue(f.filter("audio/"))
		self.assertTrue(f.filter("audio/music/"))
		self.assertTrue(f.filter("audio/music/OST/"))
		self.assertTrue(f.filter("audio/music/OST/Titanic/"))
		self.assertFalse(f.filter("audio/music/OST/Titanic/My Heart Will Go On (Recorder Cover).wav"))
		self.assertTrue(f.filter("video/"))
		self.assertTrue(f.filter("audio/audiobooks/"))
		self.assertTrue(f.filter("audio/music/OST/Titanic/cover.jpg"))

		f = psync._Filter("- ./**/foo/bar/ '**/eggs and spam/' \"Joe's Files/\" + **/*/ **/*")
		self.assertFalse(f.filter("foo/bar/"))
		self.assertFalse(f.filter("eggs and spam/"))
		self.assertFalse(f.filter("Joe's Files/"))

		f = psync._Filter("+ * - a/ b/a/ + b/*/ - **/x + ?/**/* - **/*")
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

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_scandir(self):
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

			files = psync._scandir(
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
				sorted(files.relpath_to_stats.keys()),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

			################################################################################

			files = psync._scandir(
				root = test_root,
				filter = "+ a/a?/a?b/*",
			)
			files_expected = [
				"a/aa/aab/12.txt",
				"a/ab/abb/12.jpg",
				"a/ac/acb/12.html",
			]
			self.assertEqual(
				sorted(files.relpath_to_stats.keys()),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

			################################################################################

			file_structure = {
				"f": {
					"1.txt": None,
					"2.txt": None,
				},
				"e": {
					"1.txt": None,
					"2.txt": None,
				},
			}
			create_file_structure(test_root, file_structure)
			file_structure = {
				"e": {
					"ea": test_root / "f",
				},
				"d": test_root / "e",
			}
			create_file_structure(test_root, file_structure)

			files = psync._scandir(
				root = test_root / "d",
				filter = "+ **/*/ **/*",
			)
			files_expected = [
				"1.txt",
				"2.txt",
			]
			self.assertEqual(
				sorted(files.relpath_to_stats.keys()),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

			################################################################################

			files = psync._scandir(
				root = test_root / "d",
				filter = "+ **/*/ **/*",
				follow_symlinks = True,
			)
			files_expected = [
				"1.txt",
				"2.txt",
				"ea/1.txt",
				"ea/2.txt",
			]
			self.assertEqual(
				sorted(files.relpath_to_stats.keys()),
				sorted(f.replace("/", os.sep) for f in files_expected)
			)

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_operations(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			test_root = Path(temp_root)
			file_structure = {
				"a": {
					"a": {
						"1.txt": None
					},
				},
				"b": {
					"A": {
						"1.txt": None
					},
					"empty": {
						"empty2": {
						},
					},
				},
				"c": {
					"aa": {
						"1.txt": None
					},
				},
			}
			create_file_structure(test_root, file_structure)

			a_root = test_root / "a"
			b_root = test_root / "b"
			c_root = test_root / "c"
			a_files = psync._scandir(
				root = a_root
			)
			b_files = psync._scandir(
				root = b_root
			)
			c_files = psync._scandir(
				root = c_root
			)

			actual = list(x[4] for x in psync._operations(
				a_files,
				b_files,
				trash_root	     = Path("/"),
				rename_threshold = 0,
				metadata_only	 = True
			))

			if "nt" in os.name:
				expected = [
					f"- {os.path.join('empty','empty2') + os.sep}"
				]
			else:
				expected = [
					f"R {os.path.join('a','1.txt')} -> {os.path.join('A','1.txt')}"
					f"- {os.path.join('empty','empty2') + os.sep}"
				]
			self.assertEqual(actual, expected)

			################################################################################

			actual = list(x[4] for x in psync._operations(
				a_files,
				c_files,
				trash_root	   = Path("/"),
				rename_threshold = 1000,
				metadata_only	= True
			))
			expected = [
				f"- {os.path.join('aa','1.txt')}",
				f"+ {os.path.join('a','1.txt')}"
			]
			self.assertEqual(actual, expected)

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

			src = test_root / "a" / "b" / "1.txt"
			dst = test_root / "A" / "B" / "2.txt"
			psync._move(src, dst, delete_empty_dirs_under=test_root)
			self.assertEqual(os.listdir(test_root / "A" / "B"), ["2.txt"])

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def test_backup(self):
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			test_root = Path(temp_root)
			file_structure = {
				"src": {
					"a": {
						"a": {
							"1.txt": None,
						},
						"1.txt": "new info",
						"2.txt": None,
					},
					"b": {
						"1.txt": None,
					},
					"empty": {
						"empty": {
						},
					},
					"empty2": {
					},
				},
				"dst": {
					"A": {
						"1.txt": ("old info", 1),
						"3.txt": None,
					},
					"Empty": {
						"empty": {
						},
					},
					"empty3": {
						"empty": {
						},
					},
				},
				"windows_expected_trash": {
					"A": {
						"3.txt": None,
					},
				},
				"linux_expected_trash": {
					"A": {
						"1.txt": ("old info", 1),
						"3.txt": None,
					},
				},
			}
			create_file_structure(test_root, file_structure)
			src = test_root / "src"
			dst = test_root / "dst"
			hash_src_old = hash_directory(src)
			hash_dst_old = hash_directory(dst)

			################################################################################

			# test dry_run
			self.assertFalse(hash_src_old == hash_dst_old)
			results = psync.sync(
				src,
				dst,
				trash = "auto",
				quiet = True,
				dry_run = True,
			)
			self.assertEqual(hash_directory(dst), hash_dst_old)

			################################################################################

			# test basic backup
			results = psync.sync(
				src,
				dst,
				trash = "auto",
				quiet = True
			)
			self.assertTrue(results.success)
			self.assertFalse(hash_directory(dst) == hash_dst_old)
			self.assertEqual(hash_directory(src), hash_directory(dst))
			if "nt" in os.name:
				self.assertEqual(hash_directory(results.trash_root), hash_directory(test_root / "windows_expected_trash"))
			else:
				self.assertEqual(hash_directory(results.trash_root), hash_directory(test_root / "linux_expected_trash"))

			################################################################################

			# test backup with symlink src
			file_structure = {
				"src2" : test_root / "src",
				"dst2" : {}
			}
			create_file_structure(test_root, file_structure)
			src2 = test_root / "src2"
			dst2 = test_root / "dst2"
			hash_dst2_old = hash_directory(dst2)
			results = psync.sync(
				src2,
				dst2,
				quiet = True,
			)
			self.assertFalse(hash_directory(dst2) == hash_dst2_old)
			self.assertEqual(hash_directory(src2), hash_directory(dst2))
			self.assertEqual(results.create_success, 4)
		assert not test_root.exists()

		################################################################################

		# test backup involving many overlapping file names
		with tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None) as temp_root:
			test_root = Path(temp_root)
			file_structure = {
				"dst": {
					"a1": {
						"a": (None, 1)
					},
					"b1": {},
					"c1": (None, 1),

					"b2": {
						"a": (None, 1)
					},
					"c2": {},
					"a2": (None, 1),

					"c3": {
						"a": (None, 1)
					},
					"a3": {},
					"b3": (None, 1),
				},
				"src": {
					"a1": {
						"a": None
					},
					"b1": {},
					"c1": None,

					"a2": {
						"a": None
					},
					"b2": {},
					"c2": None,

					"a3": {
						"a": None
					},
					"b3": {},
					"c3": None,
				},
			}
			create_file_structure(test_root, file_structure)
			src = test_root / "src"
			dst = test_root / "dst"
			results = psync.sync(
				src,
				dst,
				trash = "auto",
				quiet = True,
			)
			self.assertEqual(hash_directory(src), hash_directory(dst))

if __name__ == "__main__":
	try:
		unittest.main()
	except SystemExit as e:
		pass

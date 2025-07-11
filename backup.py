# Copyright (c) 2025 Joe Walter

# Terms used:
# "entry"    = file or directory
# "basename" = filename without the extension

import sys
import argparse
import os
import io
import glob
import re
import stat
import shutil
import logging
import tempfile
import time
import traceback
from fnmatch import fnmatch
from collections import Counter
from collections import namedtuple
from types import SimpleNamespace
from functools import lru_cache
from direntry_walk import direntry_walk

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Results:
	def __init__(self):
		self.create_success = 0
		self.rename_success = 0
		self.update_success = 0
		self.delete_success = 0
		self.create_error = 0
		self.rename_error = 0
		self.update_error = 0
		self.delete_error = 0
		self.byte_diff = 0

	@property
	def err_count(self):
		return self.create_error + self.rename_error + self.update_error + self.delete_error

def backup(src_root, dst_root, *, trash_root=None, filter="+ **/*/ **/*", ignore_hidden=False, rename_threshold=10000, metadata_only=False, dry_run=False, log_path="-", quiet=False, veryquiet=False):
	'''
	Copies new and updated files from `src_root` to `dst_root`, and optionally "deletes" files from `dst_root` if they are not present in `src_root` (they will be moved into `trash_root`, preserving directory structure). Furthermore, files that exist in `dst_root` but renamed in `src_root` may be renamed in `dst_root` to match. Candidates for rename are discovered by searching for files with an identical metadata signature, consisting of file size and modification time. These candidates must be above a minimum size threshold (`rename_threshold`) and have an unambiguously unique metadata signature within their respective root directories. The user is asked to confirm these renames before they are committed.

	Args
		src_root (str)           : The root directory to copy files from.
		dst_root (str)           : The root directory to copy files to.
		trash_root (bool or str) : The root directory to move 'extra' files (those that are in `dst_root` but not `src_root`). Must be on the same filesystem as `dst_root`. If set to `True`, then a directory will automatically be made next to `dst_root`. Extra files will not be moved if this argument is `None`. (Defaults to `None`.)

		filter (str)             : The filter to include/exclude files and directories. Similar to rsync, the format of the filter string is: (+ or -), followed by a list of one of more relative path patterns, and otionally repeat from the start. Including (+) or excluding (-) of entries is determined by the preceding symbol of the first matching pattern. Included files will be copied, while included directories will be searched. Each Pattern ending with "/" will apply to directories only. Otherise the pattern will apply only to files. (Defaults to "+ **/*/ **/*", which searches all directories and copies all files.)
		ignore_hidden (bool)     : Whether to skip hidden files by default. If `True`, then wildcards in glob patterns will not match entries beginning with a dot. However, globs containing a dot (e.g., "**/.*") will still match these entries. (Defaults to `False`.)
		rename_threshold (int)   : The minimum size in bytes needed to consider renaming files in `dst_root` that were renamed in `src_root`. Renamed files below this threshold will be simply deleted in `dst_root` and their replacements created. A value of `None` will mean no files in `dst_root` will be eligible for renaming. (Defaults to `10000`.)
		metadata_only (bool)     : Whether to use only metadata in determining which files in dst_root are the result of a rename. If set to False, `backup` will also compare the last 1kb of files. (Defaults to `False`.)
		dry_run (bool)           : Whether to hold off performing any operation that would make a filesystem change. Changes that would have occurred will still be printed to console. (Defaults to `False`.)

		log_path (str)           : File to write log messages to. A falsy value means no log will be created. A value of "-" means a tempfile will be used for the log, and it will be copied to the user's home directory after the backup is done. (Defaults to "-".)
		quiet (bool)             : Whether to forgo printing to stdout.
		veryquiet (bool)         : Whether to forgo printing to stdout and stderr.

	Console Output
		TODO

	Returns
		A `Results` object containing various statistics.
	'''
	with _LogManager(suppress_stdout=quiet, suppress_stderr=veryquiet) as log_manager:

		if veryquiet:
			quiet = True
		if dry_run and log_path == "-":
			log_path = None
		timestamp = str(int(time.time()*1000))
		if trash_root == True:
			trash_dir = os.path.join(os.path.dirname(dst_root), f"Trash.{timestamp}")
		elif trash_root:
			trash_dir = os.path.join(trash_root, timestamp)
		else:
			trash_dir = None

		if not isinstance(src_root, str):
			msg = f"Bad type for arg 'src_root' (expected str): {src_root}"
			raise TypeError(msg)
		if not isinstance(dst_root, str):
			msg = f"Bad type for arg 'dst_root' (expected str): {dst_root}"
			raise TypeError(msg)
		if trash_root is not None and not isinstance(trash_root, bool) and not isinstance(trash_root, str):
			msg = f"Bad type for arg 'trash_root' (expected bool or str): {trash_root}"
			raise TypeError(msg)
		if not isinstance(filter, str):
			msg = f"Bad type for arg 'filter' (expected str): {filter}"
			raise TypeError(msg)
		if not isinstance(ignore_hidden, bool):
			msg = f"Bad type for arg 'ignore_hidden' (expected bool): {ignore_hidden}"
			raise TypeError(msg)
		if rename_threshold is not None and not isinstance(rename_threshold, int):
			msg = f"Bad type for arg 'rename_threshold' (expected int): {rename_threshold}"
			raise TypeError(msg)
		if not isinstance(metadata_only, bool):
			msg = f"Bad type for arg 'metadata_only' (expected bool): {metadata_only}"
			raise TypeError(msg)
		if not isinstance(dry_run, bool):
			msg = f"Bad type for arg 'dry_run' (expected bool): {dry_run}"
			raise TypeError(msg)
		if log_path is not None and not isinstance(log_path, str):
			msg = f"Bad type for arg 'log_path' (expected str): {log_path}"
			raise TypeError(msg)
		if not isinstance(quiet, bool):
			msg = f"Bad type for arg 'quiet' (expected bool): {quiet}"
			raise TypeError(msg)
		if not isinstance(veryquiet, bool):
			msg = f"Bad type for arg 'veryquiet' (expected bool): {veryquiet}"
			raise TypeError(msg)

		if not os.path.isdir(src_root):
			msg = f"Chosen src_root is not a directory: {src_root}"
			raise ValueError(msg)
		if os.path.exists(dst_root) and not os.path.isdir(dst_root):
			msg = f"Chosen dst_root is not a directory: {dst_root}"
			raise ValueError(msg)
		if trash_root is not None and os.path.exists(trash_root):
			if os.stat(trash_root).st_dev != os.stat(dst_root).st_dev:
				msg = f"Chosen trash_root is not on the same filesystem as dst_root: {trash_root}"
				raise ValueError(msg)
			if os.path.exists(trash_dir) and not os.path.isdir(trash_dir):
				msg = f"Could not create trash folder: {trash_dir}"
				raise ValueError(msg)
		if rename_threshold is not None and rename_threshold < 0:
			msg = f"rename_threshold must be non-negative: {rename_threshold}"
			raise ValueError(msg)
		if log_path is not None and os.path.exists(log_path):
			msg = f"Chosen log already exists: {log_path}"
			raise ValueError(msg)

		src_files = _listdir(src_root, filter, ignore_hidden)
		dst_files = _listdir(dst_root, filter, ignore_hidden)

		if log_path == "-":
			log_path = os.path.expanduser(os.path.join("~", f"py-backup.{timestamp}.log"))
			log_manager.log_path = log_path
		elif log_path:
			log_manager.log_path = log_path

		logger.debug(f"Starting backup: {src_root=} {dst_root=} {trash_root=} {timestamp=} {filter=} {ignore_hidden=} {rename_threshold=} {dry_run=} {log_path=} {quiet=} {veryquiet=}")

		width = max(len(src_root), len(dst_root)) + 3
		#logger.info("=" * width)
		logger.info("   " + src_root)
		logger.info("-> " + dst_root)
		logger.info("-" * width)

		if not dry_run:
			os.makedirs(dst_root, exist_ok=True)

		results = Results()

		for op, src, dst, byte_diff, summary in _operations(
			src_files        = src_files,
			dst_files        = dst_files,
			src_root         = src_root,
			dst_root         = dst_root,
			trash_dir        = trash_dir,
			rename_threshold = rename_threshold,
			metadata_only    = metadata_only
		):
			logger.info(summary)

			if not dry_run:
				if op == "-":
					try:
						_move(src, dst, root=dst_root)
						results.delete_success += 1
						results.byte_diff += byte_diff
					except OSError as e:
						results.delete_error += 1
						logger.error(f"{e.__class__.__name__}: - {dst}")
				elif op == "+":
					try:
						_copy(src, dst)
						results.create_success += 1
						results.byte_diff += byte_diff
					except OSError as e:
						results.create_error += 1
						logger.error(f"{e.__class__.__name__}: + {dst}")
				elif op == "U":
					try:
						_copy(src, dst)
						results.update_success += 1
						results.byte_diff += byte_diff
					except OSError as e:
						results.update_error += 1
						logger.error(f"{e.__class__.__name__}: U {dst}")
				elif op == "R":
					try:
						_move(src, dst, root=dst_root)
						results.rename_success += 1
					except OSError as e:
						results.rename_error += 1
						logger.error(f"{e.__class__.__name__}: R {src} -> {dst}")
				elif op == "D+":
					try:
						os.makedirs(dst, exist_ok=True)
					except OSError as e:
						logger.error(f"{e.__class__.__name__}: + {dst}{os.sep}")
				elif op == "D-":
					try:
						_delete_empty_dirs(src, root=dst_root)
					except OSError as e:
						logger.error(f"{e.__class__.__name__}: - {src}{os.sep}")
				else:
					assert False

		logger.info("")
		logger.info("File Stats (Excluding Dirs)")
		logger.info(f"Rename Success: {results.rename_success}" + (f" / Failed: {results.rename_error}" if results.rename_error else ""))
		logger.info(f"Create Success: {results.create_success}" + (f" / Failed: {results.create_error}" if results.create_error else ""))
		logger.info(f"Update Success: {results.update_success}" + (f" / Failed: {results.update_error}" if results.update_error else ""))
		logger.info(f"Delete Success: {results.delete_success}" + (f" / Failed: {results.delete_error}" if results.delete_error else ""))
		logger.info(f"Net Change: {_human_readable_size(results.byte_diff)}")

		if results.err_count:
			logger.info("")
			logger.info(f"Finished with {results.err_count} errors.")
			logger.info(f"See the log at {log_path} for details.")
		else:
			logger.info("")
			logger.info("Finished successfully.")

		return results

class _Filter:
	def __init__(self, filter_string, *, ignore_hidden=False):
		self.patterns = []

		filter_string = filter_string.strip()
		for action, patterns in re.findall(r"(\+|-)\s+((?:(?:'[^']*'|\"[^\"]*\"|\S{2,}|[^\s\+-])\s*)+)", filter_string):
			action = action == "+"
			added_parent_dirs = set()
			for pattern in re.findall(r"'[^']*'|\"[^\"]*\"|\S{2,}|[^\s\+-]", patterns):
				if pattern[0] == "'" or pattern[0] == "\"":
					pattern = pattern[1:-1]
				if pattern[:2] == "./":
					pattern = pattern[2:]

				if pattern == ".." or pattern.startswith(f"..{os.sep}") or f"{os.sep}..{os.sep}" in pattern or pattern.endswith(f"{os.sep}.."):
					raise ValueError(f"Parent directories ('..') are not supported in pattern arguments to include/exclude: {pattern}")
				if os.path.isabs(pattern):
					raise ValueError(f"Absolute paths are not supported as arguments to include/exclude: {path}")

				regex = glob.translate(pattern, recursive=True, include_hidden=(not ignore_hidden))
				reobj = re.compile(regex)
				self.patterns.append((action, reobj))

				# include parent dirs for each include pattern
				if action == True:
					while True:
						pattern = os.path.dirname(pattern)
						if pattern == "":
							break
						if pattern in added_parent_dirs:
							break
						added_parent_dirs.add(pattern)
						regex = glob.translate(pattern + os.sep, recursive=True, include_hidden=True)
						reobj = re.compile(regex)
						self.patterns.append((action, reobj))

	def filter(self, relpath, default=False):
		for action, reobj in self.patterns:
			if reobj.match(relpath):
				#print(f"{action=} {pattern=} {relpath=}")
				return action
		return default

def _listdir(root, *, filter="+ **/*/ **/*", ignore_hidden=False):
	'''
	Retrieves file relative paths, sizes, and mtimes for files inside a directory. (All "relative paths" are relative to `root`.)

    Args
		root (str)   : The directory to search.
		filter (str) : The filter to include/exclude files and directories. Include entries by preceding a space-separated list with "+", and exclude with "-". Included files will be copied, while included directories will be searched. Each pattern ending with a slash will only apply to directories. Otherise the pattern will only apply to files. (Defaults to `+ **/*/ **/*`.)

	Returns
		A SimpleNamespace containing two fields: `relpath_stats` and `empty_dirs`. `relpath_stats` is a `dict` with keys being each file's relative path and values being a `namedtuple` of file size (`size`) and modtime (`mtime`). `empty_dirs` is a set of relative paths to empty directories.
	'''
	f = _Filter(filter, ignore_hidden=ignore_hidden)

	Metadata = namedtuple("Metadata", ["size", "mtime"])

	file_list = SimpleNamespace()
	file_list.relpath_stats = {}
	file_list.empty_dirs = set()
	#file_list.nonempty_dirs = set()
	file_list._scan_count = 0 # for debugging

	for dir, subdirnames, file_entries in direntry_walk(root):
		logger.debug("_listdir: in " + dir)
		file_list._scan_count += 1

		# sorting may be needed if _listdir is changed to yield folder-by-folder
		#subdirnames.sort()
		#file_entries.sort()

		dirname     = os.path.basename(dir)
		dir_relpath = os.path.relpath(dir, root)

		# catalog empty directory
		if not file_entries and not subdirnames:
			file_list.empty_dirs.add(dir_relpath)
			continue
		#else:
		#	file_list.nonempty_dirs.add(dir_relpath)


		# prune search tree
		i = 0
		while i < len(subdirnames):
			subdirname = subdirnames[i]
			subdir_path = os.path.join(dir, subdirname)
			subdir_relpath = os.path.relpath(subdir_path, root)
			if not f.filter(subdir_relpath + os.sep):
				del subdirnames[i]
				continue
			i += 1

		# prune files
		for entry in file_entries:
			filename = entry.name
			file_path = os.path.join(dir, filename)
			file_relpath = os.path.relpath(file_path, root)
			if (f.filter(file_relpath)):
				file_list.relpath_stats[file_relpath] = Metadata(entry.stat().st_size, entry.stat().st_mtime)

	return file_list

def _operations(*, src_files, dst_files, src_root, dst_root, trash_dir, rename_threshold, metadata_only):
	src_relpath_stats = src_files.relpath_stats
	dst_relpath_stats = dst_files.relpath_stats

	src_relpaths = set(src_relpath_stats.keys())
	dst_relpaths = set(dst_relpath_stats.keys())

	src_only_relpaths = sorted(src_relpaths.difference(dst_relpaths))
	dst_only_relpaths = sorted(dst_relpaths.difference(src_relpaths))
	both_relpaths     = sorted(src_relpaths.intersection(dst_relpaths))

	if rename_threshold is not None:
		src_only_relpath_from_stats = _reverse_dict({path:src_relpath_stats[path] for path in src_only_relpaths})
		dst_only_relpath_from_stats = _reverse_dict({path:dst_relpath_stats[path] for path in dst_only_relpaths})

		for dst_relpath in dst_only_relpaths:
			# Ignore small files
			if dst_relpath_stats[dst_relpath][0] < rename_threshold:
				continue
			try:
				rename_to = src_only_relpath_from_stats[dst_relpath_stats[dst_relpath]]
				# Ignore if there are multiple candidates
				if rename_to is None:
					continue
				rename_from = dst_only_relpath_from_stats[dst_relpath_stats[dst_relpath]]
				# Ignore if there are multiple candidates
				if rename_from is None:
					continue

				# Ignore if last 1kb do not match
				if not metadata_only:
					on_dst = os.path.join(dst_root, rename_from)
					on_src = os.path.join(src_root, rename_to)
					if not _last_bytes(on_src) == _last_bytes(on_dst):
						continue

				src = os.path.join(dst_root, rename_from)
				dst = os.path.join(dst_root, rename_to)

				src_only_relpaths.remove(rename_to)
				dst_only_relpaths.remove(rename_from)

				yield ("R", src, dst, 0, f"R {rename_from} -> {rename_to}")

			except KeyError:
				# dst file not a result of a rename
				continue

	# Deleting must be done first or backing up a.jpg -> a.JPG (or similar) on Windows will fail
	if trash_dir:
		for dst_relpath in dst_only_relpaths:
			src = os.path.join( dst_root, dst_relpath)
			dst = os.path.join(trash_dir, dst_relpath)
			byte_diff = -dst_relpath_stats[dst_relpath][0]
			yield ("-", src, dst, byte_diff, f"- {dst_relpath}")

	for src_relpath in src_only_relpaths:
		src = os.path.join(src_root, src_relpath)
		dst = os.path.join(dst_root, src_relpath)
		byte_diff = src_relpath_stats[src_relpath].size
		yield ("+", src, dst, byte_diff, f"+ {src_relpath}")

	for relpath in both_relpaths:
		src = os.path.join(src_root, relpath)
		dst = os.path.join(dst_root, relpath)
		byte_diff = src_relpath_stats[relpath].size - dst_relpath_stats[relpath].size
		src_time = src_relpath_stats[relpath].mtime
		dst_time = dst_relpath_stats[relpath].mtime
		if src_time > dst_time:
			yield ("U", src, dst, byte_diff, f"U {relpath}")
		elif src_time < dst_time:
			logger.warn(f"Working copy is older than backed-up copy, skipping update: {relpath}")

	# Empty directories
	src_only_empty_dirs = src_files.empty_dirs.difference(dst_files.empty_dirs)#.difference(dst_files.nonempty_dirs)
	for relpath in src_only_empty_dirs:
		dst = os.path.join(dst_root, relpath)
		if not os.path.isdir(dst):
			yield ("D+", None, dst, 0, f"+ {relpath}{os.sep}")
	dst_only_empty_dirs = dst_files.empty_dirs.difference(src_files.empty_dirs)#.difference(src_files.empty_dirs)
	for relpath in dst_only_empty_dirs:
		src = os.path.join(dst_root, relpath)
		if not os.listdir(src):
			yield ("D-", src, None, 0, f"- {relpath}{os.sep}")

def _copy(src, dst):
	'''Copy src to dst, keeping timestamp metadata, and overwriting any existing file.'''
	if src == dst:
		raise ValueError(f"Same file: {src} -> {dst}")
	elif "nt" in os.name and src.lower() == dst.lower():
		raise ValueError(f"Same file: {src} -> {dst}")

	delete_tmp = False
	dst_tmp = dst + ".tempcopy"
	try:
		# Copy into a temp file, with metadata
		dir = os.path.dirname(dst)
		os.makedirs(dir, exist_ok=True)
		shutil.copy2(src, dst_tmp)
		delete_tmp = True
		try:
			# Rename the temp file into the dest file
			os.replace(dst_tmp, dst)
			delete_tmp = False
		except PermissionError as e:
			# Remove read-only flag and try again
			make_readonly = False
			try:
				if not (os.stat(dst).st_mode & stat.S_IREAD):
					raise e
				os.chmod(dst, stat.S_IWRITE)
				make_readonly = True
				os.replace(dst_tmp, dst)
				delete_tmp = False
			finally:
				if make_readonly:
					os.chmod(dst, stat.S_IREAD)
	finally:
		# Remove the temp copy if there are any errors
		if delete_tmp:
			os.remove(dst_tmp)

def _move(src, dst, *, delete_empty_dirs=True, root=""):
	'''Move src to dst (on the same filesystem), failing if dst exists.'''
	# dst file must either not exist or differ from src by case
	if dst == src:
		raise ValueError(f"Same file: {src} -> {dst}")
	if os.path.exists(dst):
		if "nt" in os.name:
			if src.lower() != dst.lower():
				raise FileExistsError(f"File already exists: {src} -> {dst}")
		else:
			raise FileExistsError(f"File already exists: {src} -> {dst}")

	# move the file
	dir = os.path.dirname(dst)
	os.makedirs(dir, exist_ok=True)
	os.rename(src, dst)

	# on windows, ancestor directories need to be explicitly renamed after the file if the path name differs by capitalization
	if "nt" in os.name:
		try:
			src_dir = os.path.dirname(src)
			dst_dir = os.path.dirname(dst)
			while src_dir and src_dir != dst_dir and src_dir.lower() == dst_dir.lower():
				os.rename(src_dir, dst_dir)
				src_dir = os.path.dirname(src_dir)
				dst_dir = os.path.dirname(dst_dir)
		except OSError:
			logger.error(f"{e.__class__.__name__}: Failed to rename: {src} -> {dst}")

	# delete empty directories left after the move
	if delete_empty_dirs:
		_delete_empty_dirs(os.path.dirname(src), root=root)

def _delete_empty_dirs(dir, *, root=""):
	if not os.path.isdir(dir):
		raise ValueError(f"Expected a dir: {dir}")
	try:
		while not os.listdir(dir):
			relpath = os.path.relpath(dir, root)
			logger.info(f"- {relpath}{os.sep}")
			os.rmdir(dir)
			dir = os.path.dirname(dir)
	except OSError:
		logger.error(f"{e.__class__.__name__}: Failed to delete: {relpath}{os.sep}")

def _reverse_dict(old_dict):
	'''
	Reverses a `dict`, allowing quick retrieval of keys from values.

	If a value in `old_dict` appears more than once, then the corresponding key in the reversed dict will point to a value of `None`.
	'''
	reversed = {}
	for key, val in old_dict.items():
		if val in reversed:
			reversed[val] = None
		else:
			reversed[val] = key
	return reversed

def _last_bytes(file_path, n=1024):
	file_size = os.path.getsize(file_path)
	bytes_to_read = file_size if n > file_size else n
	with open(file_path, "rb") as f:
		f.seek(-bytes_to_read, os.SEEK_END)
		return f.read()

def _human_readable_size(num_bytes):
	sign = "-" if num_bytes < 0 else ""
	num_bytes = abs(num_bytes)
	units = ["bytes", "KB", "MB", "GB", "TB", "PB"]
	i = 0
	while num_bytes >= 1024 and i < len(units) - 1:
		num_bytes /= 1024
		i += 1
	return f"{sign}{round(num_bytes)} {units[i]}"

class _LogManager:
	def __init__(self ,*, suppress_stdout, suppress_stderr):
		self._log_path = None
		self.final_log_path = None
		self.log_handler_file = None

		self.suppress_stdout = suppress_stdout
		self.suppress_stderr = suppress_stderr
		self.log_handler_console = None

	@property
	def log_path(self):
		return self._log_path

	@log_path.setter
	def log_path(self, val):
		# file log
		if val:
			self.final_log_path = val
			with tempfile.NamedTemporaryFile(mode="w+", encoding='utf-8', delete=False) as tmp_log:
				self._log_path = tmp_log.name
			formatter = logging.Formatter("%(levelname)s: %(message)s")
			self.log_handler_file = logging.FileHandler(self.log_path, encoding='utf-8')
			self.log_handler_file.setFormatter(formatter)
			self.log_handler_file.setLevel(logging.DEBUG)
			logger.addHandler(self.log_handler_file)
			self.log_handler_console.log_file = val
		elif self.log_handler_file:
			logger.removeHandler(self.log_handler_file)
			self.log_handler_file.close()
			os.replace(self.log_path, self.final_log_path)
			self._log_path = None
			self.final_log_path = None
			self.log_handler_file = None
			self.log_handler_console.log_file = None


	def __enter__(self):
		# console log
		if not self.suppress_stdout or not self.suppress_stderr:
			formatter = logging.Formatter("%(message)s")
			self.log_handler_console = _ConsoleHandler(self.suppress_stdout, self.suppress_stderr)
			self.log_handler_console.setFormatter(formatter)
			self.log_handler_console.setLevel(logging.INFO)
			logger.addHandler(self.log_handler_console)
		return self

	def __exit__(self, exc_type, exc_value, tb):
		if exc_type:
			if exc_type is TypeError or exc_type is ValueError:
				logger.critical(f"Input Error: {exc_value}")
			elif exc_type is KeyboardInterrupt:
				logger.warn(f"Cancelled by user.")
			else:
				logger.critical(f"{exc_type.__name__}: {exc_value}")
				logger.debug(traceback.format_exc())

		if self.log_handler_file:
			logger.removeHandler(self.log_handler_file)
			self.log_handler_file.close()
			os.replace(self.log_path, self.final_log_path)
			self.log_path = self.final_log_path

		if self.log_handler_console:
			self.log_handler_console.close()
			logger.removeHandler(self.log_handler_console)

class _ConsoleHandler(logging.Handler):
	def __init__(self, suppress_stdout, suppress_stderr, max_err_recap=10):
		super().__init__()
		self.suppress_stdout = suppress_stdout
		self.suppress_stderr = suppress_stderr
		self.max_err_recap = max_err_recap
		self.errs = []
		self.count_errs = 0
		self.critical_err = False
		self.log_file = None

	def emit(self, record):
		msg = self.format(record)+"\n"
		if record.levelname == "DEBUG" or record.levelname == "INFO":
			if not self.suppress_stdout:
				sys.stdout.write(msg)
		else:
			if len(self.errs) < self.max_err_recap:
				self.errs.append(msg)
			if not self.suppress_stderr:
				sys.stderr.write(msg)
			if record.levelname == "CRITICAL":
				self.critical_err = True

	def close(self):
		if not self.suppress_stdout:
			if 0 < len(self.errs) <= self.max_err_recap and not self.critical_err:
				sys.stdout.write("Errors are reprinted below for convenience:\n")
				for err in self.errs:
					sys.stdout.write(err)

class _ArgParser:
	parser = argparse.ArgumentParser(
		description="Copy new and updated files from one directory to another, update renamed files' names to match where possible, and optionally delete non-matching files.",
		epilog="(c) 2025 Joe Walter"
	)

	parser.add_argument("src_root", help="The root directory to copy files from.")
	parser.add_argument("dst_root", help="The root directory to copy files to.")
	parser.add_argument("-t", "--trash-root", metavar="path", nargs="?", default=None, const=True, help="The root directory to move 'extra' files (those that are in `dst_root` but not `src_root`). Must be on the same filesystem as `dst_root`. If included without an argument, then a directory will automatically be made next to `dst_root`. Extra files will not be moved if this option is omitted.")

	parser.add_argument("-f", "--filter", metavar="filter_string", nargs=1, default="+ **/*/ **/*", help="The filter to include/exclude files and directories. Similar to rsync, the format of the filter string is: (+ or -), followed by a list of one of more relative path patterns, and otionally repeat from the start. Including (+) or excluding (-) of entries is determined by the preceding symbol of the first matching pattern. Included files will be copied, while included directories will be searched. Each Pattern ending with \"/\" will apply to directories only. Otherise the pattern will apply only to files. (Defaults to \"+ **/*/ **/*\", which searches all directories and copies all files.)")
	parser.add_argument("--ignore-hidden", action="store_true", default=False, help="Whether to skip hidden files by default. If `True`, then wildcards in glob patterns will not match entries beginning with a dot. However, globs containing a dot (e.g., \"**/.*\") will still match these entries. (Defaults to `False`.)")
	parser.add_argument("-r", "--rename-threshold", metavar="size", nargs=1, type=int, default=20000, help="The minimum size in bytes needed to consider renaming files in dst_root to match those in src_root. Renamed files below this threshold will be simply deleted in dst_root and their replacements copied over.")
	parser.add_argument("-m", "--metadata_only", action="store_true", default=False, help="Use only metadata in determining which files in dst_root are the result of a rename. Otherwise, backup will also compare the last 1kb of files.")
	parser.add_argument("--dry-run", action="store_true", default=False, help="Forgo performing any operation that would make a filesystem change. Changes that would have occurred will still be printed to console.")

	group = parser.add_mutually_exclusive_group(required=False)
	group.add_argument("--log", metavar="path", type=str, default="-", help="File to write log messages to. If this is not supplied, a tempfile will be used for the log, and it will be moved to the user's home directory after the backup is done.")
	group.add_argument("--no-log", action="store_true", help="Forgo writing to a log.")

	parser.add_argument("-q", action="count", default=0, help="Forgo printing to stdout (-q) and stderr (-qq).")

	@staticmethod
	def parse(args):
		if isinstance(args, str):
			args = args.split()
		args = _ArgParser.parser.parse_args(args)

		args.log_path  = None if args.no_log else args.log
		args.quiet     = args.q >= 1
		args.veryquiet = args.q >= 2

		del args.no_log
		del args.log
		del args.q

		return args

def backup2(args):
	args = _ArgParser.parse(args)
	return backup(
		args.src_root,
		args.dst_root,
		trash_root       = args.trash_root,
		filter           = args.filter,
		ignore_hidden    = args.ignore_hidden,
		rename_threshold = args.rename_threshold,
		metadata_only    = args.metadata_only,
		dry_run          = args.dry_run,
		log_path         = args.log_path,
		quiet            = args.quiet,
		veryquiet        = args.veryquiet
	)

def main():
	'''
	Return Codes:
		0 = Finished OK
		1 = Unknown error
		2 = Bad input
		3 = Finished with OSErrors
		130 = Cancelled by user
	'''
	try:
		results = backup2(sys.argv[1:])
		if results.err_count:
			sys.exit(3)
		else:
			sys.exit(0)
	except KeyboardInterrupt:
		sys.exit(130)
	except (ValueError, TypeError) as e:
		sys.exit(2)
	except Exception:
		print()
		traceback.print_exc()
		sys.exit(1)

if __name__ == "__main__":
	main()

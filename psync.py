# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

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
from pathlib import Path
from fnmatch import fnmatch
from collections import Counter
from types import SimpleNamespace
from functools import lru_cache
from direntry_walk import direntry_walk
from typing import NamedTuple, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class _DebugInfoFilter(logging.Filter):
	'''Logging filter that only allows DEBUG and INFO records to pass.'''

	def filter(self, record):
		return logging.DEBUG <= record.levelno <= logging.INFO

class _ArgParser:
	'''Argument parser for when this python file is run with arguments instead of an imported module.'''

	parser = argparse.ArgumentParser(
		description="Copy new and updated files from one directory to another, update renamed files' names to match where possible, and optionally delete non-matching files.",
		epilog="(c) 2025 Joe Walter"
	)

	parser.add_argument("src_root", help="The root directory to copy files from.")
	parser.add_argument("dst_root", help="The root directory to copy files to.")
	parser.add_argument("-t", "--trash-root", metavar="path", nargs="?", type=str, default=None, const="auto", help="The root directory to move 'extra' files (those that are in `dst_root` but not `src_root`). Must be on the same file system as `dst_root`. If set to \"auto\", then a directory will automatically be made next to `dst_root`. Extra files will not be moved if this option is omitted.")

	parser.add_argument("-f", "--filter", metavar="filter_string", nargs=1, type=str, default="+ **/*/ **/*", help="The filter string (enclosed in quotes) that includes/excludes file system entries from the `src_root` and `dst_root` directories. Similar to rsync, the format of the filter string is one of more repetitions of: (+ or -), followed by a list of one of more relative path patterns. Including (+) or excluding (-) of file system entries is determined by the preceding symbol of the first matching pattern. Included files will be copied over as part of the backup, while included directories will be searched. Each pattern ending with \"/\" will apply to directories only. Otherise the pattern will apply only to files. Note that it is still possible for excluded files in `dst_root` to be overwritten. (Defaults to \"+ **/*/ **/*\", which searches all directories and copies all files.)")
	parser.add_argument("-H", "--ignore-hidden", action="store_true", default=False, help="Skip hidden files by default. That is, wildcards in glob patterns will not match file system entries beginning with a dot. However, globs containing a dot (e.g., \"**/.*\") will still match these file system entries.")
	parser.add_argument("-L", "--follow-symlinks", action="store_true", default=False, help="Follow symbolic links under `src_root` and `dst_root`. Note that `src_root` and `dst_root` themselves will be followed regardless of this flag.")
	parser.add_argument("-R", "--rename-threshold", metavar="size", nargs=1, type=int, default=20000, help="The minimum size in bytes needed to consider renaming files in dst_root to match those in `src_root`. Renamed files below this threshold will be simply deleted in dst_root and their replacements copied over.")
	parser.add_argument("-m", "--metadata_only", action="store_true", default=False, help="Use only metadata in determining which files in `dst_root` are the result of a rename. Otherwise, the backup process will also compare the last 1kb of files.")
	parser.add_argument("-d", "--dry-run", action="store_true", default=False, help="Forgo performing any operation that would make a file system change. Changes that would have occurred will still be printed to console.")

	parser.add_argument("--log", metavar="path", nargs="?", type=str, default=None, const="auto", help="The path of the log file to use. It will be created if it does not exist. With \"auto\" or no argument, a tempfile will be used for the log, and it will be moved to the user's home directory after the backup is done. If this flag is absent, then no logging will be performed.")
	parser.add_argument("--debug", action="store_true", default=False, help="Log debug messages.")
	parser.add_argument("-q", action="count", default=0, help="Forgo printing to stdout (-q) and stderr (-qq).")

	@staticmethod
	def parse(args:list[str]) -> argparse.Namespace:
		parsed_args = _ArgParser.parser.parse_args(args)
		parsed_args.quiet     = parsed_args.q >= 1
		parsed_args.veryquiet = parsed_args.q >= 2
		del parsed_args.q
		return parsed_args

class _Filter:
	'''Object that holds a parsed filter string for quicker file filtering.'''

	patterns : list[tuple[bool, re.Pattern]]

	def __init__(self, filter_string:str, *, ignore_hidden:bool = False):
		self.patterns = []
		implicit_dirs : set[str] = set()

		filter_string = filter_string.strip()
		for action, patterns in re.findall(r"(\+|-)\s+((?:(?:'[^']*'|\"[^\"]*\"|\S{2,}|[^\s\+-])\s*)+)", filter_string):
			action = action == "+"
			if not action:
				# clear if - action
				implicit_dirs = set()
			for pattern in re.findall(r"'[^']*'|\"[^\"]*\"|\S{2,}|[^\s\+-]", patterns):
				if pattern[0] == "'" or pattern[0] == "\"":
					pattern = pattern[1:-1]
				if pattern[:2] == ".\\" or pattern[:2] == "./":
					pattern = pattern[2:]

				if pattern == ".." or re.search("^\\\\.\\.[\\\\/]", pattern) or re.search("[\\\\/]\\.\\.[\\\\/]", pattern) or re.search("[\\\\/]\\.\\.$", pattern):
					raise ValueError(f"Parent directories ('..') are not supported in pattern arguments to include/exclude: {pattern}")
				if os.path.isabs(pattern):
					raise ValueError(f"Absolute paths are not supported as arguments to include/exclude: {pattern}")

				if pattern == "":
					continue

				regex = glob.translate(pattern, recursive=True, include_hidden=(not ignore_hidden))
				reobj = re.compile(regex)
				self.patterns.append((action, reobj))

				# include parent dirs for each include pattern
				if action:
					while True:
						pattern = os.path.dirname(pattern)
						if pattern == "":
							break
						if pattern in implicit_dirs:
							break
						implicit_dirs.add(pattern)
						regex = glob.translate(pattern + "/", recursive=True, include_hidden=(not ignore_hidden))
						reobj = re.compile(regex)
						self.patterns.append((action, reobj))

	def filter(self, relpath:str, default:bool = False) -> bool:
		'''Compare the file path against the filter string.'''

		for action, reobj in self.patterns:
			if reobj.match(relpath):
				return action
		return default

class _Metadata(NamedTuple):
	'''File metadata that will be used to find probable duplicates.'''

	size  : int
	mtime : float

class _FileList(NamedTuple):
	'''File and directory information returned by `_scandir()`.'''

	root             : Path
	relpath_to_stats : dict[str, _Metadata]
	real_names       : dict[str, str]
	empty_dirs       : set[str]
	#nonempty_dirs   : set[str]
	visited_inodes   : set[int]

class Results:
	'''Various statistics and other information returned by `sync()`.'''

	def __init__(self) -> None:
		self.trash_root : Path | None = None
		self.log_file   : Path | None = None

		self.success    : bool        = False
		self.errors     : list[str]   = []

		self.create_success = 0
		self.rename_success = 0
		self.update_success = 0
		self.delete_success = 0
		self.create_error = 0
		self.rename_error = 0
		self.update_error = 0
		self.delete_error = 0
		self.byte_diff = 0

		self.dir_create_success = 0
		self.dir_create_error   = 0
		self.dir_delete_success = 0
		self.dir_delete_error   = 0

	@property
	def err_count(self) -> int:
		return self.create_error + self.rename_error + self.update_error + self.delete_error + self.dir_create_error + self.dir_delete_error

def sync_cmd(args:list[str]) -> Results:
	'''Run `sync()` with command line arguments.'''

	parsed_args = _ArgParser.parse(args)
	return sync(
		parsed_args.src_root,
		parsed_args.dst_root,
		trash            = parsed_args.trash_root,
		filter           = parsed_args.filter[0],
		ignore_hidden    = parsed_args.ignore_hidden,
		rename_threshold = parsed_args.rename_threshold[0],
		metadata_only    = parsed_args.metadata_only,
		dry_run          = parsed_args.dry_run,
		log              = parsed_args.log,
		debug            = parsed_args.debug,
		quiet            = parsed_args.quiet,
		veryquiet        = parsed_args.veryquiet
	)

def sync(
		src              : str | os.PathLike[str],
		dst              : str | os.PathLike[str],
		*,
		trash            : str | os.PathLike[str] | None = None,
		filter           : str  = "+ **/*/ **/*",
		ignore_hidden    : bool = False,
		follow_symlinks  : bool = False,
		rename_threshold : int | None  = 10000,
		metadata_only    : bool = False,
		dry_run          : bool = False,
		log              : str | os.PathLike[str] | None = None,
		debug            : bool = False,
		quiet            : bool = False,
		veryquiet        : bool = False,
	) -> Results:
	'''
	Copies new and updated files from `src` to `dst`, and optionally "deletes" files from `dst` if they are not present in `src` (they will be moved into `trash`, preserving directory structure). Furthermore, files that exist in `dst` but as a different name in `src` may be renamed in `dst` to match. Candidates for rename are discovered by searching for files with an identical metadata signature, consisting of file size and modification time. These candidates must be above a minimum size threshold (`rename_threshold`) and have an unambiguously unique metadata signature within their respective root directories. The user is asked to confirm these renames before they are committed.

	Args
		src (str or PathLike)    : The path of the root directory to copy files from. Can be a symlink to a directory.
		dst (str or PathLike)    : The path of the root directory to copy files to. Can be a symlink to a directory.
		trash (str or PathLike)  : The path of the root directory to move "extra" files to. ("Extra" files are those that are in `dst` but not `src`.) Must be on the same file system as `dst`. If set to "auto", then a directory will automatically be made next to `dst`. "Extra" files will not be moved if this argument is `None`. (Defaults to `None`.)

		filter (str)             : The filter string that includes/excludes file system entries from the `src` and `dst` directories. Similar to rsync, the format of the filter string is one of more repetitions of: (+ or -), followed by a list of one of more relative path patterns. Including (+) or excluding (-) of file system entries is determined by the preceding symbol of the first matching pattern. Included files will be copied over as part of the backup, while included directories will be searched. Each pattern ending with "/" will apply to directories only. Otherise the pattern will apply only to files. Note that it is still possible for excluded files in `dst` to be overwritten. (Defaults to "+ **/*/ **/*", which searches all directories and copies all files.)
		ignore_hidden (bool)     : Whether to skip hidden files by default. If `True`, then wildcards in glob patterns will not match file system entries beginning with a dot. However, globs containing a dot (e.g., "**/.*") will still match these file system entries. (Defaults to `False`.)
		follow_symlinks (bool)   : Whether to follow symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this argument. (Defaults to `False`.)
		rename_threshold (int)   : The minimum size in bytes needed to consider renaming files in `dst` that were renamed in `src`. Renamed files below this threshold will be simply deleted in `dst` and their replacements created. A value of `None` will mean no files in `dst` will be eligible for renaming. (Defaults to `10000`.)
		metadata_only (bool)     : Whether to use only metadata in determining which files in `dst` are the result of a rename. Otherwise, the backup process will also compare the last 1kb of files. (Defaults to `False`.)
		dry_run (bool)           : Whether to hold off performing any operation that would make a file system change. Changes that would have occurred will still be printed to console. (Defaults to `False`.)

		log (str or PathLike)    : The path of the log file to use. It will be created if it does not exist. A value of "auto" means a tempfile will be used for the log, and it will be copied to the user's home directory after the backup is done. A value of `None` will skip logging to a file. (Defaults to `None`.)
		debug (bool)             : Whether to log debug messages. (Default to `False`.)
		quiet (bool)             : Whether to forgo printing to stdout.
		veryquiet (bool)         : Whether to forgo printing to stdout and stderr.

	Example Console Output
		   path/to/src
		-> path/to/dst
		--------------
		- empty-dir-in-dst/
		R old-name.txt -> new-name.txt
		- not-in-src.txt
		+ not-in-dst.txt
		U updated.txt
		+ empty-dir-in-src/

		The program ended successfully.

		File Stats (Excluding Dirs)
		Rename Success: 1
		Create Success: 1
		Update Success: 0
		Delete Success: 1
		Net Change: 0 bytes

		Log file: path/to/log/py-backup.1753715578560.log

	Returns
		A `Results` object containing various statistics.
	'''
	results = Results()

	if logger.handlers:
		for handler in list(logger.handlers):
			logger.removeHandler(handler)

	log_file       = None
	handler_stdout = None
	handler_stderr = None
	handler_file   = None

	if veryquiet:
		quiet = True

	if not quiet:
		handler_stdout = logging.StreamHandler(sys.stdout)
		handler_stdout.setFormatter(logging.Formatter("%(message)s"))
		handler_stdout.addFilter(_DebugInfoFilter())
		if debug:
			handler_stdout.setLevel(logging.DEBUG)
		else:
			handler_stdout.setLevel(logging.INFO)
		logger.addHandler(handler_stdout)

	if not veryquiet:
		handler_stderr = logging.StreamHandler(sys.stderr)
		handler_stderr.setFormatter(logging.Formatter("%(message)s"))
		handler_stderr.setLevel(logging.WARNING)
		logger.addHandler(handler_stderr)

	try:
		if not isinstance(src, (str, os.PathLike)):
			msg = f"Bad type for arg 'src' (expected str or PathLike): {src}"
			raise TypeError(msg)
		if not isinstance(dst, (str, os.PathLike)):
			msg = f"Bad type for arg 'dst' (expected str or PathLike): {dst}"
			raise TypeError(msg)
		if trash is not None and not isinstance(trash, (str, os.PathLike)):
			msg = f"Bad type for arg 'trash' (expected str or PathLike): {trash}"
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
		if log is not None and not isinstance(log, (str, os.PathLike)):
			msg = f"Bad type for arg 'log' (expected str or PathLike): {log}"
			raise TypeError(msg)
		if not isinstance(quiet, bool):
			msg = f"Bad type for arg 'quiet' (expected bool): {quiet}"
			raise TypeError(msg)
		if not isinstance(veryquiet, bool):
			msg = f"Bad type for arg 'veryquiet' (expected bool): {veryquiet}"
			raise TypeError(msg)

		src_root = Path(src)
		dst_root = Path(dst)

		timestamp = str(int(time.time()*1000))
		if trash is None:
			trash_root = None
		elif trash == "auto":
			trash_root = Path(dst_root).parent / f"Trash.{timestamp}"
		else:
			trash_root = Path(trash) / timestamp
		results.trash_root = trash_root

		if log is None:
			log_file = None
		elif log == "auto":
			log_file = Path.home() / f"py-backup.{timestamp}.log"
		else:
			log_file = Path(log)
		results.log_file = log_file

		if src_root.exists() and not src_root.is_dir():
			msg = f"Chosen 'src' is not a directory: {src_root}"
			raise ValueError(msg)
		if dst_root.exists() and not dst_root.is_dir():
			msg = f"Chosen 'dst' is not a directory: {dst_root}"
			raise ValueError(msg)
		if src_root.resolve() == dst_root.resolve():
			msg = f"Chosen 'src' and 'dst' point to the same directory"
			raise ValueError(msg)
		if trash_root is not None and trash_root.exists() and not trash_root.is_dir():
			msg = f"Chosen trash_root is not a directory: {trash_root}"
			raise ValueError(msg)
		if log_file is not None and os.path.exists(log_file):
			msg = f"Chosen log already exists: {log_file}"
			raise ValueError(msg)

		if not dry_run:
			os.makedirs(dst_root, exist_ok=True)
			if trash_root is not None:
				os.makedirs(trash_root, exist_ok=True)

		if trash_root is not None and trash_root.exists():
			if os.stat(trash_root).st_dev != os.stat(dst_root).st_dev:
				msg = f"Chosen trash_root is not on the same file system as dst_root: {trash_root}"
				raise ValueError(msg)
		if rename_threshold is not None and rename_threshold < 0:
			msg = f"rename_threshold must be non-negative: {rename_threshold}"
			raise ValueError(msg)

		tmp_log_file = None
		if log_file is not None:
			with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False) as tmp_log:
				tmp_log_file = Path(tmp_log.name)
			formatter = logging.Formatter("%(levelname)s: %(message)s")
			handler_file = logging.FileHandler(tmp_log_file, encoding="utf-8")
			handler_file.setFormatter(formatter)
			if debug:
				handler_file.setLevel(logging.DEBUG)
			else:
				handler_file.setLevel(logging.INFO)
			logger.addHandler(handler_file)

		logger.debug(f"Starting backup: {src_root=} {dst_root=} {trash_root=} {filter=} {ignore_hidden=} {follow_symlinks=} {rename_threshold=} {dry_run=} {log_file=} {debug=} {quiet=} {veryquiet=}")

		width = max(len(str(src_root)), len(str(dst_root))) + 3
		logger.info("   " + str(src_root))
		logger.info("-> " + str(dst_root))
		logger.info("-" * width)

		src_files = _scandir(src_root, filter=filter, ignore_hidden=ignore_hidden, follow_symlinks=follow_symlinks)
		dst_files = _scandir(dst_root, filter=filter, ignore_hidden=ignore_hidden, follow_symlinks=follow_symlinks)

		for op, src_file, dst_file, byte_diff, summary in _operations(
			src_files,
			dst_files,
			trash_root       = trash_root,
			rename_threshold = rename_threshold,
			metadata_only    = metadata_only
		):
			logger.info(summary)

			if not dry_run:
				if op == "-":
					try:
						_move(src_file, dst_file, delete_empty_dirs_under=dst_root)
						results.delete_success += 1
						results.byte_diff += byte_diff
					except OSError as e:
						results.delete_error += 1
						msg = _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op == "+":
					try:
						_copy(src_file, dst_file, follow_symlinks=follow_symlinks)
						results.create_success += 1
						results.byte_diff += byte_diff
					except OSError as e:
						results.create_error += 1
						msg = _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op == "U":
					try:
						_copy(src_file, dst_file, follow_symlinks=follow_symlinks)
						results.update_success += 1
						results.byte_diff += byte_diff
					except OSError as e:
						results.update_error += 1
						msg = _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op == "R":
					try:
						_move(src_file, dst_file, delete_empty_dirs_under=dst_root)
						results.rename_success += 1
					except OSError as e:
						results.rename_error += 1
						msg = _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op == "D+":
					try:
						os.makedirs(dst_file, exist_ok=True)
						results.dir_create_success += 1
					except OSError as e:
						results.dir_create_error += 1
						msg = _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				elif op == "D-":
					try:
						_delete_empty_dirs(src_file, root=dst_root)
						results.dir_delete_success += 1
					except OSError as e:
						results.dir_delete_error += 1
						msg = _error_summary(e)
						logger.error(msg)
						results.errors.append(msg)
				else:
					assert False

		logger.info("")
		logger.info("*** psync finished successfully. ***")

		results.success = True

	except KeyboardInterrupt:
		logger.critical(f"Cancelled by user.")
	except (TypeError, ValueError) as e:
		logger.critical(f"Input Error: {e}")
	except Exception as e:
		logger.critical("Unexpected error: " + _error_summary(e))
		logger.critical(traceback.format_exc())

	finally:
		if dry_run:
			logger.info("")
			logger.info("*** DRY RUN ***")
		else:
			logger.info("")
			logger.info("Summary")
			logger.info("-------")
			logger.info(f"Rename Success: {results.rename_success}" + (f" / Failed: {results.rename_error}" if results.rename_error else ""))
			logger.info(f"Create Success: {results.create_success}" + (f" / Failed: {results.create_error}" if results.create_error else ""))
			logger.info(f"Update Success: {results.update_success}" + (f" / Failed: {results.update_error}" if results.update_error else ""))
			logger.info(f"Delete Success: {results.delete_success}" + (f" / Failed: {results.delete_error}" if results.delete_error else ""))
			logger.info(f"Net Change: {_human_readable_size(results.byte_diff)}")

		if results.err_count:
			logger.info("")
			logger.info(f"There were {results.err_count} errors.")
			if results.err_count <= 10:
				logger.info("Errors are reprinted below for convenience.")
				for error in results.errors:
					logger.info(error)

		if log_file:
			logger.info("")
			logger.info(f"Log file: {log_file}")

		if handler_stdout:
			logger.removeHandler(handler_stdout)

		if handler_stderr:
			logger.removeHandler(handler_stderr)

		if handler_file:
			logger.removeHandler(handler_file)
			handler_file.close()
			assert tmp_log_file is not None
			assert log_file is not None
			tmp_log_file.replace(log_file)

	return results

def _scandir(root:Path, *, filter:str = "+ **/*/ **/*", ignore_hidden:bool = False, follow_symlinks:bool = False) -> _FileList:
	'''
	Retrieves file information for all files under `root`, including relative paths (relative to `root`), sizes, and mtimes.

    Args
		root (Path)            : The directory to search.
		filter (str)           : The filter to include/exclude files and directories. Include file system entries by preceding a space-separated list with "+", and exclude with "-". Included files will be copied, while included directories will be searched. Each pattern ending with a slash will only apply to directories. Otherise the pattern will only apply to files. (Defaults to `+ **/*/ **/*`.)
		ignore_hidden (bool)   : Whether to skip hidden files by default. If `True`, then wildcards in glob patterns will not match file system entries beginning with a dot. However, globs containing a dot (e.g., "**/.*") will still match these file system entries. (Defaults to `False`.)
		follow_symlinks (bool) : Whether to follow symbolic links under `root`. Note that `root` itself will be followed regardless of this argument. (Defaults to `False`.)
	'''

	file_list = _FileList(
		root             = root,
		relpath_to_stats = {},
		real_names       = {},
		empty_dirs       = set(),
		visited_inodes   = set(),
	)
	f = _Filter(filter, ignore_hidden=ignore_hidden)

	for dir, subdirnames, file_entries in direntry_walk(root, followlinks=follow_symlinks):
		logger.debug(f"scanning: {dir}")

		if follow_symlinks:
			inode = os.stat(dir).st_ino
			if inode in file_list.visited_inodes:
				raise ValueError(f"Symlink circular reference: {dir}")
			file_list.visited_inodes.add(inode)

		# sorting may be needed if _listdir is changed to yield folder-by-folder
		#subdirnames.sort()
		#file_entries.sort()

		dir_relpath = os.path.relpath(dir, root)
		normed_dir_relpath = os.path.normcase(dir_relpath)

		# catalog empty directory
		if dir_relpath != "." and not file_entries and not subdirnames and f.filter(dir_relpath + os.sep):
			file_list.empty_dirs.add(normed_dir_relpath)
			file_list.real_names[normed_dir_relpath] = dir_relpath
			continue
		#else:
		#	self.nonempty_dirs.add(dir_relpath)

		# prune search tree
		i = 0
		while i < len(subdirnames):
			# symlinks are encountered here but they aren't followed unless followlinks is True
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
			normed_file_relpath = os.path.normcase(file_relpath)
			if (f.filter(file_relpath)):
				stat = entry.stat(follow_symlinks=follow_symlinks)
				meta = _Metadata(size = stat.st_size, mtime = stat.st_mtime)
				file_list.relpath_to_stats[normed_file_relpath] = meta
				file_list.real_names[normed_file_relpath] = file_relpath

	return file_list

def _operations(
		src_files        : _FileList,
		dst_files        : _FileList,
		*,
		trash_root       : Path | None,
		rename_threshold : int  | None,
		metadata_only    : bool
	):
	'''Generator of file system operations to perform for this backup.'''

	assert trash_root is None or isinstance(trash_root, Path)

	src_relpath_stats = src_files.relpath_to_stats
	dst_relpath_stats = dst_files.relpath_to_stats

	src_relpaths = set(src_relpath_stats.keys())
	dst_relpaths = set(dst_relpath_stats.keys())
	logger.debug(f"{src_relpaths=}")
	logger.debug(f"{dst_relpaths=}")

	src_only_relpaths = sorted(src_relpaths.difference(dst_relpaths))
	dst_only_relpaths = sorted(dst_relpaths.difference(src_relpaths))
	both_relpaths     = sorted(src_relpaths.intersection(dst_relpaths))
	logger.debug(f"{src_only_relpaths=}")
	logger.debug(f"{dst_only_relpaths=}")
	logger.debug(f"{both_relpaths=}")

	# Delete empty directories now in case any new files needs to take their places
	dst_only_empty_dirs = dst_files.empty_dirs.difference(src_files.empty_dirs)#.difference(src_files.empty_dirs)
	for relpath in dst_only_empty_dirs:
		dst_relpath_real = dst_files.real_names[relpath]
		src = dst_files.root / dst_relpath_real
		assert not any(src.iterdir())
		yield ("D-", src, None, 0, f"- {dst_relpath_real}{os.sep}")

	# Rename files
	if rename_threshold is not None:
		src_only_relpath_from_stats = _reverse_dict({path:src_relpath_stats[path] for path in src_only_relpaths})
		dst_only_relpath_from_stats = _reverse_dict({path:dst_relpath_stats[path] for path in dst_only_relpaths})

		for dst_relpath in list(dst_only_relpaths): # dst_only_relpaths is changed inside the loop
			# Ignore small files
			if dst_relpath_stats[dst_relpath].size < rename_threshold:
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
					on_dst = dst_files.root / rename_from
					on_src = src_files.root / rename_to
					if not _last_bytes(on_src) == _last_bytes(on_dst):
						continue

				src_only_relpaths.remove(rename_to)
				dst_only_relpaths.remove(rename_from)

				rename_from = dst_files.real_names[rename_from]
				rename_to = src_files.real_names[rename_to]

				src = dst_files.root / rename_from
				dst = dst_files.root / rename_to

				yield ("R", src, dst, 0, f"R {rename_from} -> {rename_to}")

			except KeyError:
				# dst file not a result of a rename
				continue

	# Delete files
	if trash_root is not None:
		for dst_relpath in dst_only_relpaths:
			dst_relpath_real = dst_files.real_names[dst_relpath]
			src = dst_files.root / dst_relpath_real
			dst = trash_root     / dst_relpath_real
			byte_diff = -dst_relpath_stats[dst_relpath].size
			yield ("-", src, dst, byte_diff, f"- {dst_relpath_real}")

	# Create files
	for src_relpath in src_only_relpaths:
		src_relpath_real = src_files.real_names[src_relpath]
		src = src_files.root / src_relpath_real
		dst = dst_files.root / src_relpath_real
		byte_diff = src_relpath_stats[src_relpath].size
		yield ("+", src, dst, byte_diff, f"+ {src_relpath_real}")

	# Update files that have newer mtimes
	for relpath in both_relpaths:
		src_relpath_real = src_files.real_names[relpath]
		dst_relpath_real = dst_files.real_names[relpath]
		src = src_files.root / src_relpath_real
		dst = dst_files.root / dst_relpath_real
		byte_diff = src_relpath_stats[relpath].size - dst_relpath_stats[relpath].size
		src_time = src_relpath_stats[relpath].mtime
		dst_time = dst_relpath_stats[relpath].mtime
		if src_time > dst_time:
			yield ("U", src, dst, byte_diff, f"U {dst_relpath_real}")
		elif src_time < dst_time:
			logger.warning(f"Working copy is older than backed-up copy, skipping update: {relpath}")

	# Create empty directories
	src_only_empty_dirs = src_files.empty_dirs.difference(dst_files.empty_dirs)#.difference(dst_files.nonempty_dirs)
	for relpath in src_only_empty_dirs:
		src_relpath_real = src_files.real_names[relpath]
		dst = dst_files.root / src_relpath_real
		yield ("D+", None, dst, 0, f"+ {src_relpath_real}{os.sep}")

def _reverse_dict(old_dict:dict[Any, Any]) -> dict[Any, Any]:
	'''
	Reverses a `dict` by swapping keys and values. If a value in `old_dict` appears more than once, then the corresponding key in the reversed `dict` will point to a `None`.

	>>> _reverse_dict({"a":1, "b":2, "c":2})[1]
	'a'
	>>> _reverse_dict({"a":1, "b":2, "c":2})[2] is None
	True
	'''

	reversed:dict[Any, Any] = {}
	for key, val in old_dict.items():
		if val in reversed:
			reversed[val] = None
		else:
			reversed[val] = key
	return reversed

def _copy(src:Path, dst:Path, *, exist_ok:bool = True, follow_symlinks:bool = False) -> None:
	'''Copy file from `src` to `dst`, keeping timestamp metadata. Existing files will be overwritten if `exist_ok` is `True`. Otherwise this method will raise a `FileExistsError`.'''

	if dst.exists():
		if not exist_ok:
			raise FileExistsError(f"Cannot copy, dst exists: {src} -> {dst}")
		if not dst.is_file():
			raise FileExistsError(f"Cannot copy, dst is not a file: {src} -> {dst}")
		elif src.samefile(dst):
			raise FileExistsError(f"Same file: {src} -> {dst}")

	delete_tmp = False
	dst_tmp = dst.with_name(dst.name + ".tempcopy")
	try:
		# Copy into a temp file, with metadata
		dir = dst.parent
		dir.mkdir(parents=True, exist_ok=True)
		shutil.copy2(src, dst_tmp, follow_symlinks=follow_symlinks)
		delete_tmp = True
		try:
			# Rename the temp file into the dest file
			dst_tmp.replace(dst)
			delete_tmp = False
		except PermissionError as e:
			# Remove read-only flag and try again
			make_readonly = False
			try:
				if not (dst.stat().st_mode & stat.S_IREAD):
					raise e
				dst.chmod(stat.S_IWRITE)
				make_readonly = True
				dst_tmp.replace(dst)
				delete_tmp = False
			finally:
				if make_readonly:
					dst.chmod(stat.S_IREAD)
	finally:
		# Remove the temp copy if there are any errors
		if delete_tmp:
			dst_tmp.unlink() # same as os.remove

def _move(src:Path, dst:Path, *, exist_ok:bool = False, delete_empty_dirs_under:Path|None = None) -> None:
	'''
	Move file from `src` to `dst`. Existing files will be overwritten if `exist_ok` is `True`. Otherwise this method will raise a `FileExistsError`.

	If `delete_empty_dirs_under` is supplied, then any empty directories created during this file move (and under this root directory) will be deleted.
	'''

	if dst.exists():
		if not exist_ok:
			raise FileExistsError(f"Cannot move, dst exists: {src} -> {dst}")
		if not dst.is_file():
			raise FileExistsError(f"Cannot move, dst is not a file: {src} -> {dst}")
		elif src.samefile(dst):
			raise FileExistsError(f"Same file: {src} -> {dst}")

	# move the file
	dir = dst.parent
	dir.mkdir(exist_ok=True, parents=True)
	src.replace(dst)

	# delete empty directories left after the move
	if delete_empty_dirs_under is not None:
		_delete_empty_dirs(src.parent, root=delete_empty_dirs_under)

def _delete_empty_dirs(dir:Path, *, root:Path) -> None:
	'''Iteratively delete empty directories, starting with `dir` and moving up to (but not including) `root`.'''

	if not dir.is_dir():
		raise ValueError(f"Expected a dir: {dir}")
	if not dir.is_relative_to(root):
		raise ValueError(f"root ({root}) is not an ancestor of dir ({dir})")
	#if any(dir.iterdir()):
	#	raise ValueError(f"Dir is not empty: {dir}")
	try:
		while dir != root and not any(dir.iterdir()):
			relpath = dir.relative_to(root)
			logger.debug(f"- {relpath}{os.sep}")
			dir.rmdir()
			dir = dir.parent
	except OSError as e:
		logger.warning(str(e))

def _last_bytes(file_path:Path, n:int = 1024) -> bytes:
	'''Reads and returns the last `n` bytes of a file.'''

	file_size = file_path.stat().st_size
	bytes_to_read = file_size if n > file_size else n
	with file_path.open("rb") as f:
		f.seek(-bytes_to_read, os.SEEK_END)
		return f.read()

def _human_readable_size(n:int) -> str:
	'''
	Translates `n` bytes into a human-readable size.

	>>> _human_readable_size(1023)
	'+1023 bytes'
	>>> _human_readable_size(-1024)
	'-1 KB'
	>>> _human_readable_size(2.1 * 1024 * 1024)
	'+2 MB'
	'''

	sign = "-" if n < 0 else "+"
	n = abs(n)
	units = ["bytes", "KB", "MB", "GB", "TB", "PB"]
	i = 0
	while n >= 1024 and i < len(units) - 1:
		n //= 1024
		i += 1
	return f"{sign}{round(n)} {units[i]}"

def _error_summary(e):
	'''Get a one-line summary of an Error.'''

	if isinstance(e, OSError):
		error_type = type(e).__name__
		affected_file = getattr(e, "filename", "N/A")
		msg = f"{error_type}: {affected_file}"
	else:
		error_type = type(e).__name__
		error_message = getattr(e, "strerror", "Unknown error")
		msg = f"{error_type}: {error_message}"
	return msg

def main() -> None:
	try:
		sync_cmd(sys.argv[1:])
	except SystemExit:
		# from argparse
		pass
	except Exception:
		print()
		traceback.print_exc()

if __name__ == "__main__":
	main()

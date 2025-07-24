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
from pathlib import Path
from fnmatch import fnmatch
from collections import Counter
from types import SimpleNamespace
from functools import lru_cache
from direntry_walk import direntry_walk
from typing import NamedTuple, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class DebugInfoFilter(logging.Filter):
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
	parser.add_argument("-t", "--trash-root", metavar="path", nargs="?", type=str, default=None, const="auto", help="The root directory to move 'extra' files (those that are in `dst_root` but not `src_root`). Must be on the same filesystem as `dst_root`. If set to \"auto\", then a directory will automatically be made next to `dst_root`. Extra files will not be moved if this option is omitted.")

	parser.add_argument("-f", "--filter", metavar="filter_string", nargs=1, type=str, default="+ **/*/ **/*", help="The filter to include/exclude files and directories. Similar to rsync, the format of the filter string is: (+ or -), followed by a list of one of more relative path patterns, and otionally repeat from the start. Including (+) or excluding (-) of entries is determined by the preceding symbol of the first matching pattern. Included files will be copied, while included directories will be searched. Each Pattern ending with \"/\" will apply to directories only. Otherise the pattern will apply only to files. (Defaults to \"+ **/*/ **/*\", which searches all directories and copies all files.)")
	parser.add_argument("--ignore-hidden", action="store_true", default=False, help="Skip hidden files by default. That is, wildcards in glob patterns will not match entries beginning with a dot. However, globs containing a dot (e.g., \"**/.*\") will still match these entries.")
	parser.add_argument("-L", "--follow-symlinks", action="store_true", default=False, help="Whether to follow symbolic links under `src_root` and `dst_root`. Note that `src_root` and `dst_root` themselves will be followed regardless of this argument.")
	parser.add_argument("-r", "--rename-threshold", metavar="size", nargs=1, type=int, default=20000, help="The minimum size in bytes needed to consider renaming files in dst_root to match those in `src_root`. Renamed files below this threshold will be simply deleted in dst_root and their replacements copied over.")
	parser.add_argument("-m", "--metadata_only", action="store_true", default=False, help="Use only metadata in determining which files in `dst_root` are the result of a rename. Otherwise, backup will also compare the last 1kb of files.")
	parser.add_argument("--dry-run", action="store_true", default=False, help="Forgo performing any operation that would make a filesystem change. Changes that would have occurred will still be printed to console.")

	parser.add_argument("--log", metavar="path", nargs="?", type=str, default=None, const="auto", help="File to write log messages to. If set to \"auto\", then a tempfile will be used for the log, and it will be moved to the user's home directory after the backup is done. If absent, then no logging will be performed.")
	parser.add_argument("-d", "--debug", action="store_true", default=False, help="Log debug messages.")
	parser.add_argument("-q", action="count", default=0, help="Forgo printing to stdout (-q) and stderr (-qq).")

	@staticmethod
	def parse(args:list[str]) -> argparse.Namespace:
		parsed_args = _ArgParser.parser.parse_args(args)
		parsed_args.quiet     = parsed_args.q >= 1
		parsed_args.veryquiet = parsed_args.q >= 2
		del parsed_args.q
		return parsed_args

class _Filter:
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
		for action, reobj in self.patterns:
			if reobj.match(relpath):
				return action
		return default

class _Metadata(NamedTuple):
	size  : int
	mtime : float

class _FileList(NamedTuple):
	root             : Path
	relpath_to_stats : dict[str, _Metadata]
	real_names       : dict[str, str]
	empty_dirs       : set[str]
	#nonempty_dirs   : set[str]
	visited_inodes   : set[int]

class Results:
	def __init__(self) -> None:
		self.trash_root : Path | None = None
		self.log_file   : Path | None = None

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
		return self.create_error + self.rename_error + self.update_error + self.delete_error + self.dir_create_success + self.dir_create_error + self.dir_delete_success + self.dir_delete_error

def backup2(args:list[str]) -> Results:
	'''Run backup with command line arguments.'''

	parsed_args = _ArgParser.parse(args)
	return backup(
		parsed_args.src_root,
		parsed_args.dst_root,
		trash            = parsed_args.trash_root,
		filter           = parsed_args.filter,
		ignore_hidden    = parsed_args.ignore_hidden,
		rename_threshold = parsed_args.rename_threshold,
		metadata_only    = parsed_args.metadata_only,
		dry_run          = parsed_args.dry_run,
		log              = parsed_args.log,
		debug            = parsed_args.debug,
		quiet            = parsed_args.quiet,
		veryquiet        = parsed_args.veryquiet
	)

def backup(
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
		src (str or PathLike)    : The root directory to copy files from. Can be a symlink to a directory.
		dst (str or PathLike)    : The root directory to copy files to. Can be a symlink to a directory.
		trash (str or PathLike)  : The root directory to move 'extra' files (those that are in `dst` but not `src`). Must be on the same filesystem as `dst`. If set to "auto", then a directory will automatically be made next to `dst`. Extra files will not be moved if this argument is `None`. (Defaults to `None`.)

		filter (str)             : The filter to include/exclude files and directories. Similar to rsync, the format of the filter string is: (+ or -), followed by a list of one of more relative path patterns, and otionally repeat from the start. Including (+) or excluding (-) of entries is determined by the preceding symbol of the first matching pattern. Included files will be copied, while included directories will be searched. Each Pattern ending with "/" will apply to directories only. Otherise the pattern will apply only to files. (Defaults to "+ **/*/ **/*", which searches all directories and copies all files.)
		ignore_hidden (bool)     : Whether to skip hidden files by default. If `True`, then wildcards in glob patterns will not match entries beginning with a dot. However, globs containing a dot (e.g., "**/.*") will still match these entries. (Defaults to `False`.)
		follow_symlinks (bool)   : Whether to follow symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed regardless of this argument. (Defaults to `False`.)
		rename_threshold (int)   : The minimum size in bytes needed to consider renaming files in `dst` that were renamed in `src`. Renamed files below this threshold will be simply deleted in `dst` and their replacements created. A value of `None` will mean no files in `dst` will be eligible for renaming. (Defaults to `10000`.)
		metadata_only (bool)     : Whether to use only metadata in determining which files in dst are the result of a rename. If set to False, `backup` will also compare the last 1kb of files. (Defaults to `False`.)
		dry_run (bool)           : Whether to hold off performing any operation that would make a filesystem change. Changes that would have occurred will still be printed to console. (Defaults to `False`.)

		log (str or PathLike)    : File to write log messages to. A value of "auto" means a tempfile will be used for the log, and it will be copied to the user's home directory after the backup is done. A value of `None` will skip logging. (Defaults to `None`.)
		debug (bool)             : Whether to log debug messages. (Default to `False`.)
		quiet (bool)             : Whether to forgo printing to stdout.
		veryquiet (bool)         : Whether to forgo printing to stdout and stderr.

	Console Output
		TODO

	Returns
		A `Results` object containing various statistics.
	'''
	results = Results()

	if veryquiet:
		quiet = True

	if logger.handlers:
		for handler in list(logger.handlers):
			logger.removeHandler(handler)

	handler_stdout = None
	handler_stderr = None
	handler_file   = None

	if not quiet:
		handler_stdout = logging.StreamHandler(sys.stdout)
		handler_stdout.setFormatter(logging.Formatter("%(message)s"))
		handler_stdout.addFilter(DebugInfoFilter())
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

		if src_root.exists() and not src_root.is_dir(): # should also allow a symlink pointing to a dir
			msg = f"Chosen src_root is not a directory: {src_root}"
			raise ValueError(msg)
		if dst_root.exists() and not dst_root.is_dir():
			msg = f"Chosen dst_root is not a directory: {dst_root}"
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
				msg = f"Chosen trash_root is not on the same filesystem as dst_root: {trash_root}"
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
		#logger.info("=" * width)
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
						err = str(e)
						logger.error(err)
						results.errors.append(err)
				elif op == "+":
					try:
						_copy(src_file, dst_file)
						results.create_success += 1
						results.byte_diff += byte_diff
					except OSError as e:
						results.create_error += 1
						err = str(e)
						logger.error(err)
						results.errors.append(err)
				elif op == "U":
					try:
						_copy(src_file, dst_file)
						results.update_success += 1
						results.byte_diff += byte_diff
					except OSError as e:
						results.update_error += 1
						err = str(e)
						logger.error(err)
						results.errors.append(err)
				elif op == "R":
					try:
						_move(src_file, dst_file, delete_empty_dirs_under=dst_root)
						results.rename_success += 1
					except OSError as e:
						results.rename_error += 1
						err = str(e)
						logger.error(err)
						results.errors.append(err)
				elif op == "D+":
					try:
						os.makedirs(dst_file, exist_ok=True)
						results.dir_create_success += 1
					except OSError as e:
						results.dir_create_error += 1
						err = str(e)
						logger.error(err)
						results.errors.append(err)
				elif op == "D-":
					try:
						_delete_empty_dirs(src_file, root=dst_root)
						results.dir_delete_success += 1
					except OSError as e:
						results.dir_delete_error += 1
						err = str(e)
						logger.error(err)
						results.errors.append(err)
				else:
					assert False

	except KeyboardInterrupt:
		logger.critical(f"Cancelled by user.")
	except (TypeError, ValueError) as e:
		logger.critical(f"Input Error: {e}")
	except Exception as e:
		logger.critical(str(e))
		logger.debug(traceback.format_exc())

	finally:
		if dry_run:
			logger.info("")
			logger.info("*** DRY RUN ***")
		else:
			logger.info("")
			logger.info("File Stats (Excluding Dirs)")
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
			handler_stdout.close()

		if handler_stderr:
			logger.removeHandler(handler_stderr)
			handler_stderr.close()

		if handler_file:
			logger.removeHandler(handler_file)
			handler_file.close()
			assert tmp_log_file is not None
			assert log_file is not None
			tmp_log_file.replace(log_file)

	return results

def _scandir(root:Path, *, filter:str = "+ **/*/ **/*", ignore_hidden:bool = False, follow_symlinks:bool = False) -> _FileList:
	'''
	Retrieves file relative paths (relative to `root`), sizes, and mtimes for all descendant files inside a directory.

    Args
		root (Path)            : The directory to search.
		filter (str)           : The filter to include/exclude files and directories. Include entries by preceding a space-separated list with "+", and exclude with "-". Included files will be copied, while included directories will be searched. Each pattern ending with a slash will only apply to directories. Otherise the pattern will only apply to files. (Defaults to `+ **/*/ **/*`.)
		ignore_hidden (bool)   : Whether to skip hidden files by default. If `True`, then wildcards in glob patterns will not match entries beginning with a dot. However, globs containing a dot (e.g., "**/.*") will still match these entries. (Defaults to `False`.)
		follow_symlinks (bool) : Whether to follow symbolic links under `src` and `dst`. Note that `src` and `dst` themselves will be followed if either is a symlink. (Defaults to `False`.)
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
		if not file_entries and not subdirnames:
			file_list.empty_dirs.add(dir_relpath)
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
			# ignore file symlinks for now, shutil.copy2() would follow and copy their contents if included in the output
			if entry.is_symlink():
				continue
			filename = entry.name
			file_path = os.path.join(dir, filename)
			file_relpath = os.path.relpath(file_path, root)
			normed_file_relpath = os.path.normcase(file_relpath)
			if (f.filter(file_relpath)):
				meta = _Metadata(size = entry.stat().st_size, mtime = entry.stat().st_mtime)
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
	'''Generator of the list of filesystem operations to perform for this backup.'''

	assert trash_root is None or isinstance(trash_root, Path)

	src_relpath_stats = src_files.relpath_to_stats
	dst_relpath_stats = dst_files.relpath_to_stats

	src_relpaths = set(src_relpath_stats.keys())
	dst_relpaths = set(dst_relpath_stats.keys())

	src_only_relpaths = sorted(src_relpaths.difference(dst_relpaths))
	dst_only_relpaths = sorted(dst_relpaths.difference(src_relpaths))
	both_relpaths     = sorted(src_relpaths.intersection(dst_relpaths))

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

	# Deleting must be done first or backing up a.jpg -> a.JPG (or similar) on Windows will fail
	if trash_root is not None:
		for dst_relpath in dst_only_relpaths:
			dst_relpath_real = dst_files.real_names[dst_relpath]
			src = dst_files.root / dst_relpath_real
			dst = trash_root     / dst_relpath_real
			byte_diff = -dst_relpath_stats[dst_relpath][0]
			yield ("-", src, dst, byte_diff, f"- {dst_relpath_real}")

	for src_relpath in src_only_relpaths:
		src_relpath_real = src_files.real_names[src_relpath]
		src = src_files.root / src_relpath_real
		dst = dst_files.root / src_relpath_real
		byte_diff = src_relpath_stats[src_relpath].size
		yield ("+", src, dst, byte_diff, f"+ {src_relpath_real}")

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

	# Empty directories
	src_only_empty_dirs = src_files.empty_dirs.difference(dst_files.empty_dirs)#.difference(dst_files.nonempty_dirs)
	for relpath in src_only_empty_dirs:
		dst_relpath_real = dst_files.real_names[relpath]
		dst = dst_files.root / dst_relpath_real
		if not dst.exists():
			yield ("D+", None, dst, 0, f"+ {dst_relpath_real}{os.sep}")
		elif not dst.is_dir():
			logger.error(f"FileExistsError: Could not create dir: {dst_relpath_real}")
	dst_only_empty_dirs = dst_files.empty_dirs.difference(src_files.empty_dirs)#.difference(src_files.empty_dirs)
	for relpath in dst_only_empty_dirs:
		dst_relpath_real = dst_files.real_names[relpath]
		src = dst_files.root / dst_relpath_real
		if not any(src.iterdir()):
			yield ("D-", src, None, 0, f"- {dst_relpath_real}{os.sep}")

def _reverse_dict(old_dict:dict[Any, Any]) -> dict[Any, Any]:
	'''
	Reverses a `dict` by swapping keys and values. If a value in `old_dict` appears more than once, then the corresponding key in the reversed `dict` will point to a value of `None`.

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

def _copy(src:Path, dst:Path, *, exist_ok:bool = True) -> None:
	'''Copy file from `src` to `dst`, keeping timestamp metadata. `exist_ok` must be `True` if `dst` exists. Otherwise this method will raise a `FileExistsError`.'''

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
		dir.mkdir(exist_ok=True)
		shutil.copy2(src, dst_tmp)
		delete_tmp = True
		try:
			# Rename the temp file into the dest file
			dst_tmp.replace(dst) # same as os.replace
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
	Move file from `src` to `dst`. `exist_ok` must be `True` if `dst` exists. Otherwise this method will raise a `FileExistsError`.

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
	src.rename(dst)

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
		logger.error(f"{e.__class__.__name__}: Failed to delete: {relpath}{os.sep}")

def _last_bytes(file_path:Path, n:int = 1024) -> bytes:
	'''Reads and returns the last `n` bytes of a file.'''

	file_size = file_path.stat().st_size
	bytes_to_read = file_size if n > file_size else n
	with open(file_path, "rb") as f:
		f.seek(-bytes_to_read, os.SEEK_END)
		return f.read()

def _human_readable_size(num_bytes:int) -> str:
	'''
	Translates `num_bytes` into a human-readable size.

	>>> _human_readable_size(1023)
	'1023 bytes'
	>>> _human_readable_size(1024)
	'1 KB'
	>>> _human_readable_size(2.1 * 1024 * 1024)
	'2 MB'
	'''

	sign = "-" if num_bytes < 0 else ""
	num_bytes = abs(num_bytes)
	units = ["bytes", "KB", "MB", "GB", "TB", "PB"]
	i = 0
	while num_bytes >= 1024 and i < len(units) - 1:
		num_bytes //= 1024
		i += 1
	return f"{sign}{round(num_bytes)} {units[i]}"

def main() -> None:
	try:
		backup2(sys.argv[1:])
	except SystemExit:
		# from argparse
		pass
	except Exception:
		print()
		traceback.print_exc()

if __name__ == "__main__":
	main()

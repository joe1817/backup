# Copyright (c) 2025 Joe Walter

# Terminology
# "entry" = file or directory
# "basename" = filename without the extension

import sys
import argparse
import os
import io
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

def backup(src_root, dst_root, *, trash_root=None, include=[], ignore_missing=False, exclude=[], rename_threshold=10000, metadata_only=False, dry_run=False, log_path="-", quiet=False, veryquiet=False):
	'''
	Copies new and updated files from `src_root` to `dst_root`, and optionally "deletes" files from `dst_root` if they are not present in `src_root` (they will be moved into `trash_root`, preserving directory structure). Furthermore, files that exist in `dst_root` but renamed in `src_root` may be renamed in `dst_root` to match. Candidates for rename are discovered by searching for files with an identical metadata signature, consisting of file size and modification time. These candidates must be above a minimum size threshold (`rename_threshold`) and have an unambiguously unique metadata signature within their respective root directories. The user is asked to confirm these renames before they are committed.

	Args
		src_root (str)         : The root directory to copy files from.
		dst_root (str)         : The root directory to copy files to.
		trash_root (str)       : The root directory to place files that are "deleted" from `dst_root`. Must be on the same filesystem as `dst_root`. Files will not be "deleted" if this is `None`. (Defaults to `None`.)
		include (list(str))    : A whitelist of relative paths that will exclude all other files and directories from the backup. Non-recursive globs (e.g., *.txt) are supported. (Defaults to `[]`.)
		ignore_missing (bool)  : Whether the relative paths indicated by `include` may point to non-existent files in `src_root`. (Defaults to `False`.)
		exclude (list(str))    : A blacklist of names and/or relative paths indicating files and directories to ignore. The blacklist is applied to entries in `src_root` and `dst_root`, except for those indicated by `include`. Entries ending with `os.sep` will be treated as a directory only. (Defaults to `[]`.)
		rename_threshold (int) : The minimum size in bytes needed to consider renaming files in `dst_root` that were renamed in `src_root`. Renamed files below this threshold will be simply deleted in `dst_root` and their replacements created. A value of `None` will mean no files in `dst_root` will be eligible for renaming. (Defaults to `10000`.)
		metadata_only (bool)   : Whether to use only metadata in determining which files in dst_root are the result of a rename. If set to False, `backup` will also compare the last 1kb of files. (Defaults to `False`.)
		dry_run (bool)         : Whether to hold off performing any operation that would make a filesystem change. Changes that would have occurred will still be printed to console. (Defaults to `False`.)
		log_path (str)         : File to write log messages to. A falsy value means no log will be created. A value of '-' means a tempfile will be used for the log, and it will be copied to the user's home directory after the backup is done. (Defaults to '-'.)
		quiet (bool)           : Whether to forgo printing to stdout.
		veryquiet (bool)       : Whether to forgo printing to stdout and stderr.

	Console Output


	Returns
		A `Results` object containing various statistics.
	'''
	with _LogManager(suppress_stdout=quiet, suppress_stderr=veryquiet) as log_manager:

		if veryquiet:
			quiet = True
		if dry_run and log_path == "-":
			log_path = None
		timestamp = str(int(time.time()*1000))
		trash_dir = os.path.join(trash_root, timestamp) if trash_root else None

		if not isinstance(src_root, str):
			msg = f"Bad type for arg 'src_root' (expected str): {src_root}"
			raise TypeError(msg)
		if not isinstance(dst_root, str):
			msg = f"Bad type for arg 'dst_root' (expected str): {dst_root}"
			raise TypeError(msg)
		if trash_root is not None and not isinstance(trash_root, str):
			msg = f"Bad type for arg 'trash_root' (expected str): {trash_root}"
			raise TypeError(msg)
		if trash_root is None:
			logger.warn("Trash flag (-t/--trash) present but no trash directory provided. Ignoring.")
		if not isinstance(include, list):
			msg = f"Bad type for arg 'include' (expected str): {include}"
			raise TypeError(msg)
		if not isinstance(ignore_missing, bool):
			msg = f"Bad type for arg 'ignore_missing' (expected bool): {ignore_missing}"
			raise TypeError(msg)
		if not isinstance(exclude, list):
			msg = f"Bad type for arg 'exclude' (expected list): {exclude}"
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
				msg = f"Could not create trash folder {timestamp} in trash_root: {trash_root}"
				raise ValueError(msg)
		if rename_threshold is not None and rename_threshold < 0:
			msg = f"rename_threshold must be non-negative: {rename_threshold}"
			raise ValueError(msg)
		if log_path is not None and os.path.exists(log_path):
			msg = f"Chosen log already exists: {log_path}"
			raise ValueError(msg)

		src_files = _listdir(src_root, include, ignore_missing, exclude)
		dst_files = _listdir(dst_root, include,           True, exclude)

		if log_path == "-":
			log_path = os.path.expanduser(os.path.join("~", f"py-backup.{timestamp}.log"))
			log_manager.log_path = log_path
		elif log_path:
			log_manager.log_path = log_path

		logger.debug(f"Starting backup: {src_root=} {dst_root=} {trash_root=} {timestamp=} {exclude=} {include=} {ignore_missing=} {rename_threshold=} {dry_run=} {log_path=} {quiet=} {veryquiet=}")

		width = max(len(src_root), len(dst_root)) + 3
		#logger.info("=" * width)
		logger.info("   " + src_root)
		logger.info("-> " + dst_root)
		logger.info("-" * width)

		if not dry_run:
			os.makedirs(dst_root, exist_ok=True)

		results = Results()

		for op, src, dst, byte_diff, summary in _operations(src_files, dst_files, src_root, dst_root, trash_dir, rename_threshold, metadata_only):
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
						os.rmdir(src)
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

def _fnmatch(path, pattern):
	if path.count(os.sep) != pattern.count(os.sep):
		return False
	return fnmatch(path, pattern)

@lru_cache
def _fnmatch_or_child(path, pattern):
	if path.count(os.sep) < pattern.count(os.sep):
		return False
	path = path.split(os.sep)
	pattern = pattern.split(os.sep)
	for a,b in zip(path, pattern):
		if not fnmatch(a, b):
			return False
	return True

def _pattern(pat):
	if "**" in pat:
		raise ValueError(f"Recursive globs ('**') are not supported in pattern arguments to include/exclude: {pat}")
	if pat == ".." or pat.startswith(f"..{os.sep}") or f"{os.sep}..{os.sep}" in pat or pat.endswith(f"{os.sep}.."):
		raise ValueError(f"Parent directories ('..') are not supported in pattern arguments to include/exclude: {pat}")
	if os.path.isabs(pat):
		raise ValueError(f"Absolute paths are not supported as arguments to include/exclude: {path}")
	pattern = SimpleNamespace()
	pattern.trailing_slash = pat[-1] == "/" or pat[-1] == "\\"
	pattern.current_dir    = pat.startswith("./") or pat.startswith(".\\")
	pattern.extension      = "." in pat[1:-1] and pat[-1] != "."
	pattern.pat            = os.path.normpath(pat)
	pattern.multipart      = os.sep in pattern.pat
	return pattern

def _listdir(root, include=[], exclude=[], ignore_missing=False, all_dir_patterns_are_paths=True):
	'''
	Retrieves file relative paths, sizes, and mtimes for files inside a directory. (All "relative paths" are relative to `root`.)

	If `include` is supplied, then the output will be a concatenation of `root` with each relative path in `include`.
	Otherwise, the output will be a recursive listing of all files in `root`, excluding files indicated by `exclude`.

    Args
		root (str)            : The directory to search.
		include (list)        : A list of relative paths of files to include in the output. Wildcards are not supported at this time. (Defaults to `[]`.)
		exclude (list)        : A list of names and relative paths to ignore while searching recursively. (Defaults to `[]`.)
		ignore_missing (bool) : Whether to ignore paths made using `include` that point to non-existent files. If False, this will raise a `ValueError` instead. (Defaults to `False`.)
		all_dir_patterns_are_paths (bool) : Whether to treat all directory patterns as relative paths. (Defaults to `True`.)

	Returns
		A SimpleNamespace containing two fields: `relpath_stats` and `empty_dirs`. `relpath_stats` is a `dict` with keys being each file's relative path and values being a `namedtuple` of file size (`size`) and modtime (`mtime`). `empty_dirs` is a set of relative paths to empty directories.

	Raises
		ValueError: If `ignore_missing` is `False` and any file indicated by a relative path in `include` does not exist.
	'''
	Metadata = namedtuple("Metadata", ["size", "mtime"])

	file_list = SimpleNamespace()
	file_list.relpath_stats = {}
	file_list.empty_dirs = set()
	#file_list.nonempty_dirs = set()
	file_list._scan_count = 0 # for debugging

	if isinstance(include, str):
		include = [include]
	if isinstance(exclude, str):
		exclude = [exclude]

	# categorize include patterns
	include_dirnames  = set()
	include_dirpaths  = set() # TODO include_dir_relpaths
	include_filenames = set()
	include_filepaths = set() # TODO include_file_relpaths
	for pat in include:
		pattern = _pattern(pat)
		if pattern.trailing_slash:
			# dir only
			if pattern.current_dir or pattern.multipart:
				include_dirpaths.add(pattern.pat)
			else:
				include_dirnames.add(pattern.pat)
		elif pattern.extension:
			if pattern.current_dir or pattern.multipart:
				include_filepaths.add(pattern.pat)
			else:
				include_filenames.add(pattern.pat)
		else:
			if pattern.current_dir or pattern.multipart:
				include_dirpaths.add(pattern.pat)
				include_filepaths.add(pattern.pat)
			else:
				include_dirnames.add(pattern.pat)
				include_filenames.add(pattern.pat)

	# categorize exclude patterns
	exclude_dirnames  = set()
	exclude_dirpaths  = set() # TODO exclude_dir_relpaths
	exclude_filenames = set()
	exclude_filepaths = set() # TODO exclude_file_relpaths
	for pat in exclude:
		pattern = _pattern(pat)
		if pattern.trailing_slash:
			# dir only
			if pattern.current_dir or pattern.multipart:
				exclude_dirpaths.add(pattern.pat)
			else:
				exclude_dirnames.add(pattern.pat)
		elif pattern.extension:
			if pattern.current_dir or pattern.multipart:
				exclude_filepaths.add(pattern.pat)
			else:
				exclude_filenames.add(pattern.pat)
		else:
			if pattern.current_dir or pattern.multipart:
				exclude_dirpaths.add(pattern.pat)
				exclude_filepaths.add(pattern.pat)
			else:
				exclude_dirnames.add(pattern.pat)
				exclude_filenames.add(pattern.pat)

	# if treating all dirnames as dirpaths
	if all_dir_patterns_are_paths:
		include_dirpaths |= include_dirnames
		include_dirnames = set()
		exclude_dirpaths |= exclude_dirnames
		exclude_dirnames = set()

	# gather dirs for a narrow search
	# if filename or dirname patterns exists then do a full scan
	ancestors_of_included = set()
	if not include_filenames and not include_dirnames:
		for dir in include_dirpaths:
			while dir:
				ancestors_of_included.add(dir)
				dir = os.path.dirname(dir)
		for dir in include_filepaths:
			while True:
				dir = os.path.dirname(dir)
				ancestors_of_included.add(dir)
				if not dir:
					break

	logger.debug(f"{ancestors_of_included=}")
	logger.debug(f"{include_dirnames=}")
	logger.debug(f"{include_dirpaths=}")
	logger.debug(f"{include_filenames=}")
	logger.debug(f"{include_filepaths=}")
	logger.debug(f"{exclude_dirnames=}")
	logger.debug(f"{exclude_dirpaths=}")
	logger.debug(f"{exclude_filenames=}")
	logger.debug(f"{exclude_filepaths=}")
	logger.debug(f"{ancestors_of_included=}")
	logger.debug("***")

	used_path_patterns = set()

	in_included_relpath_dir = False
	depth_in_included_relpath_dir = 9999

	for dir, subdirnames, file_entries in direntry_walk(root):
		logger.debug("_listdir: in " + dir)
		file_list._scan_count += 1

		# sorting may be needed if _listdir is changed to yield folder-by-folder
		#subdirnames.sort()
		#file_entries.sort()

		dirname     = os.path.basename(dir)
		dir_relpath = os.path.relpath(dir, root)
		depth       = dir_relpath.count(os.sep)

		if depth <= depth_in_included_relpath_dir:
			if any(_fnmatch_or_child(dir_relpath, pat) for pat in include_dirpaths):
				in_included_relpath_dir = True
				depth_in_included_relpath_dir = depth
			else:
				in_included_relpath_dir = False
				depth_in_included_relpath_dir = 9999

		# determine which dirpath pattern includes this dir, if any
		# TODO? move into depth calc section
		if not ignore_missing and depth == depth_in_included_relpath_dir:
			for pat in include_dirpaths:
				if _fnmatch(dir_relpath, pat): #okay to use fnmatch
					used_path_patterns.add(pat)
					break

		# determine if we're in an explicitly included dir
		# this may not be the case during full searches (i.e., searches with name patterns)
		in_included_dir =	(in_included_relpath_dir) or \
							(include_dirnames and any(_fnmatch(dirname, pat) for pat in include_dirnames))

		# catalog empty directory
		if not file_entries and not subdirnames:
			file_list.empty_dirs.add(dir_relpath)
			continue
		#else:
		#	file_list.nonempty_dirs.add(dir_relpath)

		# in case _listdir is given a recurse option
		#if not recursive:
		#	subdirnames[:] = []

		# prune search tree
		i = 0
		while i < len(subdirnames):
			# prune by excluded dir names
			subdirname = subdirnames[i]
			if any(_fnmatch(subdirname, pat) for pat in exclude_dirnames):
				del subdirnames[i]
				continue

			# prune by excluded dir path
			subdir_path = os.path.join(dir, subdirname)
			subdir_relpath = os.path.relpath(subdir_path, root)
			if any(_fnmatch(subdir_relpath, pat) for pat in exclude_dirpaths):
				del subdirnames[i]
				continue

			# prune search tree during narrow searches (i.e., searches with only relpath patterns)
			# paths need to be a child to an included dir or a parent to an included entry (file or dir)
			if not in_included_dir and ancestors_of_included and not any(_fnmatch(subdir_relpath, pat) for pat in ancestors_of_included):
				del subdirnames[i]
				continue
			i += 1

		for entry in file_entries:
			filename = entry.name
			# prune by excluded file names
			if any(_fnmatch(filename, pat) for pat in exclude_filenames):
				continue

			# prune by excluded file path
			file_path = os.path.join(dir, filename)
			file_relpath = os.path.relpath(file_path, root)
			if any(_fnmatch(file_relpath, pat) for pat in exclude_filepaths):
				continue

			# determine if this is an explicitly included file
			included_file = (include_filepaths and any(_fnmatch(file_relpath, pat) for pat in include_filepaths)) or \
							(include_filenames and any(_fnmatch(filename, pat) for pat in include_filenames))

			# determine which filepath pattern includes this file, if any
			if not ignore_missing:
				for pat in include_filepaths:
					if _fnmatch(file_relpath, pat):
						used_path_patterns.add(pat)
						break

			# ignore file if it is not specified by a file-select pattern or dir-select pattern
			if include and not in_included_dir and not included_file:
				continue

			# file meets all criteria, add it to the return result
			file_list.relpath_stats[file_relpath] = Metadata(entry.stat().st_size, entry.stat().st_mtime)

	# raise error if any explicitly included files were not found during the search, unless ignore_missing is True
	if not ignore_missing:
		unused_path_patterns = (include_dirpaths | include_filepaths) - used_path_patterns
		if unused_path_patterns:
			raise ValueError(f"Did not find these entries for backup: {unused_path_patterns}")

	return file_list

def _operations(src_files, dst_files, src_root, dst_root, trash_dir, rename_threshold, metadata_only):
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
	'''Copy src to dst, keeping metadata, and overwriting any existing file.'''
	if src.lower() == dst.lower(): #if os.path.samefile(src, dst):
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
	if os.path.exists(dst) and ("nt" not in os.name or src.lower() != dst.lower()):
		raise FileExistsError(f"File already exists: {src} -> {dst}")
	# move the file
	dir = os.path.dirname(dst)
	os.makedirs(dir, exist_ok=True)
	os.rename(src, dst)
	# delete empty directories left after the move
	if delete_empty_dirs:
		try:
			dir = src
			while True:
				dir = os.path.dirname(dir)
				if not os.listdir(dir):
					relpath = os.path.relpath(dir, root)
					logger.info(f"- {relpath}{os.sep}")
					os.rmdir(dir)
				else:
					break
		except OSError:
			logger.error(f"{e.__class__.__name__}: - {relpath}{os.sep}")

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
			with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_log:
				self._log_path = tmp_log.name
			formatter = logging.Formatter("%(levelname)s: %(message)s")
			self.log_handler_file = logging.FileHandler(self.log_path)
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
		self.log_records = []
		self.count_errs = 0
		self.critical_err = False
		self.log_file = None

	def emit(self, record):
		msg = self.format(record)+"\n"
		if record.levelname == "DEBUG" or record.levelname == "INFO":
			if not self.suppress_stdout:
				sys.stdout.write(msg)
		else:
			self.count_errs += 1
			if self.count_errs < self.max_err_recap:
				self.log_records.append(msg)
			if not self.suppress_stderr:
				sys.stderr.write(msg)
			if record.levelname == "CRITICAL":
				self.critical_err = True

	def close(self):
		if not self.suppress_stdout:
			if 0 < self.count_errs <= self.max_err_recap and not self.critical_err:
				sys.stdout.write("Errors are reprinted below for convenience:\n")
				for err in self.log_records:
					sys.stdout.write(err)

class _ArgParser:
	parser = argparse.ArgumentParser(
		description="Copy new and updated files from one directory to another, update renamed files' names to match where possible, and optionally delete non-matching files.",
		epilog="(c) 2025 Joe Walter")

	parser.add_argument("src_root", help="The root directory to copy files from.")
	parser.add_argument("dst_root", help="The root directory to copy files to.")

	parser.add_argument("-i", "--include", metavar="name_or_relpath", nargs="*", default=[], help="A whitelist of relative paths that will exclude all other files and directories from the backup. Non-recursive globs (e.g., *.txt) are supported.")
	parser.add_argument("-x", "--exclude", metavar="name_or_relpath", nargs="*", default=[], help=f"A blacklist of names and/or relative paths indicating files and directories to ignore. The blacklist is applied to entries in src_root and dst_root, except for those indicated by --include. Entries ending with {os.sep} will be treated as a directory only.")
	parser.add_argument("-t", "--trash-root", metavar="path", nargs="?", default=None, help="The root directory to place files that are 'deleted' from dst_root. Must be on the same filesystem as dst_root. Files will not be 'deleted' if this option is omitted.")
	parser.add_argument("--ignore-missing", action="store_true", default=False, help="Indicate the relative paths indicated by --include may point to non-existent files.")
	parser.add_argument("-r", "--rename-threshold", metavar="size", type=int, default=20000, help="The minimum size in bytes needed to consider renaming files in dst_root that were renamed in src_root. Renamed files below this threshold will be simply deleted in dst_root and their replacements created.")
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
		exclude          = args.exclude,
		include          = args.include,
		ignore_missing   = args.ignore_missing,
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

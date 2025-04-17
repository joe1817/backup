# Copyright (c) 2025 Joe Walter

# TODO break backup() into parts: 1. list files, 2. create +/-/R/U subsets, 3. perform operations
# TODO test cases for above
# TODO write a method to do backup()s in stages for large directories

import sys
import argparse
import os
import io
import stat
import shutil
import fnmatch
import traceback
import tempfile
import logging
import time
from collections import Counter
from collections import namedtuple

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
		self.diff_bytes = 0

	@property
	def errors(self):
		return self.create_error + self.rename_error + self.update_error + self.delete_error

def backup(src_root, dst_root, *, trash_root=None, exclude=[], only=[], ignore_missing=False, rename_threshold=10000, metadata_only=False, dry_run=False, log_path="-", quiet=False, veryquiet=False):
	'''
	Copies new and updated files from `src_root` to `dst_root`, and optionally "deletes" files from `dst_root` if they are not present in `src_root` (they will be moved into `trash_root`, preserving directory structure). Furthermore, files that exist in `dst_root` but renamed in `src_root` may be renamed in `dst_root` to match. Candidates for rename are discovered by searching for files with an identical metadata signature, consisting of file size and modification time. These candidates must be above a minimum size threshold (`rename_threshold`) and have an unambiguously unique metadata signature within their respective root directories. The user is asked to confirm these renames before they are committed.

	Args
		src_root (str)         : The root directory to copy files from.
		dst_root (str)         : The root directory to copy files to.
		trash_root (str)       : The root directory to place files that are "deleted" from `dst_root`. Files will not be "deleted" if this is `None`. (Defaults to `None`.)
		only (list(str))       : A whitelist of file relative paths that will exclude all other files and directories from the backup. Mutually exclusive with the `exclude` parameter. (Defaults to `[]`.)
		exclude (list(str))    : A blacklist of names and/or relative paths indicating files and directories to ignore. The blacklist is applied to both `src_root` and `dst_root`. Entries ending with `os.sep` will be treated as a directory only. Mutually exclusive with the `only` parameter. (Defaults to `[]`.)
		ignore_missing (bool)  : Whether the relative paths indicated by `only` may point to non-existent files. (Defaults to `False`.)
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

	if veryquiet:
		quiet = True

	if not isinstance(src_root, str):
		msg = f"Bad type for arg 'src_root' (expected str): {src_root}"
		raise TypeError(msg)
	if not isinstance(dst_root, str):
		msg = f"Bad type for arg 'dst_root' (expected str): {dst_root}"
		raise TypeError(msg)
	if trash_root is not None and not isinstance(trash_root, str):
		msg = f"Bad type for arg 'trash_root' (expected str): {trash_root}"
		raise TypeError(msg)
	if not isinstance(exclude, list):
		msg = f"Bad type for arg 'exclude' (expected list): {exclude}"
		raise TypeError(msg)
	if not isinstance(only, list):
		msg = f"Bad type for arg 'only' (expected str): {only}"
		raise TypeError(msg)
	if not isinstance(ignore_missing, bool):
		msg = f"Bad type for arg 'ignore_missing' (expected bool): {ignore_missing}"
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
		if not os.path.isdir(trash_root):
			msg = f"Chosen trash_root is not a directory: {trash_root}"
			raise ValueError(msg)
		if os.stat(trash_root).st_dev != os.stat(dst_root).st_dev:
			msg = f"Chosen trash_root is not on the same filesystem as dst_root: {trash_root}"
			raise ValueError(msg)
	if rename_threshold is not None and rename_threshold < 0:
		msg = f"rename_threshold must be non-negative: {rename_threshold}"
		raise ValueError(msg)
	if log_path is not None and os.path.exists(log_path):
		msg = f"Chosen log already exists: {log_path}"
		raise ValueError(msg)

	with _LogManager(log_path, suppress_stdout=quiet, suppress_stderr=veryquiet):
		logger.debug(f"Starting backup: {src_root=} {dst_root=} {trash_root=} {exclude=} {only=} {ignore_missing=} {rename_threshold=} {dry_run=} {log_path=} {quiet=} {veryquiet=}")
		results = Results()

		width = max(len(src_root), len(dst_root)) + 3
		#logger.info("=" * width)
		logger.info("   " + src_root)
		logger.info("-> " + dst_root)
		logger.info("-" * width)

		if not dry_run:
			os.makedirs(dst_root, exist_ok=True)

		src_relpath_stats = _listdir(src_root, exclude=exclude, only=only, ignore_missing=ignore_missing)
		dst_relpath_stats = _listdir(dst_root, exclude=exclude, only=only, ignore_missing=ignore_missing)

		src_relpaths = set(src_relpath_stats.keys())
		dst_relpaths = set(dst_relpath_stats.keys())

		src_only_relpaths = sorted(src_relpaths.difference(dst_relpaths))
		dst_only_relpaths = sorted(dst_relpaths.difference(src_relpaths))
		both_relpaths     = sorted(src_relpaths.intersection(dst_relpaths))

		if rename_threshold is not None:
			src_only_relpath_from_stats = _reverse_dict({path:src_relpath_stats[path] for path in src_only_relpaths})
			dst_only_relpath_from_stats = _reverse_dict({path:dst_relpath_stats[path] for path in dst_only_relpaths})
			
			for dst_relpath in dst_only_relpaths:
				if dst_relpath_stats[dst_relpath][0] < rename_threshold:
					# Ignore renaming small files
					continue
				try:
					rename_to = src_only_relpath_from_stats[dst_relpath_stats[dst_relpath]]
					if rename_to is None:
						# dst file may be the result of a rename, but there are multiple candidates
						continue
					rename_from = dst_only_relpath_from_stats[dst_relpath_stats[dst_relpath]]
					if rename_from is None:
						# dst file may be the result of a rename, but there are multiple candidates
						continue
					
					# compare last 1kb
					if not metadata_only:
						on_dst = os.path.join(dst_root, rename_from)
						on_src = os.path.join(src_root, rename_to)
						if not _last_bytes(on_src) == _last_bytes(on_dst):
							continue
					
					logger.info(f"R {rename_from} -> {rename_to}")
					src_only_relpaths.remove(rename_to)
					dst_only_relpaths.remove(rename_from)
					
					#rename
					if not dry_run:
						src = os.path.join(dst_root, rename_from)
						dst = os.path.join(dst_root, rename_to)
						try:
							_move(src, dst)
							results.rename_success += 1
						except OSError as e:
							results.rename_error += 1
							logger.error(f"{e.__class__.__name__}: R {src} -> {dst}", extra={})
					
				except KeyError:
					# dst file not a result of a rename
					continue

		# Deleting must be done first or backing up a.jpg -> a.JPG (or similar) on Windows will fail
		if trash_root:
			for dst_relpath in dst_only_relpaths:
				src = os.path.join(  dst_root, dst_relpath)
				dst = os.path.join(trash_root, dst_relpath)
				logger.info(f"- {dst_relpath}")
				if not dry_run:
					try:
						_move(src, dst)
						results.delete_success += 1
						results.diff_bytes -= dst_relpath_stats[dst_relpath][0]
					except OSError as e:
						results.delete_error += 1
						logger.error(f"{e.__class__.__name__}: - {dst}")

		for src_relpath in src_only_relpaths:
			src = os.path.join(src_root, src_relpath)
			dst = os.path.join(dst_root, src_relpath)
			logger.info(f"+ {src_relpath}")
			if not dry_run:
				try:
					_copy(src, dst)
					results.create_success += 1
					results.diff_bytes += src_relpath_stats[src_relpath].size
				except OSError as e:
					results.create_error += 1
					logger.error(f"{e.__class__.__name__}: + {dst}")

		for relpath in both_relpaths:
			src = os.path.join(src_root, relpath)
			dst = os.path.join(dst_root, relpath)
			src_time = src_relpath_stats[relpath].mtime
			dst_time = dst_relpath_stats[relpath].mtime
			if src_time > dst_time:
				logger.info(f"U {relpath}")
				if not dry_run:
					try:
						_copy(src, dst)
						results.update_success += 1
						results.diff_bytes += src_relpath_stats[relpath].size - dst_relpath_stats[relpath].size
					except OSError as e:
						results.update_error += 1
						logger.error(f"{e.__class__.__name__}: U {dst}")
			elif src_time < dst_time:
				logger.warn(f"Working copy is older than backed-up copy, skipping: {src}")

		logger.info("")
		logger.info(f"Rename Success: {results.rename_success}" + (f" / Failed: {results.rename_error}" if results.rename_error else ""))
		logger.info(f"Create Success: {results.create_success}" + (f" / Failed: {results.create_error}" if results.create_error else ""))
		logger.info(f"Update Success: {results.update_success}" + (f" / Failed: {results.update_error}" if results.update_error else ""))
		logger.info(f"Delete Success: {results.delete_success}" + (f" / Failed: {results.delete_error}" if results.delete_error else ""))
		logger.info(f"Net Change in Bytes: {_human_readable_size(results.diff_bytes)}")

		return results

def _listdir(root, *, exclude=[], only=[], ignore_missing=False):
	'''
	Retrieves file relative paths, sizes, and mtimes for files inside a directory. (All "relative paths" are relative to `root`.)

	If `only` is supplied, then the output will be a concatenation of `root` with each relative path in `only`.
	Otherwise, the output will be a recursive listing of all files in `root`, excluding files indicated by `exclude`.

    Args
		root (str)            : The directory to search.
		exclude (list)        : A list of names and relative paths to ignore while searching recursively. (Defaults to `[]`.)
		only (list)           : A list of relative paths of files to include in the output. (Defaults to `[]`.)
		ignore_missing (bool) : Whether to ignore paths made using `only` that point to non-existent files. If False, this will raise a `FileNotFoundError` instead. (Defaults to `False`.)

	Returns
		A `dict` with keys being each file's relative path and values being a `namedtuple` of file size ("size") and modtime ("mtime").

	Raises
		FileNotFoundError: If `ignore_missing` is `False` and any file indicated by a relative path in `only` does not exist.
	'''

	Metadata = namedtuple("Metadata", ["size", "mtime"])

	relpaths = {}

	if only:

		if isinstance(only, str):
			only = [only]

		for relpath in only:
			#if any(relpath.endswith(ft) for ft in exclude_filetypes):
			#	continue
			path = os.path.join(root, relpath)
			if not os.path.isfile(path):
				if ignore_missing:
					continue
				raise FileNotFoundError(path)
			stats = os.stat(path)
			relpaths[relpath] = (stats.st_size, stats.st_mtime)
		return relpaths

	else:

		if isinstance(exclude, str):
			exclude = [exclude]

		exclude_files = set(f for f in exclude if not f.endswith(os.sep))
		exclude_dirs  = set(f[:-1] if f.endswith(os.sep) else f for f in exclude)

		exclude_dirnames  = set(f for f in exclude_dirs if os.sep not in f)
		exclude_dirpaths  = set(os.path.join(root, f) for f in exclude_dirs if os.sep in f)
		exclude_filenames = set(f for f in exclude_files if os.sep not in f)
		exclude_filepaths = set(os.path.join(root, f) for f in exclude_files if os.sep in f)

		for dir, dirnames, filenames in os.walk(root):
			i = 0
			while i < len(dirnames):
				dirname = dirnames[i]
				if any(fnmatch.fnmatch(dirname, pat) for pat in exclude_dirnames):
					del dirnames[i]
					i -= 1
				else:
					dirpath = os.path.join(dir, dirname)
					if any(fnmatch.fnmatch(dirpath, pat) for pat in exclude_dirpaths):
						del dirnames[i]
						i -= 1
				i += 1
			for filename in filenames:
				if any(fnmatch.fnmatch(filename, pat) for pat in exclude_filenames):
					continue
				filepath = os.path.join(dir, filename)
				if any(fnmatch.fnmatch(filepath, pat) for pat in exclude_filepaths):
					continue
				relpath = os.path.relpath(filepath, root)
				stats = os.stat(filepath)
				relpaths[relpath] = Metadata(stats.st_size, stats.st_mtime)
		return relpaths

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

def _move(src, dst, *, delete_empty_dirs=True):
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
		#try:
		dir = src
		while True:
			dir = os.path.dirname(dir)
			if not os.listdir(dir):
				# print(f"- {dir}")
				os.rmdir(dir)
			else:
				break
		#except OSError:
		#	pass

def _last_bytes(file_path, n=1024):
	file_size = os.path.getsize(file_path)
	bytes_to_read = file_size if n > file_size else n
	with open(file_path, "rb") as f:
		f.seek(-bytes_to_read, os.SEEK_END)
		return f.read()

def _human_readable_size(num_bytes):
	units = ["bytes", "KB", "MB", "GB", "TB", "PB"]
	i = 0
	while num_bytes >= 1024 and i < len(units) - 1:
		num_bytes /= 1024
		i += 1
	return f"{round(num_bytes)} {units[i]}"

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

class _ConsoleHandler(logging.Handler):
	def __init__(self, suppress_stdout, suppress_stderr, max_err_recap=0):
		super().__init__()
		self.suppress_stdout = suppress_stdout
		self.suppress_stderr = suppress_stderr
		self.max_err_recap = max_err_recap
		self.log_records = []
		self.count_errs = 0
		self.critical_err = False

	def emit(self, record):
		msg = self.format(record)+"\n"
		if record.levelname == "DEBUG" or record.levelname == "INFO":
			if not self.suppress_stdout:
				sys.stdout.write(msg)
		else:
			self.count_errs += 1
			if self.count_errs <= self.max_err_recap:
				self.log_records.append(msg)
			if not self.suppress_stderr:
				sys.stderr.write(msg)
			if record.levelname == "CRITICAL":
				self.critical_err = True

	def close(self, log_path):
		if not self.suppress_stderr:
			if self.count_errs > 0:
				if self.critical_err:
					sys.stderr.write("Encountered a critical internal issue. This is not due to user input.\n")
				else:
					sys.stderr.write(f"Encountered {self.count_errs} filesystem errors.\n")
				if log_path:
					sys.stderr.write(f"See the log at {log_path} for details.\n")
			if 0 < self.count_errs <= self.max_err_recap and not self.critical_err:
				sys.stderr.write("Errors are reprinted below for convenience:\n")
				for err in self.log_records:
					sys.stderr.write(err)

class _LogManager:
	def __init__(self, log_path ,*, suppress_stdout=False, suppress_stderr=False):
		self.log_path = log_path
		self.final_log_path = log_path
		self.log_handler_file = None

		self.suppress_stdout = suppress_stdout
		self.suppress_stderr = suppress_stderr
		self.log_handler_console = None

	def __enter__(self):
		# file log
		if self.log_path == "-":
			with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_log:
				self.log_path = tmp_log.name
			self.final_log_path = os.path.expanduser(os.path.join("~", f"py-backup.{int(time.time()*1000)}.log"))
		if self.log_path:
			formatter = logging.Formatter("%(levelname)s: %(message)s")
			self.log_handler_file = logging.FileHandler(self.log_path)
			self.log_handler_file.setFormatter(formatter)
			self.log_handler_file.setLevel(logging.DEBUG)
			logger.addHandler(self.log_handler_file)

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
			logger.critical(f"{exc_type.__name__}: {exc_value}")
			logger.debug(traceback.format_exc())

		if self.log_handler_file:
			logger.removeHandler(self.log_handler_file)
			self.log_handler_file.close()
			if self.log_path != self.final_log_path:
				os.replace(self.log_path, self.final_log_path)
				self.log_path = self.final_log_path

		if self.log_handler_console:
			self.log_handler_console.close(self.log_path)
			logger.removeHandler(self.log_handler_console)

class _ArgParser:
	parser = argparse.ArgumentParser(
		description="Copy new and updated files from one directory to another, update renamed files' names to match where possible, and optionally delete non-matching files.",
		epilog="(c) 2025 Joe Walter")
	parser.add_argument("src_root", help="The root directory to copy files from.")
	parser.add_argument("dst_root", help="The root directory to copy files to.")

	group = parser.add_mutually_exclusive_group(required=False)
	group.add_argument("--only", metavar="name_or_relpath", nargs="+", default=[], help="A whitelist of file relative paths that will exclude all other files and directories from the backup. Mutually exclusive with --exclude.")
	group.add_argument("-x", "--exclude", metavar="name_or_relpath", nargs="+", default=[], help=f"A blacklist of names and/or relative paths indicating files and directories to ignore. The blacklist is applied to both src_root and dst_root. Entries ending with {os.sep} will be treated as a directory only. Mutually exclusive with --only.")

	parser.add_argument("-t", "--trash-root", metavar="path", default=None, help="A root directory to place files that are 'deleted' from dst_root. Must be in the same filesystem as dst_root. Files will not be 'deleted' if this option is omitted.")
	parser.add_argument("--ignore-missing", action="store_true", default=False, help="Indicate the relative paths indicated by --only may point to non-existent files.")
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
		only             = args.only,
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
		if results.errors:
			print("Finished with errors.")
			sys.exit(3)
		else:
			print("Finished successfully.")
			sys.exit(0)
	except KeyboardInterrupt:
		print("Cancelled by user.")
		sys.exit(130)
	except (ValueError, TypeError) as e:
		print(f"Input Error: {e}")
		sys.exit(2)
	except Exception:
		traceback.print_exc()
		sys.exit(1)

if __name__ == "__main__":
	main()

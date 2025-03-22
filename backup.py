import os, shutil, stat

def copy(src, dst):
	'''Copy src to dst, keeping metadata, and overwriting any existing file.'''
	if src.lower() == dst.lower(): #if os.path.samefile(src, dst):
		return False
	try:
		# Copy into a temp file, with metadata
		dir = os.path.dirname(dst)
		os.makedirs(dir, exist_ok=True)
		shutil.copy2(src, dst+".tempcopy")
		try:
			# Rename the temp file into the dest file
			os.replace(dst+".tempcopy", dst)
		except PermissionError as e:
			# Remove read-only flag and try again
			make_readonly = False
			try:
				if not (os.stat(dst).st_mode & stat.S_IREAD):
					raise e
				os.chmod(dst, stat.S_IWRITE)
				make_readonly = True
				os.replace(dst+".tempcopy", dst)
				os.chmod(dst, stat.S_IREAD)
				return True
			except Exception as e:
				if make_readonly:
					os.chmod(dst, stat.S_IREAD)					
				raise e
		return True
	except Exception as e:
		# Remove the temp copy if there are any errors
		try:
			os.remove(dst+".tempcopy")
		except FileNotFoundError:
			pass
		if isinstance(e, KeyboardInterrupt):
			raise e
		return False

def move(src, dst, delete_empty_dirs=True):
	'''Move src to dst (on the same filesystem), failing if dst exists.'''
	if os.path.exists(dst) and src != dst:
		return False
	try:
		dir = os.path.dirname(dst)
		os.makedirs(dir, exist_ok=True)
		os.replace(src, dst)

		if delete_empty_dirs:
			try:
				dir = src
				while True:
					dir = os.path.dirname(dir)
					if not os.listdir(dir):
						# print(f"- {dir}")
						os.rmdir(dir)
					else:
						break
			except OSError:
				pass

		return True
	except KeyboardInterrupt as e:
		raise e
	except:
		return False

def listdir(root, exclude_dirs=[], exclude_filetypes=[], only_files=[], missing_ok=False):
	'''
	Retrieves file paths, sizes, and mtime's for files inside a directory.
	
	If only_files is supplied, then only the concatenation of root with each relative path in only_files will be in the output.
	Otherwise, a recursive listing of all files will be in the output.
	
	Args
		root (str)               : The directory to search.
		exclude_dirs (list)      : A list of directory names to ignore while searching recursively.
		exclude_filetypes (list) : A list of filetypes to ignore while searching recursively. Filetypes should start with a dot.
		only_files (list)        : A list of paths relative to root that will be in the output.
		missing_ok (bool)        : Whether filepaths made using only_files can point to nonexistent files.

	Returns
		A dict from each file's path (relative to root) to a tuple of its size and mtime.
	
	Raises
		FileNotFoundError: If missing_ok is False and any file given by only_files does not exist.
	'''
	relpaths = {}

	if only_files:
		for relpath in only_files:
			#if any(relpath.endswith(ft) for ft in exclude_filetypes):
			#	continue
			path = os.path.join(root, relpath)
			if not os.path.isfile(path):
				if missing_ok:
					continue
				raise FileNotFoundError(path)
			stats = os.stat(path)
			relpaths[relpath] = (stats.st_size, stats.st_mtime)
		return relpaths

	for dir, dirnames, filenames in os.walk(root):
		for d in exclude_dirs:
			if d in dirnames:
				dirnames.remove(d)
		for filename in filenames:
			if any(filename.endswith(ft) for ft in exclude_filetypes):
				continue
			filepath = os.path.join(dir, filename)
			relpath = os.path.relpath(filepath, root)
			stats = os.stat(filepath)
			relpaths[relpath] = (stats.st_size, stats.st_mtime)
	return relpaths

def _reverse_dict(old_dict):
	'''
	Reverses a dict, allowing quick retrieval of keys from values.
	
	If a value in old_dict appears more than once, then the corresponding key in the reversed dict will point to a value of None.
	'''
	reversed = {}
	for key, val in old_dict.items():
		if val in reversed:
			reversed[val] = None
		else:
			reversed[val] = key
	return reversed

def backup(*, src_root, dst_root, trash_root=None, exclude_dirs=[], exclude_filetypes=[], only_files=[], missing_ok=False, rename_threshold=20000, dry_run=True):
	if not dry_run:
		os.makedirs(dst_root, exist_ok=True)

	src_relpath_stats = listdir(src_root, only_files, exclude_dirs, exclude_filetypes)
	dst_relpath_stats = listdir(dst_root, only_files, exclude_dirs, exclude_filetypes)

	src_relpaths = set(src_relpath_stats.keys())
	dst_relpaths = set(dst_relpath_stats.keys())

	src_only_relpaths = src_relpaths.difference(dst_relpaths)
	dst_only_relpaths = dst_relpaths.difference(src_relpaths)
	both_relpaths     = src_relpaths.intersection(dst_relpaths)

	errors = []

	if rename_threshold is not None:
		proposed_renames = []
		src_only_relpath_from_stats = _reverse_dict({path:src_relpath_stats[path] for path in src_only_relpaths})
		dst_only_relpath_from_stats = _reverse_dict({path:dst_relpath_stats[path] for path in dst_only_relpaths})
		src_only_relpaths_with_renames = src_only_relpaths.copy()
		dst_only_relpaths_with_renames = dst_only_relpaths.copy()
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
				src_only_relpaths_with_renames.remove(rename_to)
				dst_only_relpaths_with_renames.remove(rename_from)
				proposed_renames.append((rename_from, rename_to))
			except KeyError:
				# dst file not a result of a rename
				continue

		if proposed_renames:
			print("Proposed renames:")
			for rename_from, rename_to in proposed_renames:
				print(f"R {rename_from} ->")
				print(f"  {rename_to}")

			print("Accept these renames? [y] yes, then continue (default)  [Y] yes, then quit  [n] no, then continue  [N] no, and quit")
			ans = input("> ").strip()		
			if ans == "":
				ans = "y"
			assert ans in ["Y", "y", "N", "n"]
			if ans == "N":
				return

			# If renames are accepted
			if ans == "y" or ans == "Y":
				src_only_relpaths = src_only_relpaths_with_renames
				dst_only_relpaths = dst_only_relpaths_with_renames

				if not dry_run:
					for rename_from, rename_to in proposed_renames:
						src = os.path.join(dst_root, rename_from)
						dst = os.path.join(dst_root, rename_to)
						success = move(src, dst)
						if not success:
							errors.append(f"[*ERR] Failed to rename {src} to {dst}")

			if ans == "Y":
				return

	# Deleting must be done first or backing up a.jpg -> a.JPG (or similar) on Windows will fail
	if trash_root:
		for dst_relpath in dst_only_relpaths:
			src = os.path.join(  dst_root, dst_relpath)
			dst = os.path.join(trash_root, dst_relpath)
			print(f"- {src}")
			if not dry_run:
				success = move(src, dst)
				if not success:
					errors.append(f"[*ERR] Failed to delete {src}")

	for src_relpath in src_only_relpaths:
		src = os.path.join(src_root, src_relpath)
		dst = os.path.join(dst_root, src_relpath)
		print(f"+ {dst}")
		if not dry_run:
			success = copy(src, dst)
			if not success:
				errors.append(f"[*ERR] Failed to create {dst}")

	for relpath in both_relpaths:
		src_time = src_relpath_stats[relpath][1]
		dst_time = dst_relpath_stats[relpath][1]
		if src_time > dst_time:
			src = os.path.join(src_root, relpath)
			dst = os.path.join(dst_root, relpath)
			print(f"U {dst}")
			if not dry_run:
				success = copy(src, dst)
				if not success:
					errors.append(f"[*ERR] Failed to update {dst}")
		elif src_time < dst_time:
			errors.append(f"[WARN] Working copy is older than backed-up copy: {src}")

	for err in errors:
		print(err)

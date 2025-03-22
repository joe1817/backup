import os, time, re, shutil, subprocess #, tempfile

def copy(src, dst):
	#if os.path.samefile(src, dst):
	if src == dst:
		return False
	try:
		dir = os.path.dirname(dst)
		os.makedirs(dir, exist_ok=True)
		shutil.copy2(src, dst+'.tempcopy')
		try:
			os.replace(dst+'.tempcopy', dst)
		except PermissionError as e:
			if os.name == 'nt':
				# os.system(f'attrib -r "{dst}"')
				subprocess.run(["attrib", "-r", dst])
				os.replace(dst+'.tempcopy', dst)
				subprocess.run(["attrib", "+r", dst])
			else:
				raise e
		return True
	except:
		try:
			os.remove(dst+'.tempcopy')
		except FileNotFoundError:
			pass
		return False

def move(src, dst, delete_empty_dirs=True):
	if os.path.exists(dst):
		return False
	try:
		dir = os.path.dirname(dst)
		os.makedirs(dir, exist_ok=True)
		os.replace(src, dst)

		if delete_empty_dirs:
			dir = src
			while True:
				try:
					dir = os.path.dirname(dir)
					if not os.listdir(dir):
						# print(f"- {dir}")
						os.rmdir(dir)
					else:
						break
				except OSError:
					break

		return True
	except:
		return False

def scan(dir, exclude_dirs, exclude_file_types):
	def _scan(root, dir, exclude_dirs, exclude_file_types):
		for entry in os.scandir(dir):
			if entry.is_symlink() or entry.is_junction():
				continue
			elif entry.is_dir():
				if entry.name not in exclude_dirs:
					yield from _scan(root, entry.path, exclude_dirs, exclude_file_types)
			else:
				if any(entry.name.endswith(ext) for ext in exclude_file_types):
					continue
				yield (os.path.relpath(entry.path, root), entry)
	return {relpath:entry for relpath,entry in _scan(dir, dir, exclude_dirs, exclude_file_types)}

'''
def scan(dir, exclude_dirs, exclude_file_types):
	"""
	Returns a list of pathnames (relative to dir) for all files within dir.
	"""
	files = set()
	for root, dirs, filenames in os.walk(dir, followlinks=False, topdown=True):
		dirs[:] = [d for d in dirs if d not in exclude_dirs]
		for filename in filenames:
			if any(filename.endswith(ext) for ext in exclude_file_types):
				continue
			file_path = os.path.join(root, filename)
			# TODO skip symlinks
			relative_path = os.path.relpath(file_path, dir)
			files.add(relative_path)
	return files
'''

def file_sets(srcroot, dstroot, filenames, exclude_dirs, exclude_file_types):
	if filenames:
		srcroot_files = {f for f in os.scandir(srcroot) if f.is_file() and f.name in filenames}
		srcroot_files = {os.path.relpath(f.path, srcroot):f for f in srcroot_files}
		dstroot_files = {f for f in os.scandir(dstroot) if f.is_file() and f.name in filenames}
		dstroot_files = {os.path.relpath(f.path,dstroot):f for f in dstroot_files}
	else:
		srcroot_files = scan(srcroot, exclude_dirs, exclude_file_types)
		dstroot_files = scan(dstroot, exclude_dirs, exclude_file_types)
	new_files     = sorted(f for f in srcroot_files if f not in dstroot_files)
	extra_files   = sorted(f for f in dstroot_files if f not in srcroot_files)
	compare_files = sorted(set(srcroot_files.keys()) & set(dstroot_files.keys()))
	update_files  = []
	for f in compare_files:
		src = os.path.join(srcroot, f)
		dst = os.path.join(dstroot, f)
		src_time = srcroot_files[f].stat().st_mtime
		dst_time = dstroot_files[f].stat().st_mtime
		if src_time < dst_time:
			print(f"[WARN] {dst} is newer than {src}")
		elif src_time > dst_time:
			update_files.append(f)
	return (new_files, update_files, extra_files)

def backup(srcroot, dstroot, trashroot=None, filenames=[], exclude_dirs=[], exclude_file_types=[], dry_run=True):
	os.makedirs(dstroot, exist_ok=True)
	new_files, update_files, extra_files = file_sets(srcroot, dstroot, filenames, exclude_dirs, exclude_file_types)
	'''
	if whitelist:
		whitelist = [re.compile(pat) for pat in whitelist]
		new_files = [f for f in new_files if any(p.fullmatch(os.path.basename(f)) is not None for p in whitelist)]
		update_files = [f for f in update_files if any(p.fullmatch(os.path.basename(f)) is not None for p in whitelist)]
	if blacklist:
		blacklist = [re.compile(pat) for pat in blacklist]
		new_files = [f for f in new_files if all(p.fullmatch(os.path.basename(f)) is None for p in blacklist)]
		update_files = [f for f in update_files if all(p.fullmatch(os.path.basename(f)) is None for p in blacklist)]
	'''
	# Deleting must be done first or backing up a.jpg -> a.JPG (or similar) on Windows will fail.
	errors = []
	if trashroot:
		for f in extra_files:
			src = os.path.join(dstroot, f)
			dst = os.path.join(trashroot, f)
			print(f"- {src}")
			if not dry_run:
				success = move(src, dst)
				if not success:
					errors.append(f"[ERR] Failed to delete {src}")
	for f in update_files:
		src = os.path.join(srcroot, f)
		dst = os.path.join(dstroot, f)
		print(f"U {dst}")
		if not dry_run:
			success = copy(src, dst)
			if not success:
				errors.append(f"[ERR] Failed to update {dst}")
	for f in new_files:
		src = os.path.join(srcroot, f)
		dst = os.path.join(dstroot, f)
		print(f"+ {dst}")
		if not dry_run:
			success = copy(src, dst)
			if not success:
				errors.append(f"[ERR] Failed to create {dst}")
	for err in errors:
		print(err)


'''
import os, shutil

def backup(src, dst, trash=None, ignore=None, dry_run=True):
	warnings = []
	os.makedirs(dst, exist_ok=True)

	with os.scandir(src) as it:
		src_entries = list(it)
	src_files = []
	src_dirs  = []
	for srcentry in src_entries:
		if ignore is not None and ignore(srcentry.name):
			continue
		if srcentry.is_symlink():
			# skip symlinks
			warnings.append(f"[WARN] Skipping symlink: {srcpath}")
		elif srcentry.is_dir():
			src_dirs.append(srcentry.name)
		else:
			src_files.append(srcentry.name)

	with os.scandir(dst) as it:
		dst_entries = list(it)
	dst_files = []
	dst_dirs  = []
	for dstentry in dst_entries:
		if dstentry.is_dir():
			dst_dirs.append(dstentry.name)
		else:
			dst_files.append(dstentry.name)

	# recycle extras if trash is given
	if trash is not None:
		for d in dst_dirs:
			if d not in src_dirs:
				oldfile = os.path.join(dst, d)
				newfile = os.path.join(trash, d)
				print(f"  E {oldfile}")
				if not dry_run:
					try:
						os.makedirs(trash, exist_ok=True)
						shutil.move(oldfile, newfile)
					except:
						warnings.append(f"[WARN] Could not recycle: {oldfile}")
		for f in dst_files:
			if f not in src_files:
				oldfile = os.path.join(dst, f)
				newfile = os.path.join(trash, f)
				print(f"  E {oldfile}")
				if not dry_run:
					try:
						os.makedirs(trash, exist_ok=True)
						shutil.move(oldfile, newfile)
					except:
						warnings.append(f"[WARN] Could not recycle: {oldfile}")

	# copy newer files
	for f in src_files:
		srcpath = os.path.join(src, f)
		dstpath = os.path.join(dst, f)
		# TODO check dst_entries
		if os.path.exists(dstpath):
			srctime = os.path.getmtime(srcpath)
			dsttime = os.path.getmtime(dstpath)
			if srctime < dsttime:
				warnings.append(f"[WARN] dst newer than src: {dstpath}")
				continue
			elif srctime == dsttime:
				continue
			print(f" U  {dstpath}")
			if not dry_run:
				shutil.copy2(srcpath, dstpath+'.tempcopy')
				os.remove(dstpath)
				shutil.move(dstpath+'.tempcopy', dstpath)
		else:
			# copies metadata too
			print(f"N   {dstpath}")
			if not dry_run:
				shutil.copy2(srcpath, dstpath+'.tempcopy')
				shutil.move(dstpath+'.tempcopy', dstpath)
	for d in src_dirs:
		srcpath = os.path.join(src, d)
		dstpath = os.path.join(dst, d)
		trashpath = None if trash is None else os.path.join(trash, d)
		warn = update(srcpath, dstpath, trashpath, ignore, dry_run)
		warnings.extend(warn)

	return warnings
'''
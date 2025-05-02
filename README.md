# backup.py

## Features

- Can be used on the command line or as an imported module.
- Files that were renamed in the src folder will be renamed in the dst folder, without unnecessarily copying files.
- Won't delete files (by default) but will "recycle" files instead.
- Include/exclude files based on non-recursive glob patterns.
- Log the results to a log file.
- dry-run option to print would-be results without actually making changes to the file system.

## Examples

`python backup.py src dst`

&emsp; ↳ Recursively copies files inside `src` to `dst`, replacing files whose modtimes are newer in `src`.

`python backup.py src dst --trash-root trash`

&emsp; ↳ Same as above but will also "recycle" extra files (i.e., those that exist in `dst` but not `src`) into `trash`.

### Include/ Exclude

`backup.py` can include or exclude files and folders based on user-supplied non-recursive glob patterns. To make use of these options, keep in mind the following rules: By default, patterns apply to files and folder *names* with some exceptions. If a pattern has a slash (e.g., `foo/bar`, `baz/`), then it is considered a *relative path* pattern, not a *name* pattern. Also, a trailing slash (e.g., `baz/`) indicates the pattern is meant to only match folders. If a pattern ends in a 3-or-4-character extension (e.g., `index.html`, `cat.???`), then the pattern will only match files.

`python backup.py src dst --incldue foo`

&emsp; ↳ Copies any file or folder named `foo` inside `src` to `dst`.

`python backup.py src dst --incldue foo/`

&emsp; ↳ Copies the `foo` folder inside `src` to `dst`. Folder patterns are always treated as relative paths.

`python backup.py src dst --incldue *.txt`

&emsp; ↳ Copies any .txt file inside `src` to `dst`. The `.txt` extension indicates `\*.txt` is a file.

`python backup.py src dst --incldue ./*.txt`

&emsp; ↳ Non-recursively copies any .txt file inside `src` to `dst`.

`python backup.py src dst --incldue *.txt --exclude foo/bar/*`

&emsp; ↳ Copies any .txt file inside `src` to `dst`, unless the file is in the `foo/bar` folder.

`python backup.py src dst --incldue py* --exclude __pycache__ .pytest*`

&emsp; ↳ Copies any entry beginning with "py" inside `src` to `dst`, except for entries matching `__pycache__` or  `.pytest*`.
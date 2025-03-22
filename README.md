# psync.py

## Features

- Can be used on the command line or as an imported module.
- Files that were renamed in the src folder will be renamed in the dst folder, without unnecessarily copying files.
- Won't delete files (by default) but will "recycle" files instead by moving them to a different folder of your choosing.
- Include/exclude files based on recursive glob patterns.
- Log the results to a log file.
- `--dry-run` option to print would-be results without actually making changes to the file system.

## Examples

`python psync.py src dst`

&emsp; ↳ Recursively copies files inside `src/` to `dst/`, replacing files whose modtimes are newer in `src/`.

`python psync.py src dst -t trash`

&emsp; ↳ Same as above but will also "recycle" extra files (i.e., those that exist in `dst/` but not `src/`) into `trash/`.

### Include/ Exclude

`psync.py` can include or exclude files and folders based on user-supplied, recursive glob patterns. Also note that the recursive glob, `**`, is treated like the non-recurisve glob, `*`, if it is adjacent to any character other than "\\" or "/".

To make use of this option, keep in mind the following rules. This feature is accessed by supplying a filter string. The string contains various space-separated patterns, preceded by include ("+") and exclude ("-") indicators (e.g., `- skip.txt skip2.txt + **`). The patterns are matched against the relative paths of files and folders inside `src/` and `dst/`. If a pattern has a trailing slash (e.g., `foo/`), then the pattern will apply to folders only. Otherwise, the pattern will apply to files only. (The only exception is `**`, which applies to both files and folders.)

The indicator preceding the first matching pattern determines whether to include or exclude the filesystem entry. Included files will be copied in the backup, whereas included folders will be searched and their contents tested against the filter string. The default filter string is `+ **/*/ **/*`, which searches all folders and copies all files. If a user-defined filter is supplied, then any unmatched file or folder will be skipped (neither copied nor "recycled").

When a nested file is included, e.g., `+ foo/**/bar.txt`, then parent folders are also automatically included. This example is equiavlent to `+ foo/ foo/**/ foo/**/bar.txt`.

`python psync.py src dst -f '+ foo.txt'`

&emsp; ↳ Copies the `foo.txt` file inside `src/` to `dst/`. Skips all other files and does not recurse into subfolders.

`python psync.py src dst -f '+ **/*.txt'`

&emsp; ↳ Copies all .txt files inside `src/` to `dst/`.

`python psync.py src dst -f '- "skip these/" + **/*.txt'`

&emsp; ↳ Copies all .txt files inside `src/` to `dst/`, except those in the `src/skip these/` folder.

`python psync.py src dst -f '- **/__pycache__/ + **'`

&emsp; ↳ Copies all files inside `src/` to `dst/`, except for those inside folders named `__pycache__`.
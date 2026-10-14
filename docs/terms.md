# Terms

| Term | Meaning |
|---|---|
| root | A directory a capture (later: a tree) starts from. Paths are always opened below a root. |
| visit | Read a directory's stat and CephFS xattrs. The cost doesn't depend on the subtree's size. |
| list | Read a directory's entries (readdir) and visit its subdirectories. The cost grows with the number of entries, files included. |
| rstats | CephFS recursive statistics (`ceph.dir.rbytes`, `rentries`, `rfiles`, `rsubdirs`, `rctime`), kept by the MDS for a whole subtree and propagated lazily: a parent may briefly disagree with its children. `rentries` and `rsubdirs` count the directory itself (an empty directory has 1 of each). `rbytes` is the sum of file sizes, not pool usage. |
| rctime | The newest ctime or mtime in a subtree. It never decreases. |
| pinned rctime | An rctime more than `FUTURE_SLACK_S` after the observation time. Set by an entry with a future mtime (`touch -d`, archives unpacked with bogus dates) or ctime; it stays after that entry is fixed or removed. |
| chain | The pinned directories from a root down to where the entry that pins them is. |
| chain end | A pinned, listed directory without pinned subdirectories: *culprit* (it or one of its entries has a future mtime or ctime), *stale* (nothing visible is in the future), or *undetermined* (an unreadable or vanished subdirectory, or a truncated file scan). |
| capture | A JSON Lines file of raw filesystem state written by `dwusage capture`, for replay and diagnostics. |

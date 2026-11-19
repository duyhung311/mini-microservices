# Project rules: mini-microservices (Windows 11, Git Bash, PowerShell 5.1)

- Docker is not on Git Bash's PATH: `export PATH="/c/Users/ldhun/AppData/Local/Programs/DockerDesktop/resources/bin:$PATH"`.
- Compose Postgres is on host port 5433; a native Windows PostgreSQL owns 5432.
- Never call `python`/`python3`: the Windows Store stub hangs on stdin. Use bash, grep, sed, awk, curl.
- Never `export MSYS_NO_PATHCONV=1` in a script; set it only on the `docker compose exec` command that
  needs it. Exported, it breaks `mvnw` (classpath) and `curl -o /dev/null`.
- In Git Bash: `grep` needs `-U` to see CR; `$'\r'` inside `$( )` expands to nothing, so put it in a
  variable first (`cr=$'\r'`).
- In verify.ps1 (PS 5.1, ErrorActionPreference=Stop): run docker and other native tools through
  `cmd /c "… 2>&1"`; match paths with `[\\/]`.
- The VS Code Java language server rewrites `target/` after edits. Run Maven with `clean` after
  changing sources, and do not edit sources while Maven runs.
- Run `bash scripts/verify.sh --full` and `scripts\verify.ps1 -Full` before claiming done.

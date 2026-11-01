import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { mkdirSync, mkdtempSync, realpathSync, symlinkSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import test from "node:test"

const { default: plugin } = await import("../.kilo/plugin/specialist-commands.ts")
const { approved, tokenize } = await import("../.kilo/lib/specialist-commands.ts")

const project = realpathSync(mkdtempSync(join(tmpdir(), "specialist-commands-")))
mkdirSync(join(project, ".scratch", "node-smith", "ex"), { recursive: true })
mkdirSync(join(project, ".scratch", "designer"), { recursive: true })
mkdirSync(join(project, "src"), { recursive: true })
mkdirSync(join(project, "front"), { recursive: true })
writeFileSync(join(project, "clip.mp4"), "")
symlinkSync("/etc", join(project, "out"))
symlinkSync("/etc", join(project, "linkout"))
symlinkSync("/etc", join(project, "etc"))
mkdirSync(join(project, "sub"))
symlinkSync("/etc", join(project, "sub", "esc"))
mkdirSync(join(project, "a", "b", "c"), { recursive: true })
symlinkSync("a/b/c", join(project, "link"))
writeFileSync(join(project, "src", "a.txt"), "")

// `agent`: null for the main session, else the subagent's agent name.
// `want`: "allow" (goes on and is noted as approved), "deny" (the hook throws),
// "none" (goes on, left to Kilo's usual permissions).
type Case = [agent: string | null, command: string, want: "allow" | "deny" | "none", tool?: string, extra?: any]

const cases: Case[] = [
  // The cases tangle/claude-code's hook passes, with Kilo's agent names.
  [null, "rm -rf /", "none"],
  ["general", "rm -rf /", "none"],
  ["run-digger", "weft events abc --node x --full", "allow"],
  ["run-digger", "weft rm --yes", "deny"],
  ["run-digger", "weft events abc --on prod", "deny"],
  ["run-digger", "weft --on prod events abc", "deny"],
  ["run-digger", "weft events abc --json > out.json", "deny"],
  ["run-digger", "weft events abc 2>&1", "deny"],
  ["run-digger", "weft events abc | jq .", "deny"],
  ["run-digger", "weft events $(cat x)", "deny"],
  ["run-digger", "timeout 15 weft status --json", "allow"],
  ["run-digger", "weft daemon restart", "deny"],
  ["run-digger", "weft daemon logs --tail 200", "allow"],
  ["group-builder", "weft validate --file src/main.weft < src/main.weft", "allow"],
  ["group-builder", "weft validate --file src/main.weft < /etc/passwd", "deny"],
  ["group-builder", "weft run --group 'triage={\"text\":\"a && b; c\"}'", "allow"],
  ["group-builder", "weft run --group 'x={}' && rm -rf src", "deny"],
  ["node-smith", "weft test-node Gmail", "allow"],
  ["node-smith", "weft test-node Gmail --tier live", "deny"],
  ["node-smith", "weft test-node --tier=live", "deny"],
  ["node-smith", "weft test-node --yes", "deny"],
  ["node-smith", "weft infra terminate --yes", "allow"],
  ["node-smith", "cd ./.scratch/node-smith/ex && weft infra start", "allow"],
  ["node-smith", "cd /tmp && weft infra start", "deny"],
  ["node-smith", "cd ./../.. && ls", "deny"],
  // The prompt-engineer's trial is Kilo's own script here, never `claude -p`.
  ["prompt-engineer", "python3 .kilo/scripts/try_prompt.py --model kilo/~anthropic/claude-sonnet-latest --prompt assets/prompts/x.md --input .scratch/prompt-engineer/t/input-1.txt", "allow"],
  ["prompt-engineer", 'claude -p --model sonnet "hi"', "deny"],
  ["prompt-engineer", "python3 .kilo/scripts/try_prompt.py --models < /tmp/x", "deny"],
  ["frontend-builder", "pnpm dlx sv@0.17.0 create front --template minimal --types ts --no-add-ons --no-install", "allow"],
  ["frontend-builder", "pnpm dlx evil", "deny"],
  ["frontend-builder", "pnpm exec rm -rf /", "deny"],
  ["frontend-builder", "cd ./front && pnpm add clsx tailwind-merge && pnpm add -D tw-animate-css shadcn-svelte@1.6.1 && pnpm install", "allow"],
  // No `kill` on Kilo: the dev server stops with background_process's own stop.
  ["frontend-builder", "kill 4242", "deny"],
  ["frontend-builder", "kill -9 -1", "deny"],
  ["frontend-builder", "kill 1", "deny"],
  ["frontend-builder", "openssl rand -base64 32", "allow"],
  ["frontend-builder", "weft infra env db --into front/.env --set A=B", "allow"],
  ["frontend-builder", "weft token mint", "deny"],
  ["designer", "pnpm run dev --port 5174 --strictPort", "allow"],
  ["designer", "node x.mjs", "deny"],
  ["deployer", "weft activate --on prod", "allow"],
  ["deployer", "timeout 360 weft resync --on prod --all", "allow"],
  ["deployer", "weft target export prod --github", "deny"],
  ["deployer", "weft login prod", "deny"],
  ["deployer", "weft daemon restart", "deny"],
  ["deployer", "weft branch abc --on prod", "allow"],
  ["deployer", "weft workers set --min-instances 1 --on prod", "deny"],
  ["deployer", "weft workers --on prod", "allow"],
  ["deployer", "curl https://x", "deny"],
  ["deployer", "weft stop abc --on prod", "allow"],
  ["deployer", "weft run --on prod --target schema --detach", "allow"],
  ["deployer", "weft wait abc --on prod", "allow"],
  ["deployer", "weft logs abc --on prod", "allow"],
  ["deployer", "weft events abc --on prod", "allow"],
  ["red-teamer", "weft describe-nodes --node X --compact", "allow"],
  ["red-teamer", "weft run", "deny"],
  ["frontend-builder", "weft run 'x", "deny"],
  // Kilo's own commands and tools.
  ["designer", "pnpm run dev --port 5174 --strictPort", "allow", "background_process", { action: "start" }],
  ["designer", "rm -rf design-lab", "deny", "background_process", { action: "start" }],
  ["frontend-builder", "pnpm run build", "allow", "background_process", { action: "monitor" }],
  ["designer", "", "none", "background_process", { action: "stop", id: "bp_1" }],
  ["run-digger", "python3 .kilo/scripts/specialist_quiet.py ses_abc", "deny"],
  ["run-digger", "kilo export ses_abc", "deny"],
  ["run-digger", 'kilo db "SELECT 1"', "deny"],
  ["prompt-engineer", "python3 .kilo/scripts/try_prompt.py --models", "allow"],
  ["prompt-engineer", "python3 .kilo/scripts/specialist_quiet.py ses_abc", "deny"],
  ["node-smith", "weft test-node Gmail", "allow", "bash", { workdir: ".scratch/node-smith/ex" }],
  ["node-smith", "cd ./ex && weft infra start", "allow", "bash", { workdir: ".scratch/node-smith" }],
  ["node-smith", "cd ./.. && weft infra start", "deny", "bash", { workdir: ".scratch/node-smith/ex" }],
  ["node-smith", "weft infra start", "none", "read"],
  // The read-only tools, each with its own check.
  ["group-builder", "weft validate --file src/main.weft < $HOME/.ssh/id_rsa", "deny"],
  ["deployer", "weft daemon logs --tail 50", "allow"],
  // Stopping a run: whoever starts runs.
  ["group-builder", "weft stop abc", "allow"],
  ["prompt-engineer", "weft stop abc", "allow"],
  ["frontend-builder", "weft stop abc", "allow"],
  ["node-smith", "weft stop abc --on prod", "deny"],
  ["red-teamer", "weft stop abc", "deny"],
  // grep and rg, inside the project only.
  ["red-teamer", "grep -rn 'fn main' src", "allow"],
  ["red-teamer", "grep -rn \"route$\" src", "allow"],
  ["red-teamer", "grep -rn /api/v1 src", "allow"],
  ["red-teamer", "grep -c x src/main.weft && rg -n x src", "allow"],
  ["red-teamer", "grep root /etc/passwd", "deny"],
  ["red-teamer", "grep -r key ..", "deny"],
  ["red-teamer", "grep -r key ~/.ssh", "deny"],
  ["red-teamer", "grep -r key /etc/*", "deny"],
  ["red-teamer", "grep -f /etc/passwd src", "deny"],
  ["red-teamer", "grep --file=/etc/passwd src", "deny"],
  ["red-teamer", "grep -r key {/etc,src}", "deny"],
  ["red-teamer", "grep -r key out", "deny"],
  ["red-teamer", "grep -r key $HOME", "deny"],
  ["run-digger", "rg -n 'TODO' src -g '*.weft'", "allow"],
  ["run-digger", "rg --pre cat x src", "deny"],
  ["run-digger", "rg --pre=cat x src", "deny"],
  ["run-digger", "rg -z x src", "deny"],
  ["run-digger", "rg -iz x src", "deny"],
  ["run-digger", "rg -ez src", "allow"],
  ["run-digger", "rg --search-zip x src", "deny"],
  ["run-digger", "rg --hostname-bin=/bin/sh x src", "deny"],
  ["deployer", "rg -n target weft.toml", "allow"],
  // ffprobe: one file inside the project.
  ["run-digger", "ffprobe -v error -show_format -show_streams -of json -f mp4 -i clip.mp4", "allow"],
  ["run-digger", "ffprobe -hide_banner -f mp4 -i clip.mp4", "allow"],
  ["run-digger", "ffprobe -f mp4 -i /etc/passwd", "deny"],
  ["run-digger", "ffprobe -f lavfi -i 'movie=/etc/passwd'", "deny"],
  ["run-digger", "ffprobe -o .scratch/x.json -f mp4 -i clip.mp4", "deny"],
  ["run-digger", "ffprobe -f mp4 -i file:clip.mp4", "deny"],
  ["run-digger", "ffprobe -report -f mp4 -i clip.mp4", "deny"],
  ["red-teamer", "ffprobe clip.mp4", "deny"],
  // ffmpeg: reads inside the project, writes under .scratch/<agent>/.
  ["designer", "ffmpeg -y -f mp4 -i clip.mp4 -vf 'fps=10,scale=640:-1' .scratch/designer/frames-%03d.png", "allow"],
  ["designer", "ffmpeg -ss 2 -f mp4 -i clip.mp4 -frames:v 1 .scratch/designer/frame.png", "allow"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -vf \"split[a][b];[a]palettegen[p];[b][p]paletteuse\" .scratch/designer/x.gif", "allow"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -vf \"select='eq(n,0)'\" .scratch/designer/x.png", "allow"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -vf 'select=eq(n\\,0)' .scratch/designer/x.png", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -vf \"scale='1\\',movie=/etc/passwd'\" .scratch/designer/x.png", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -af 'volume=1\\,amovie=/etc/passwd' .scratch/designer/x.wav", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -af loudnorm=print_format=json -f null -", "allow"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 out.png", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 src/main.weft", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 /tmp/x.png", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 .scratch/designer/../../src/main.weft", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 .scratch/frontend-builder/x.png", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -an src/x.wav .scratch/designer/ok.mp4", "deny"],
  ["designer", "ffmpeg -f mp4 -i /etc/passwd .scratch/designer/x.mp4", "deny"],
  ["designer", "ffmpeg -f lavfi -i testsrc .scratch/designer/x.mp4", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -f tee '.scratch/designer/a.mp4|/tmp/b.mp4'", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -vf movie=/etc/passwd .scratch/designer/x.mp4", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -vf 'scale=1:1,metadata=print:file=/tmp/x' .scratch/designer/x.mp4", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 -filter_complex null .scratch/designer/x.mp4", "deny"],
  ["designer", "ffmpeg -report -f mp4 -i clip.mp4 .scratch/designer/x.mp4", "deny"],
  ["designer", "ffmpeg -f mp4 -i clip.mp4 tcp://example.com:1", "deny"],
  ["run-digger", "ffmpeg -f mp4 -i clip.mp4 .scratch/run-digger/x.png", "deny"],
  // psql: one read, inside a read-only transaction the database holds it to.
  ["run-digger", "psql postgresql://app@127.0.0.1:5432/app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT id, status FROM orders LIMIT 5'", "allow"],
  ["run-digger", "psql -h 127.0.0.1 -p 5432 -U app -d app -At -c 'begin transaction read only;' -c 'select count(*) from orders;'", "allow"],
  ["run-digger", "psql --host=127.0.0.1 --dbname=app --csv --command='BEGIN  TRANSACTION READ ONLY' --command='SHOW server_version'", "allow"],
  ["run-digger", "psql -X app -c 'BEGIN TRANSACTION READ ONLY' -c 'EXPLAIN SELECT * FROM orders'", "allow"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1; DROP TABLE orders'", "deny"],
  ["run-digger", "psql app -c 'SELECT 1'; psql app -c 'DROP TABLE orders'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'DELETE FROM orders'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c '\\! rm -rf /'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1 \\g /tmp/x'", "deny"],
  ["run-digger", "psql app -f drop.sql", "deny"],
  ["run-digger", "psql app -o /tmp/out -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1'", "deny"],
  ["run-digger", "psql app -L /tmp/log -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT * INTO copy FROM orders'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'EXPLAIN ANALYZE DELETE FROM orders'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'EXPLAIN (ANALYZE) DELETE FROM orders'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'WITH d AS (DELETE FROM orders RETURNING *) SELECT 1'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1' -c 'SELECT 2'", "deny"],
  // a read with no read-only transaction around it
  ["run-digger", "psql app -c 'SELECT 1'", "deny"],
  // the transaction with no statement in it
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY'", "deny"],
  // a transaction that may write
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ WRITE' -c 'SELECT 1'", "deny"],
  // a plain transaction
  ["run-digger", "psql app -c 'BEGIN' -c 'SELECT 1'", "deny"],
  // the statement before the transaction
  ["run-digger", "psql app -c 'SELECT 1' -c 'BEGIN TRANSACTION READ ONLY'", "deny"],
  // the transaction closed in its own -c
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY; COMMIT' -c 'DELETE FROM orders'", "deny"],
  ["run-digger", "psql app", "deny"],
  ["red-teamer", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c \"SELECT '{1,2}'::int[], '#x'\"", "allow"],
  ["run-digger", "psql 'postgresql://h/db?sslkey%6cogfile=/tmp/k' -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1'", "deny"],
  ["run-digger", "psql 'postgresql://h/db?x=%zz' -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1'", "deny"],
  ["run-digger", "psql 'service=x' -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1'", "deny"],
  ["run-digger", "psql 'postgresql://h/db?servicefile=/tmp/s' -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1'", "deny"],
  ["run-digger", "psql -d 'host=h options=-cwork_mem=1' -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1'", "deny"],
  ["run-digger", "psql service_db -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT 1'", "allow"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT dblink(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT dblink_exec(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_terminate_backend(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_cancel_backend(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_reload_conf()'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_rotate_logfile()'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT lo_export(1, 1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT lo_import(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_read_file(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_read_binary_file(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_ls_dir(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_stat_file(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_advisory_lock(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_advisory_xact_lock(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_try_advisory_lock(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_notify(1, 1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_switch_wal()'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_create_restore_point(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_logical_emit_message(true, 1, 1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT set_config(1, 1, false)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT query_to_xml(1, true, false, 1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT PG_CATALOG.PG_TERMINATE_BACKEND(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT \"pg_terminate_backend\"(1)'", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c 'SELECT pg_catalog . pg_read_file(1)'", "deny"],
  ["run-digger", "weft status <(touch /tmp/pwn)", "deny"],
  ["run-digger", "weft status >(touch /tmp/pwn)", "deny"],
  ["run-digger", "weft status <> /tmp/newfile", "deny"],
  ["run-digger", "weft status <<< x", "deny"],
  ["run-digger", "rg x {--pre=sh,} a.sh", "deny"],
  ["node-smith", "weft test-node {--yes,}", "deny"],
  ["run-digger", "psql db -c \"BEGIN TRANSACTION READ ONLY\" -c \"SELECT 1\" {-f.scratch/x.sql,}", "deny"],
  ["red-teamer", "grep -r x .{.,x}", "deny"],
  ["red-teamer", "grep root $'/etc/passwd'", "deny"],
  ["node-smith", "ffmpeg -i README.md -y .scratch/node-smith/$'..'/$'..'/x.mp4", "deny"],
  ["node-smith", "cd $'/tmp'", "deny"],
  ["node-smith", "ffmpeg -i $'file\\x3a/etc/passwd' .scratch/node-smith/x.mp4", "deny"],
  ["red-teamer", "grep root $\"/etc/passwd\"", "deny"],
  ["red-teamer", "grep x \"$HOME\"", "deny"],
  ["red-teamer", "grep x \"\\$HOME\"", "deny"],
  ["red-teamer", "grep x src $1", "deny"],
  ["red-teamer", "grep x src $$", "deny"],
  ["red-teamer", "grep x src $0", "deny"],
  ["red-teamer", "grep x src $@", "deny"],
  ["red-teamer", "grep x src $?", "deny"],
  ["red-teamer", "grep x src $*", "deny"],
  ["red-teamer", "grep x src $#", "deny"],
  ["red-teamer", "grep x src $!", "deny"],
  ["red-teamer", "grep x src $-", "deny"],
  ["red-teamer", "grep x src $_", "deny"],
  ["red-teamer", "grep -rn 'route$' src", "allow"],
  ["red-teamer", "grep -rn x$ src", "allow"],
  ["red-teamer", "grep -rn \\$HOME src", "allow"],
  ["run-digger", "weft status x#; rm -rf y", "deny"],
  ["run-digger", "weft status # x", "deny"],
  ["red-teamer", "grep -rn '#x' src", "allow"],
  ["red-teamer", "grep -rn \"a#b {c}\" src", "allow"],
  ["red-teamer", "grep -rf/etc/passwd x .", "deny"],
  ["red-teamer", "grep -of/etc/passwd src/main.weft", "deny"],
  ["red-teamer", "rg -uf/etc/passwd x src", "deny"],
  ["red-teamer", "grep -rn pattern src", "allow"],
  ["red-teamer", "rg -uu pattern", "allow"],
  ["red-teamer", "grep -R key src", "deny"],
  ["red-teamer", "grep -nR key src", "deny"],
  ["red-teamer", "grep --dereference-recursive key src", "deny"],
  ["red-teamer", "grep --deref key src", "deny"],
  ["red-teamer", "grep -eR src", "allow"],
  ["red-teamer", "rg -L key src", "deny"],
  ["red-teamer", "rg -uL key src", "deny"],
  ["red-teamer", "rg --follow key src", "deny"],
  // what zsh acts on: glob qualifiers, =(...), =cmd; and a dot glob reaching ..
  ["red-teamer", "grep -r x *(e:'touch P3':)", "deny"],
  ["run-digger", "weft status *(e:'touch P1':)", "deny"],
  ["run-digger", "weft status =(touch P2)", "deny"],
  ["red-teamer", "grep x =ls", "deny"],
  ["red-teamer", "grep x src =ls", "deny"],
  ["red-teamer", "grep -r x .*", "deny"],
  ["red-teamer", "grep -r x src/.*", "deny"],
  ["red-teamer", "grep -r x '(a)' --include=a=b src", "allow"],
  // a glob reaching out through a link inside the project, and zsh's `**`
  ["red-teamer", "grep -r root lin*/passwd", "deny"],
  ["red-teamer", "grep root < linkou*/passwd", "deny"],
  ["red-teamer", "grep -r root linkou*", "deny"],
  ["red-teamer", "rg root linko?t/passwd", "deny"],
  ["run-digger", "ffprobe -f mp4 -i linkou*/passwd", "deny"],
  ["red-teamer", "grep -r x **/passwd", "deny"],
  ["red-teamer", "grep -r x src/*.txt", "allow"],
  ["red-teamer", "grep -r x src/*.none", "allow"],
  // what the shell reads differently from this check: an unquoted `[`, a `cd` earlier in the line, a `.` glob that bash matches to `..`
  ["red-teamer", "grep x [[:alpha:]]tc/hostname", "deny"],
  ["red-teamer", "grep x [^x]tc/hostname", "deny"],
  ["node-smith", "cd ./sub && grep x es?/hostname", "deny"],
  ["node-smith", "cd ./sub && grep x esc/hostname", "deny"],
  ["node-smith", "cd ./src; grep x main.weft", "deny"],
  ["node-smith", "cd ./sub & grep x esc/hostname", "deny"],
  ["node-smith", "cd ./sub || grep x esc/hostname", "deny"],
  ["node-smith", "cd -", "deny"],
  ["node-smith", "cd", "deny"],
  ["node-smith", "pushd src", "deny"],
  ["node-smith", "popd", "deny"],
  ["node-smith", "cd ./sr*", "deny"],
  ["node-smith", "cd ./src/main.weft", "deny"],
  ["node-smith", "cd ./.?", "deny"],
  ["run-digger", "ffprobe -f mp4 -i .?/file", "deny"],
  ["red-teamer", "grep x < .?/x", "deny"],
  ["red-teamer", "grep x src/.h*", "deny"],
  ["red-teamer", "grep -rn '[a-z]+' src", "allow"],
  // a `cd` written without `./` (which would follow CDPATH), and an input whose format is not named right before it
  ["node-smith", "cd src && grep x main.weft", "deny"],
  ["node-smith", "cd ./src/../src && grep x main.weft", "deny"],
  ["run-digger", "ffprobe clip.mp4", "deny"],
  ["run-digger", "ffprobe -i clip.mp4", "deny"],
  ["run-digger", "ffprobe -f hls -i clip.mp4", "deny"],
  ["designer", "ffmpeg -i clip.mp4 .scratch/designer/x.png", "deny"],
  ["designer", "ffmpeg -f mp4 -ss 2 -i clip.mp4 .scratch/designer/x.png", "deny"],
  ["designer", "ffmpeg -f concat -i clip.mp4 .scratch/designer/x.png", "deny"],
  ["run-digger", "ffprobe -v error -f mp4 -i clip.mp4", "allow"],
  ["designer", "ffmpeg -ss 2 -f mp4 -i clip.mp4 .scratch/designer/x.png", "allow"],
  // a `cd` whose folder holds a link or `..`, where the shell and this check would disagree on where it is; and an ffmpeg output read from the folder a `cd` went to
  ["node-smith", "cd ./link && cd ./.. && grep -r SECRET ../..", "deny"],
  ["node-smith", "cd ./link/.. && grep -r SECRET ../..", "deny"],
  ["node-smith", "cd ./link", "deny"],
  ["node-smith", "cd ./a/b/../b", "deny"],
  ["node-smith", "cd ./sub && cd ./.. && grep x src", "deny"],
  ["node-smith", "cd ./src && ffmpeg -f mp4 -i ../clip.mp4 x.png", "deny"],
  ["node-smith", "cd ./a/b && grep x c", "allow"],
  ["node-smith", "cd ./.scratch && ffmpeg -f mp4 -i ../clip.mp4 node-smith/x.png", "allow"],
  ["node-smith", "cd ./src && grep x main.weft", "allow"],
  // a function name spelled with Unicode escapes
  ["run-digger", "psql -h localhost -d app -c \"BEGIN TRANSACTION READ ONLY\" -c \"SELECT U&\\\"pg!0074erminate_backend\\\" UESCAPE '!'(pid) FROM pg_stat_activity\"", "deny"],
  ["run-digger", "psql app -c 'BEGIN TRANSACTION READ ONLY' -c \"SELECT u&'\\\\0041'\"", "deny"],
]

const clientFor = (agent: string | null) => ({
  session: {
    get: async () => ({ data: agent === null ? { id: "ses_main" } : { id: "ses_child", parentID: "ses_main", agent } }),
  },
})

let n = 0
for (const [agent, command, want, tool = "bash", extra = {}] of cases) {
  const index = ++n
  test(`${index}. ${agent ?? "main session"} via ${tool}: ${command.slice(0, 70)} -> ${want}`, async () => {
    const hooks = await plugin({ client: clientFor(agent), directory: project })
    const callID = `call_${index}`
    let thrown: Error | undefined
    try {
      await hooks["tool.execute.before"]({ tool, sessionID: "s", callID }, { args: { command, ...extra } })
    } catch (error) {
      thrown = error as Error
    }
    const got = thrown ? "deny" : approved.has(callID) ? "allow" : "none"
    approved.delete(callID)
    assert.equal(got, want, thrown?.message)
    if (thrown) assert.match(thrown.message, /you may run only|refused/)
  })
}

test("a refusal names the specialist's list", async () => {
  const hooks = await plugin({ client: clientFor("red-teamer"), directory: project })
  await assert.rejects(
    hooks["tool.execute.before"]({ tool: "bash", sessionID: "s", callID: "c" }, { args: { command: "weft run" } }),
    /`weft run` is not on your list\. As red-teamer you may run only: weft describe-nodes, weft validate, weft parse, weft status, weft daemon logs, grep, rg\. Do it another way, or report BLOCKED with the exact command\./,
  )
})

test("a session that cannot be read refuses the command", async () => {
  const client = { session: { get: async () => ({ error: { name: "NotFoundError" } }) } }
  const hooks = await plugin({ client, directory: project })
  await assert.rejects(
    hooks["tool.execute.before"]({ tool: "bash", sessionID: "s", callID: "c" }, { args: { command: "weft status" } }),
    /could not tell which agent is asking/,
  )
})

test("a subagent session that names no agent refuses the command", async () => {
  const client = { session: { get: async () => ({ data: { id: "ses_child", parentID: "ses_main" } }) } }
  const hooks = await plugin({ client, directory: project })
  await assert.rejects(
    hooks["tool.execute.before"]({ tool: "bash", sessionID: "s", callID: "c" }, { args: { command: "weft status" } }),
    /names no agent/,
  )
})

test("tool.execute.after forgets the call", async () => {
  const hooks = await plugin({ client: clientFor("run-digger"), directory: project })
  await hooks["tool.execute.before"]({ tool: "bash", sessionID: "s", callID: "c_after" }, { args: { command: "weft status" } })
  assert.ok(approved.has("c_after"))
  await hooks["tool.execute.after"]({ callID: "c_after" })
  assert.ok(!approved.has("c_after"))
})

// The tokenizer must read a line exactly as Python's shlex does in the
// claude-code hook, so both folders split the same line the same way.
test("the tokenizer agrees with Python's shlex", () => {
  const lines = [
    "weft run --group 'triage={\"text\":\"a && b; c\"}'",
    'echo "a \\"b\\" c" d\\ e',
    "a&&b||c;d|e&f |& g",
    "weft events abc 2>&1",
    "x >> y < z",
    "a#b c",
    "# all comment",
    "a 'b''c' \"\" ''",
    "pnpm dlx sv@0.17.0 create front",
    'say "it\'s" \'"q"\'',
    "back\\\\slash \"in \\\\ quote\" 'single \\\\ kept'",
    "(sub) {x} a=b ~/p ./q *.ts ?x",
    "tab\tsep  two   spaces",
    'mixed"quo"ted\'parts\'',
    "cd .scratch/node-smith/ex && weft infra start",
    "trailing &",
    "\"unicode é ñ\" ß",
  ]
  const script =
    "import json,shlex,sys\n" +
    "out=[]\n" +
    "for line in json.load(sys.stdin):\n" +
    "    lx=shlex.shlex(line,posix=True,punctuation_chars=True); lx.whitespace_split=True\n" +
    "    out.append(list(lx))\n" +
    "print(json.dumps(out))\n"
  const done = spawnSync("python3", ["-c", script], { input: JSON.stringify(lines), encoding: "utf8" })
  assert.equal(done.status, 0, done.stderr)
  const python = JSON.parse(done.stdout)
  lines.forEach((line, i) => assert.deepEqual(tokenize(line), python[i], line))
  for (const broken of ["weft run 'x", 'a "b', "a\\"]) {
    assert.throws(() => tokenize(broken))
    const check = spawnSync(
      "python3",
      ["-c", "import shlex,sys\nlx=shlex.shlex(sys.argv[1],posix=True,punctuation_chars=True); lx.whitespace_split=True\nlist(lx)", broken],
      { encoding: "utf8" },
    )
    assert.notEqual(check.status, 0, broken)
  }
})

"""The cases of .codex/hooks/specialist_commands.py, against the real lists.

Run with `python3 tangle/codex/tests/test_specialist_commands.py`
(standard library only). Each case is an agent from the lists, a command,
and whether the hook allows it.
"""

import importlib.util
import json
import os
import tempfile
import unittest

HOOKS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".codex", "hooks")
spec = importlib.util.spec_from_file_location("specialist_commands", os.path.join(HOOKS, "specialist_commands.py"))
hook = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hook)
with open(os.path.join(HOOKS, "specialist_commands.json"), encoding="utf-8") as handle:
    LISTS = json.load(handle)

CASES = [
    ("run-digger", "weft events abc --node x --full", True),
    ("run-digger", "weft rm --yes", False),
    ("run-digger", "weft events abc --on prod", False),
    ("run-digger", "weft events abc --json > out.json", False),
    ("run-digger", "weft events abc | jq .", False),
    ("run-digger", "weft events $(cat x)", False),
    ("run-digger", "timeout 15 weft status --json", True),
    ("group-builder", "weft validate --file src/main.weft < src/main.weft", True),
    ("group-builder", "weft validate --file src/main.weft < /etc/passwd", False),
    ("group-builder", "weft validate --file src/main.weft < $HOME/.ssh/id_rsa", False),
    ("group-builder", "weft run --group 'triage={\"text\":\"a && b; c\"}'", True),
    ("node-smith", "weft test-node Gmail --tier live", False),
    ("node-smith", "cd ./.scratch/node-smith && weft infra start", False),
    ("node-smith", "cd /tmp && weft infra start", False),
    ("frontend-builder", "python3 .codex/scripts/stop_server.py --port 5174", True),
    ("frontend-builder", "kill -9 -1", False),
    ("deployer", "weft activate --on prod", True),
    ("deployer", "weft daemon logs --tail 50", True),
    # Stopping a run: whoever starts runs.
    ("group-builder", "weft stop abc", True),
    ("prompt-engineer", "weft stop abc", True),
    ("frontend-builder", "weft stop abc", True),
    ("node-smith", "weft stop abc --on prod", False),
    ("red-teamer", "weft stop abc", False),
    # grep and rg, inside the project only.
    ("red-teamer", "grep -rn 'fn main' src", True),
    ("red-teamer", "grep -rn \"route$\" src", True),
    ("red-teamer", "grep -rn /api/v1 src", True),
    ("red-teamer", "grep -c x src/main.weft && rg -n x src", True),
    ("red-teamer", "grep root /etc/passwd", False),
    ("red-teamer", "grep -r key ..", False),
    ("red-teamer", "grep -r key ~/.ssh", False),
    ("red-teamer", "grep -r key /etc/*", False),
    ("red-teamer", "grep -f /etc/passwd src", False),
    ("red-teamer", "grep --file=/etc/passwd src", False),
    ("red-teamer", "grep -r key {/etc,src}", False),
    ("red-teamer", "grep -r key out", False),
    ("red-teamer", "grep -r key $HOME", False),
    ("run-digger", "rg -n 'TODO' src -g '*.weft'", True),
    ("run-digger", "rg --pre cat x src", False),
    ("run-digger", "rg --pre=cat x src", False),
    ("run-digger", "rg -z x src", False),
    ("run-digger", "rg -iz x src", False),
    ("run-digger", "rg -ez src", True),
    ("run-digger", "rg --search-zip x src", False),
    ("run-digger", "rg --hostname-bin=/bin/sh x src", False),
    ("deployer", "rg -n target weft.toml", True),
    # ffprobe: one file inside the project.
    ("run-digger", "ffprobe -v error -show_format -show_streams -of json -f mp4 -i clip.mp4", True),
    ("run-digger", "ffprobe -hide_banner -f mp4 -i clip.mp4", True),
    ("run-digger", "ffprobe -f mp4 -i /etc/passwd", False),
    ("run-digger", "ffprobe -f lavfi -i 'movie=/etc/passwd'", False),
    ("run-digger", "ffprobe -o .scratch/x.json -f mp4 -i clip.mp4", False),
    ("run-digger", "ffprobe -f mp4 -i file:clip.mp4", False),
    ("run-digger", "ffprobe -report -f mp4 -i clip.mp4", False),
    ("red-teamer", "ffprobe clip.mp4", False),
    # ffmpeg: reads inside the project, writes under .scratch/<agent>/.
    ("designer", "ffmpeg -y -f mp4 -i clip.mp4 -vf 'fps=10,scale=640:-1' .scratch/designer/frames-%03d.png", True),
    ("designer", "ffmpeg -ss 2 -f mp4 -i clip.mp4 -frames:v 1 .scratch/designer/frame.png", True),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -vf \"split[a][b];[a]palettegen[p];[b][p]paletteuse\" .scratch/designer/x.gif", True),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -vf \"select='eq(n,0)'\" .scratch/designer/x.png", True),
    # a backslash in a filter, which ffmpeg and this check would split differently
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -vf 'select=eq(n\\,0)' .scratch/designer/x.png", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -vf \"scale='1\\',movie=/etc/passwd'\" .scratch/designer/x.png", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -af 'volume=1\\,amovie=/etc/passwd' .scratch/designer/x.wav", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -af loudnorm=print_format=json -f null -", True),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 out.png", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 src/main.weft", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 /tmp/x.png", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 .scratch/designer/../../src/main.weft", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 .scratch/frontend-builder/x.png", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -an src/x.wav .scratch/designer/ok.mp4", False),
    ("designer", "ffmpeg -f mp4 -i /etc/passwd .scratch/designer/x.mp4", False),
    ("designer", "ffmpeg -f lavfi -i testsrc .scratch/designer/x.mp4", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -f tee '.scratch/designer/a.mp4|/tmp/b.mp4'", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -vf movie=/etc/passwd .scratch/designer/x.mp4", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -vf 'scale=1:1,metadata=print:file=/tmp/x' .scratch/designer/x.mp4", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 -filter_complex null .scratch/designer/x.mp4", False),
    ("designer", "ffmpeg -report -f mp4 -i clip.mp4 .scratch/designer/x.mp4", False),
    ("designer", "ffmpeg -f mp4 -i clip.mp4 tcp://example.com:1", False),
    ("run-digger", "ffmpeg -f mp4 -i clip.mp4 .scratch/run-digger/x.png", False),
    # psql: on no list here, since the sandbox a specialist's command runs
    # in has no network, not even to 127.0.0.1.
    ("run-digger", "psql postgresql://app@127.0.0.1:5432/app -c 'SELECT id, status FROM orders LIMIT 5'", False),
    ("run-digger", "psql -h 127.0.0.1 -p 5432 -U app -d app -Atc 'select count(*) from orders;'", False),
    ("run-digger", "psql --host=127.0.0.1 --dbname=app --csv --command='SHOW server_version'", False),
    ("run-digger", "psql -X app -c 'EXPLAIN SELECT * FROM orders'", False),
    ("run-digger", "psql app -c 'SELECT 1; DROP TABLE orders'", False),
    ("run-digger", "psql app -c 'SELECT 1'; psql app -c 'DROP TABLE orders'", False),
    ("run-digger", "psql app -c 'DELETE FROM orders'", False),
    ("run-digger", "psql app -c '\\! rm -rf /'", False),
    ("run-digger", "psql app -c 'SELECT 1 \\g /tmp/x'", False),
    ("run-digger", "psql app -f drop.sql", False),
    ("run-digger", "psql app -o /tmp/out -c 'SELECT 1'", False),
    ("run-digger", "psql app -L /tmp/log -c 'SELECT 1'", False),
    ("run-digger", "psql app -c 'SELECT * INTO copy FROM orders'", False),
    ("run-digger", "psql app -c 'EXPLAIN ANALYZE DELETE FROM orders'", False),
    ("run-digger", "psql app -c 'EXPLAIN (ANALYZE) DELETE FROM orders'", False),
    ("run-digger", "psql app -c 'WITH d AS (DELETE FROM orders RETURNING *) SELECT 1'", False),
    ("run-digger", "psql app -c 'SELECT 1' -c 'SELECT 2'", False),
    ("run-digger", "psql app", False),
    ("red-teamer", "psql app -c 'SELECT 1'", False),
] + [
    # shell syntax the matcher does not read: process substitution and `<>`
    ("run-digger", "weft status <(touch /tmp/pwn)", False),
    ("run-digger", "weft status >(touch /tmp/pwn)", False),
    ("run-digger", "weft status <> /tmp/newfile", False),
    ("run-digger", "weft status <<< x", False),
    # brace expansion
    ("run-digger", "rg x {--pre=sh,} a.sh", False),
    ("node-smith", "weft test-node {--yes,}", False),
    ("red-teamer", "grep -r x .{.,x}", False),
    # ANSI-C and locale quoting, and special parameters
    ("red-teamer", "grep root $'/etc/passwd'", False),
    ("node-smith", "ffmpeg -i README.md -y .scratch/node-smith/$'..'/$'..'/x.mp4", False),
    ("node-smith", "cd $'/tmp'", False),
    ("node-smith", "ffmpeg -i $'file\\x3a/etc/passwd' .scratch/node-smith/x.mp4", False),
    ("red-teamer", "grep root $\"/etc/passwd\"", False),
    ("red-teamer", "grep x \"$HOME\"", False),
    ("red-teamer", "grep x \"\\$HOME\"", False),
] + [("red-teamer", f"grep x src ${p}", False) for p in ("1", "$", "0", "@", "?", "*", "#", "!", "-", "_")] + [
    ("red-teamer", "grep -rn 'route$' src", True),
    ("red-teamer", "grep -rn x$ src", True),
    ("red-teamer", "grep -rn \\$HOME src", True),
    # `#` inside a word: shlex reads a comment where bash reads a word
    ("run-digger", "weft status x#; rm -rf y", False),
    ("run-digger", "weft status # x", False),
    ("red-teamer", "grep -rn '#x' src", True),
    ("red-teamer", "grep -rn \"a#b {c}\" src", True),
    # a file glued to a cluster of short options
    ("red-teamer", "grep -rf/etc/passwd x .", False),
    ("red-teamer", "grep -of/etc/passwd src/main.weft", False),
    ("red-teamer", "rg -uf/etc/passwd x src", False),
    ("red-teamer", "grep -rn pattern src", True),
    ("red-teamer", "rg -uu pattern", True),
    # following links out of the project
    ("red-teamer", "grep -R key src", False),
    ("red-teamer", "grep -nR key src", False),
    ("red-teamer", "grep --dereference-recursive key src", False),
    ("red-teamer", "grep --deref key src", False),
    ("red-teamer", "grep -eR src", True),
    ("red-teamer", "rg -L key src", False),
    ("red-teamer", "rg -uL key src", False),
    ("red-teamer", "rg --follow key src", False),
    # what zsh acts on: glob qualifiers, =(...), =cmd; and a dot glob reaching ..
    ("red-teamer", "grep -r x *(e:'touch P3':)", False),
    ("run-digger", "weft status *(e:'touch P1':)", False),
    ("run-digger", "weft status =(touch P2)", False),
    ("red-teamer", "grep x =ls", False),
    ("red-teamer", "grep x src =ls", False),
    ("red-teamer", "grep -r x .*", False),
    ("red-teamer", "grep -r x src/.*", False),
    ("red-teamer", "grep -r x '(a)' --include=a=b src", True),
    # a glob reaching out through a link inside the project, and zsh's `**`
    ("red-teamer", "grep -r root lin*/passwd", False),
    ("red-teamer", "grep root < linkou*/passwd", False),
    ("red-teamer", "grep -r root linkou*", False),
    ("red-teamer", "rg root linko?t/passwd", False),
    ("run-digger", "ffprobe -f mp4 -i linkou*/passwd", False),
    ("red-teamer", "grep -r x **/passwd", False),
    ("red-teamer", "grep -r x src/*.txt", True),
    ("red-teamer", "grep -r x src/*.none", True),
    # what the shell reads differently from this check: an unquoted `[`, a `cd` earlier in the line, a `.` glob that bash matches to `..`
    ("red-teamer", "grep x [[:alpha:]]tc/hostname", False),
    ("red-teamer", "grep x [^x]tc/hostname", False),
    ("node-smith", "cd ./sub && grep x es?/hostname", False),
    ("node-smith", "cd ./sub && grep x esc/hostname", False),
    ("node-smith", "cd ./src; grep x main.weft", False),
    ("node-smith", "cd ./sub & grep x esc/hostname", False),
    ("node-smith", "cd ./sub || grep x esc/hostname", False),
    ("node-smith", "cd -", False),
    ("node-smith", "cd", False),
    ("node-smith", "pushd src", False),
    ("node-smith", "popd", False),
    ("node-smith", "cd ./sr*", False),
    ("node-smith", "cd ./src/main.weft", False),
    ("node-smith", "cd ./.?", False),
    ("run-digger", "ffprobe -f mp4 -i .?/file", False),
    ("red-teamer", "grep x < .?/x", False),
    ("red-teamer", "grep x src/.h*", False),
    ("red-teamer", "grep -rn '[a-z]+' src", True),
    # a `cd` written without `./` (which would follow CDPATH), and an input whose format is not named right before it
    ("node-smith", "cd src && grep x main.weft", False),
    ("node-smith", "cd ./src/../src && grep x main.weft", False),
    ("run-digger", "ffprobe clip.mp4", False),
    ("run-digger", "ffprobe -i clip.mp4", False),
    ("run-digger", "ffprobe -f hls -i clip.mp4", False),
    ("designer", "ffmpeg -i clip.mp4 .scratch/designer/x.png", False),
    ("designer", "ffmpeg -f mp4 -ss 2 -i clip.mp4 .scratch/designer/x.png", False),
    ("designer", "ffmpeg -f concat -i clip.mp4 .scratch/designer/x.png", False),
    ("run-digger", "ffprobe -v error -f mp4 -i clip.mp4", True),
    ("designer", "ffmpeg -ss 2 -f mp4 -i clip.mp4 .scratch/designer/x.png", True),
    # a `cd` whose folder holds a link or `..`, where the shell and this check would disagree on where it is; and an ffmpeg output read from the folder a `cd` went to
    ("node-smith", "cd ./link && cd ./.. && grep -r SECRET ../..", False),
    ("node-smith", "cd ./link/.. && grep -r SECRET ../..", False),
    ("node-smith", "cd ./link", False),
    ("node-smith", "cd ./a/b/../b", False),
    ("node-smith", "cd ./sub && cd ./.. && grep x src", False),
    ("node-smith", "cd ./src && ffmpeg -f mp4 -i ../clip.mp4 x.png", False),
]


class SpecialistCommands(unittest.TestCase):
    def test_cases(self):
        with tempfile.TemporaryDirectory() as temp:
            project = os.path.realpath(temp)
            for folder in ("src", ".scratch/node-smith", ".scratch/designer"):
                os.makedirs(os.path.join(project, folder))
            for name in ("src/main.weft", "clip.mp4"):
                open(os.path.join(project, name), "w").close()
            os.symlink("/etc", os.path.join(project, "out"))
            os.symlink("/etc", os.path.join(project, "linkout"))
            os.symlink("/etc", os.path.join(project, "etc"))
            os.makedirs(os.path.join(project, "sub"))
            os.symlink("/etc", os.path.join(project, "sub", "esc"))
            os.makedirs(os.path.join(project, "a", "b", "c"))
            os.symlink("a/b/c", os.path.join(project, "link"))
            open(os.path.join(project, "src/a.txt"), "w").close()
            for agent, command, allowed in CASES:
                with self.subTest(agent=agent, command=command):
                    reason = hook.refusal(command, LISTS[agent], project, project)
                    self.assertEqual(reason is None, allowed, reason or "allowed")


if __name__ == "__main__":
    unittest.main()

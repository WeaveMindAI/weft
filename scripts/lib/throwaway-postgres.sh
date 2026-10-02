# Start a throwaway Postgres container and wait until it accepts
# connections. Shared by run-db-tests.sh and setup.sh's --migration
# block, so there is one way to do this.
#
#   start_throwaway_postgres <name-prefix>
#
# The container is named <name-prefix>-<pid> and listens on a
# loopback-only port the kernel picks, so concurrent runs (worktrees,
# CI jobs) never fight over a name or a port, and nothing off the
# machine can reach it. Fails loudly (with the container's logs) if
# Postgres never comes up or the container dies, and leaves the
# connection URL in $THROWAWAY_DATABASE_URL and the container name in
# $THROWAWAY_PG_CONTAINER. The CALLER owns cleanup (its own EXIT trap
# running `docker rm -f -v "$THROWAWAY_PG_CONTAINER"`): scripts already
# carry traps of their own, and a trap set here would silently replace
# them. That caller-side removal is also why there is no `--rm` here:
# a container that crashes must stay around long enough for the
# failure branch below to read its logs. Every removal in here and in
# the callers takes `-v`: postgres:18 declares a VOLUME, so a plain
# `rm` strands the container's anonymous volume (an initdb cluster)
# forever; `-v` removes anonymous volumes with the container and never
# touches named ones.

start_throwaway_postgres() {
  local container="$1-$$"
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is not on PATH, so no throwaway postgres can be started" >&2
    return 1
  fi
  # Every container made here carries the uid of the user who made it,
  # and only this user's are ever removed: the docker daemon is the
  # machine's, and another user's run is none of ours.
  local owner
  owner="weft.throwaway-owner=$(id -u)"
  # A leftover same-name container from a run whose trap never fired
  # (SIGKILL, crashed caller): remove it WITH its anonymous volume (-v),
  # the same volume this would otherwise strand. Its pid is ours now, so
  # the run that made it is gone.
  if [ -n "$(docker ps -aq --filter "name=^$container\$" --filter "label=$owner")" ]; then
    docker rm -f -v "$container" >/dev/null 2>&1 || true
  fi
  # The same for every earlier run of this prefix whose process is gone
  # (killed before its trap ran): each is named after its runner's pid,
  # so a gone pid is a container nothing will ever remove. `ps -p` sees
  # every user's processes; `kill -0` fails on a live process of another
  # user too, which would read as gone.
  local leftover pid
  for leftover in $(docker ps -a --format '{{.Names}}' --filter "name=^$1-[0-9]+\$" --filter "label=$owner"); do
    pid="${leftover##*-}"
    if ! ps -p "$pid" >/dev/null 2>&1; then
      docker rm -f -v "$leftover" >/dev/null 2>&1 || true
    fi
  done
  # Exported BEFORE the container runs, so a Ctrl-C anywhere in the
  # readiness wait still leaves the caller's trap pointing at the right
  # container instead of an unset variable and an orphan.
  THROWAWAY_PG_CONTAINER="$container"
  # Host port 0 asks the kernel for a free loopback port; docker port
  # reports the one it got.
  # SYNC: postgres image tag <-> setup.sh (--purge --postgres, which
  #       reclaims exactly this image),
  #       crates/weft-cli/src/commands/daemon.rs (POSTGRES_IMAGE, 18-alpine)
  if ! docker run -d --name "$container" --label "$owner" -p 127.0.0.1:0:5432 \
       -e POSTGRES_PASSWORD=postgres postgres:18 >/dev/null; then
    echo "could not start the throwaway postgres container '$container'" >&2
    return 1
  fi
  local port
  port=$(docker port "$container" 5432/tcp | head -n1)
  port="${port##*:}"
  if [ -z "$port" ]; then
    echo "docker reported no host port for '$container'; its logs:" >&2
    docker logs "$container" >&2 2>&1 || true
    docker rm -f -v "$container" >/dev/null 2>&1 || true
    return 1
  fi
  local tries=0
  until docker exec "$container" pg_isready -U postgres >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null)" != "true" ] \
       || [ "$tries" -gt 60 ]; then
      echo "the throwaway postgres '$container' never became ready; its logs:" >&2
      docker logs "$container" >&2 2>&1 || true
      docker rm -f -v "$container" >/dev/null 2>&1 || true
      return 1
    fi
    sleep 1
  done
  THROWAWAY_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:$port/postgres"
}

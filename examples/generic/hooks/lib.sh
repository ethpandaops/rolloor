# Shared by the hooks, which each read one target, or the soak document, as
# JSON on stdin. Exit 0 means yes, 1 no and 3 could not check; the first line
# of stdout is the reason.

# answer CODE LINE...: prints the lines and exits with CODE.
answer() {
  answered=1
  printf '%s\n' "${@:2}"
  exit "$1"
}

# Leaving any other way, such as a missing tool or a command failing where no
# answer expected it, means the hook could not check.
trap 'exit_status=$?; [ -n "${answered:-}" ] || [ "$exit_status" -eq 0 ] || { echo "stopped unexpectedly (exit $exit_status)"; exit 3; }' EXIT

# container_state SERVICE: running, another docker state such as exited or
# restarting, or absent when the service has no container. Fails when docker
# does not answer.
container_state() {
  local state
  state=$(docker ps -a --filter "name=^/?rolloor-example-$1-1\$" --format '{{.State}}') || return 1
  echo "${state:-absent}"
}

# check SERVICE PORT: sets verdict (0, 1 or 3) and why for SERVICE's container
# answering 200 on PORT. Silence or a gateway error is a no once the container
# is gone or stopped, and unknown while it runs.
check() {
  local code state
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:$2/") || code=000

  case $code in
    200) verdict=0 why="200 on :$2"; return ;;
    000) why="no answer on :$2" ;;
    502 | 504) why="HTTP $code from a gateway on :$2" ;;
    *) verdict=1 why="HTTP $code on :$2"; return ;;
  esac

  if ! state=$(container_state "$1"); then
    verdict=3 why="$why; docker does not answer"
  elif [ "$state" = running ]; then
    verdict=3 why="$why; the container is running"
  else
    verdict=1 why="$why; the container is $state"
  fi
}

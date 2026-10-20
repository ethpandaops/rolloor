# Shared by the hooks. Each hook gets one target (or a soak document) as JSON
# on stdin; these read the fields the targets file puts under extra. Exit 0
# means yes, 1 no and 3 could not check; the first line of stdout is the reason.

# answer CODE LINE...: prints the lines and exits with CODE.
answer() {
  answered=1
  printf '%s\n' "${@:2}"
  exit "$1"
}

# Leaving any other way, such as a missing tool or a command failing where no
# answer expected it, means the hook could not check.
trap 'exit_status=$?; [ -n "${answered:-}" ] || [ "$exit_status" -eq 0 ] || { echo "stopped unexpectedly (exit $exit_status)"; exit 3; }' EXIT

# updater_get PATH: GET from the target's updater (watchtower) API.
updater_get() {
  curl -fsS --max-time 10 -H "Authorization: Bearer ${UPDATER_TOKEN:-}" "$(jq -r .extra.updater <<<"$target")$1"
}

# updater_post PATH: POST to the target's updater API.
updater_post() {
  curl -fsS --max-time 30 -X POST -H "Authorization: Bearer ${UPDATER_TOKEN:-}" "$(jq -r .extra.updater <<<"$target")$1"
}

# container_field LISTING FIELD: FIELD of the target's container in an updater
# listing, or empty.
container_field() {
  jq -r --arg n "$(jq -r .extra.container <<<"$target")" --arg f "$2" \
    '.containers[]? | select((.name | ltrimstr("/")) == $n) | .[$f] | tostring' <<<"$1"
}

# container_state: running, stopped or absent, as the updater reports the
# target's container. Fails when the updater does not give a clear answer.
container_state() {
  local details running
  details=$(updater_get "/v1/containers/details?name=$(jq -r .extra.container <<<"$target")") || return 1
  running=$(container_field "$details" running) || return 1

  case $running in
    true) echo running ;;
    false) echo stopped ;;
    "") echo absent ;;
    *) return 1 ;;
  esac
}

# unreachable API: answers for a target whose API gave no usable answer. A
# stopped or missing container is a no; a running one, or an updater that does
# not answer, leaves it unknown.
unreachable() {
  local state
  state=$(container_state) || answer 3 "$1 unreachable; the updater gives no clear answer"
  [ "$state" = running ] || answer 1 "$1 unreachable; the container is $state"
  answer 3 "$1 unreachable; the container is running"
}

# short DIGEST: the first 12 hex characters.
short() {
  local d=${1#sha256:}
  echo "${d:0:12}"
}

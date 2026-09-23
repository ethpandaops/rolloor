# Shared by the hooks. Each hook gets one target (or a soak document) as JSON
# on stdin; these read the fields the targets file puts under extra.

# updater_get PATH: GET from the target's updater (watchtower) API.
updater_get() {
  curl -fsS --max-time 10 -H "Authorization: Bearer ${UPDATER_TOKEN}" "$(jq -r .extra.updater <<<"$target")$1"
}

# updater_post PATH: POST to the target's updater API.
updater_post() {
  curl -fsS --max-time 30 -X POST -H "Authorization: Bearer ${UPDATER_TOKEN}" "$(jq -r .extra.updater <<<"$target")$1"
}

# container_field LISTING FIELD: FIELD of the target's container in an updater
# listing, or empty.
container_field() {
  jq -r --arg n "$(jq -r .extra.container <<<"$target")" --arg f "$2" \
    '.containers[]? | select((.name | ltrimstr("/")) == $n) | .[$f] | tostring' <<<"$1"
}

# short DIGEST: the first 12 hex characters.
short() {
  local d=${1#sha256:}
  echo "${d:0:12}"
}

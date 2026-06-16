#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
config_path="${COMMITLINT_CONFIG:-${repo_root}/commitlint.toml}"

usage() {
  echo "usage: $0 <commit-msg-file> | --message <header> [label]" >&2
}

if [[ $# -eq 0 ]]; then
  usage
  exit 2
fi

if [[ "${1}" == "--message" ]]; then
  if [[ $# -lt 2 ]]; then
    usage
    exit 2
  fi
  header="${2}"
  label="${3:-commit}"
else
  message_file="${1}"
  header="$(sed -n '1p' "${message_file}")"
  label="${message_file}"
fi

max_header_length="$(
  sed -n 's/^max_header_length[[:space:]]*=[[:space:]]*\([0-9][0-9]*\).*$/\1/p' \
    "${config_path}"
)"
max_header_length="${max_header_length:-100}"

types="$(
  awk '
    /^types[[:space:]]*=/ { in_types = 1 }
    in_types {
      line = $0
      gsub(/[\[\]",]/, " ", line)
      n = split(line, parts, /[[:space:]]+/)
      for (i = 1; i <= n; i++) {
        if (parts[i] != "" && parts[i] !~ /^(types|=)$/) print parts[i]
      }
      if ($0 ~ /\]/) in_types = 0
    }
  ' "${config_path}" | paste -sd'|' -
)"
types="${types:-build|chore|ci|docs|feat|fix|perf|refactor|revert|style|test}"

allow_merge="$(
  sed -n 's/^allow_merge_commits[[:space:]]*=[[:space:]]*\(true\|false\).*$/\1/p' \
    "${config_path}"
)"
allow_revert="$(
  sed -n 's/^allow_revert_commits[[:space:]]*=[[:space:]]*\(true\|false\).*$/\1/p' \
    "${config_path}"
)"
subject_full_stop="$(
  sed -n 's/^subject_full_stop[[:space:]]*=[[:space:]]*\(true\|false\).*$/\1/p' \
    "${config_path}"
)"

if [[ -z "${header}" ]]; then
  echo "commitlint: empty commit header in ${label}" >&2
  exit 1
fi

if (( ${#header} > max_header_length )); then
  echo "commitlint: header too long in ${label}: ${#header} > ${max_header_length}" >&2
  echo "  ${header}" >&2
  exit 1
fi

if [[ "${allow_merge:-false}" == "true" && "${header}" == Merge\ * ]]; then
  exit 0
fi

if [[ "${allow_revert:-false}" == "true" && "${header}" == Revert\ \"* ]]; then
  exit 0
fi

if ! [[ "${header}" =~ ^(${types})(\([a-z0-9._-]+\))?(!)?:\ .+ ]]; then
  echo "commitlint: invalid commit header in ${label}" >&2
  echo "  ${header}" >&2
  echo "expected: <type>(<scope>)?: <subject>" >&2
  echo "allowed types: ${types//|/, }" >&2
  exit 1
fi

if [[ "${subject_full_stop:-false}" == "false" && "${header}" == *. ]]; then
  echo "commitlint: subject must not end with a period in ${label}" >&2
  echo "  ${header}" >&2
  exit 1
fi

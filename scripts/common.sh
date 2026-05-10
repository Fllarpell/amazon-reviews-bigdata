#!/usr/bin/env bash

load_dotenv() {
  local root="$1"
  if [[ -f "${root}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${root}/.env"
    set +a
  fi
}

resolve_python_cmd() {
  local root="$1"
  PYTHON_CMD=()
  if [[ -n "${PYTHON:-}" ]]; then
    PYTHON_CMD=("${PYTHON}")
  elif [[ -x "${root}/.venv/bin/python" ]]; then
    PYTHON_CMD=("${root}/.venv/bin/python")
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD=(python3)
  elif command -v py >/dev/null 2>&1; then
    PYTHON_CMD=(py -3)
  elif command -v py.exe >/dev/null 2>&1; then
    PYTHON_CMD=(py.exe -3)
  else
    echo "No Python interpreter found (tried .venv/bin/python, python3, py)." >&2
    return 1
  fi
}

python_script_path_for_platform() {
  local script_path="$1"
  if [[ "${PYTHON_CMD[0]}" =~ ^(py|py\.exe|python\.exe)$ ]]; then
    if command -v cygpath >/dev/null 2>&1; then
      script_path="$(cygpath -w "${script_path}")"
    elif [[ "${script_path}" == /mnt/* ]]; then
      local drive_letter
      local suffix
      drive_letter="$(echo "${script_path}" | cut -d'/' -f3 | tr '[:lower:]' '[:upper:]')"
      suffix="$(echo "${script_path}" | cut -d'/' -f4- | sed 's#/#\\\\#g')"
      script_path="${drive_letter}:\\${suffix}"
    fi
  fi
  printf '%s\n' "${script_path}"
}

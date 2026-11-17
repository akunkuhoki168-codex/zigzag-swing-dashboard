#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
if [ -f push.pid ]; then
  P=$(cat push.pid 2>/dev/null)
  if [ -n "$P" ] && kill -0 "$P" 2>/dev/null; then
    cwd=$(readlink -f /proc/$P/cwd 2>/dev/null)
    if [ "$cwd" = "$(pwd)" ]; then
      kill "$P" 2>/dev/null || true
      echo "stopped publisher pid $P"
    else
      echo "pid $P is not this publisher; leaving it alone"
    fi
  fi
  rm -f push.pid
fi

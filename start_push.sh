#!/usr/bin/env bash
# Idempotent: starts the Zigzag Swing dashboard publisher unless it is already running.
cd "$(dirname "$0")" || exit 1
PIDF=push.pid
LOG=push.log
if [ -f "$PIDF" ]; then
  P=$(cat "$PIDF" 2>/dev/null)
  if [ -n "$P" ] && kill -0 "$P" 2>/dev/null && tr '\0' ' ' < /proc/$P/cmdline | grep -q "adx-swing-dashboard/push.py\|/workspace/adx-swing-dashboard/push.py\|push.py"; then
    # make sure this is OUR push.py, not mexc/binance
    if [ "$(readlink -f /proc/$P/cwd 2>/dev/null)" = "$(pwd)" ] || tr '\0' ' ' < /proc/$P/cmdline | grep -q "adx-swing-dashboard"; then
      echo "Zigzag Swing publisher already running (pid $P)"; exit 0
    fi
  fi
fi
nohup setsid python3 -u push.py >> "$LOG" 2>&1 < /dev/null &
echo $! > "$PIDF"
sleep 1
echo "started Zigzag Swing publisher (pid $(cat $PIDF)), log: $(pwd)/$LOG"

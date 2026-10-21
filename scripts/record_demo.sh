#!/usr/bin/env bash
# Record a README animation of one `logplane demo` scenario.
#
#   scripts/record_demo.sh [scenario]      # default: recovery
#
# Writes docs/demo-<scenario>.gif. Needs asciinema and agg 1.5 or newer
# (macOS: brew install asciinema agg) and an environment where `logplane`
# is installed. The recording uses your terminal's size; about 100x36 reads
# well on GitHub. Demo answers use their defaults (--yes) so takes match, and
# --read-pauses holds each narration long enough to read.
set -euo pipefail

scenario="${1:-recovery}"
pace="${LOGPLANE_DEMO_PACE:-1}"
root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/docs/demo-$scenario.gif"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

for tool in asciinema agg logplane; do
  command -v "$tool" >/dev/null || { echo "record_demo: $tool is not on PATH" >&2; exit 2; }
done

cat > "$work/session.sh" <<SESSION
cd "$work"
printf '\033[1;32m\$\033[0m logplane demo %s\n' "$scenario"
logplane demo "$scenario" --pace "$pace" --yes --no-keep-running --read-pauses
SESSION

asciinema rec -c "bash $work/session.sh" "$work/demo.cast"
mkdir -p "$(dirname "$out")"
agg --font-size 14 --idle-time-limit 8 --last-frame-duration 8 "$work/demo.cast" "$out"
echo "Wrote $out"

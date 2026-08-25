#!/bin/sh
set -eu
candidate_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
server_root=${ELECTRIC_CIRCUITS_SERVER_ROOT:-/Users/bozilabs/labs/electric-circuits}
exec pnpm --dir "$server_root" exec tsx "$candidate_dir/Scripts/real-filtered-windows-pg18.ts"

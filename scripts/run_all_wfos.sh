#!/bin/bash
# DT818_pro WFO chain — per-session (LDN + NY) with two phases each.
# Phase 1: entry sweep (caps=0).  Phase 2: cap sweep with entry winner fixed.
# Final: portfolio Phase E (deal-merges per-session winners).
set -u
cd c:/Users/Zhu-En/zgb-opti

LOGDIR=output/logs
mkdir -p "$LOGDIR"

run() {
    local name=$1
    shift
    echo "===================================" | tee -a "$LOGDIR/wfo_chain.log"
    echo "$(date +%H:%M:%S) Launching $name" | tee -a "$LOGDIR/wfo_chain.log"
    echo "===================================" | tee -a "$LOGDIR/wfo_chain.log"
    python -u "$@" 2>&1 | tee "$LOGDIR/${name,,}.log"
    local rc=$?
    echo "$(date +%H:%M:%S) $name exit=$rc" | tee -a "$LOGDIR/wfo_chain.log"
    return 0
}

run "ORB_LDN_P1"  scripts/sim_wfo_orb.py --session ldn --phase 1
run "ORB_LDN_P2"  scripts/sim_wfo_orb.py --session ldn --phase 2
run "ORB_NY_P1"   scripts/sim_wfo_orb.py --session ny  --phase 1
run "ORB_NY_P2"   scripts/sim_wfo_orb.py --session ny  --phase 2
run "PhaseE"      scripts/sim_dt818_pro_phase_e.py

echo "$(date +%H:%M:%S) DONE - chain complete" | tee -a "$LOGDIR/wfo_chain.log"

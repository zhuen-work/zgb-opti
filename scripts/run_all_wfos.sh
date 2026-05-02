#!/bin/bash
# Chained sequential WFO runner. Launches each WFO; if a WFO crashes,
# logs the error and continues to the next. Final phase E always runs.
set -u
cd c:/Users/Zhu-En/zgb-opti

LOGDIR=output/logs
mkdir -p "$LOGDIR"

run() {
    local name=$1
    local script=$2
    echo "===================================" | tee -a "$LOGDIR/wfo_chain.log"
    echo "$(date +%H:%M:%S) Launching $name WFO" | tee -a "$LOGDIR/wfo_chain.log"
    echo "===================================" | tee -a "$LOGDIR/wfo_chain.log"
    python -u "$script" 2>&1 | tee "$LOGDIR/wfo_${name,,}_may2.log"
    local rc=$?
    echo "$(date +%H:%M:%S) $name WFO exit=$rc" | tee -a "$LOGDIR/wfo_chain.log"
    return 0  # always continue
}

# ORB already running externally — wait for its winner.json
echo "$(date +%H:%M:%S) Waiting for ORB WFO to complete..." | tee -a "$LOGDIR/wfo_chain.log"
until [ -f "output/wfo_orb_spread70_may2/winner.json" ]; do sleep 30; done
echo "$(date +%H:%M:%S) ORB winner detected — continuing chain" | tee -a "$LOGDIR/wfo_chain.log"

run "EMP"    scripts/sim_wfo_ema_pullback.py
run "FBO_S1" scripts/sim_wfo_fbo_s1.py
run "FBO_S2" scripts/sim_wfo_fbo_s2.py
run "LSFVG"  scripts/sim_wfo_lsfvg.py

echo "===================================" | tee -a "$LOGDIR/wfo_chain.log"
echo "$(date +%H:%M:%S) All WFOs done. Running Phase E..." | tee -a "$LOGDIR/wfo_chain.log"
echo "===================================" | tee -a "$LOGDIR/wfo_chain.log"
python -u scripts/sim_dt818_pro_phase_e.py 2>&1 | tee "$LOGDIR/phase_e_may2.log"
echo "$(date +%H:%M:%S) DONE — chain complete" | tee -a "$LOGDIR/wfo_chain.log"

//+------------------------------------------------------------------+
//| DT818_pro_v6.mq5                                                  |
//| ORB EA: 6-stream rank-portfolio + 6 STOP-on-EXTENSION hedges      |
//|         + V2 fractal-confirmed entry (global)                     |
//|                                                                   |
//| v6 changes vs v5 (2026-05-24 PT2):                                |
//|   v5 used SMART-TP reverse LIMIT hedge (two-stage partial close   |
//|     at parent_entry waiting for retracement).                     |
//|   v6 replaces it with STOP-on-EXTENSION hedge:                    |
//|     - Parent BUY  SL'd → SELL_STOP at parent_SL - ExtPts          |
//|     - Parent SELL SL'd → BUY_STOP  at parent_SL + ExtPts          |
//|     - Single TP at ExtPts × TPMult (continuation direction)       |
//|     - Single SL at parent_SL_dist × SLMult (against continuation) |
//|     - Risk-equalized lots = parent_lots / SLMult (no split)       |
//|     - Bet: parent SL was real; price continues past parent_SL.    |
//|   Magic numbers unchanged (8111-8666) so live tracking remains    |
//|     continuous, but the comment/logic distinguishes v6 trades.    |
//|   Setfile inputs repurposed:                                      |
//|     _HEDGE_S*_ExtPts          = points past parent SL (NEW)       |
//|     _HEDGE_S*_TPMult           = TP = ExtPts × TPMult (re-meaning)|
//|     _HEDGE_S*_SLMult           = hedge SL dist mult (unchanged)   |
//|     _HEDGE_S*_PartialFraction  = DEPRECATED (kept for setfile compat)|
//|     _HEDGE_S*_ProfitMult       = DEPRECATED                        |
//|   Rationale (output/wfo_hedge_geometry_may23/ + ac9ec64):         |
//|     - LIMIT hedge decayed in W3/W4 OOS (slope -103%, W4 -$2,964)  |
//|     - STOP-ext at ext=100/tp_mult=3.0/sl_mult=1.0 stays uniformly |
//|       positive (W1 +$1,907 W2 +$1,762 W3 +$1,105 W4 +$1,724).    |
//|     - Mode B (STOP-ext only): +$28k NP vs parents-only for only   |
//|       +$2k DD (13:1 NP-per-DD ratio).                              |
//|                                                                   |
//| v5 changes vs v4 (2026-05-24):                                    |
//|   v4 fired BUY_STOP + SELL_STOP immediately at range close.       |
//|   v5 adds optional GLOBAL fractal-confirm gate:                   |
//|     - When _ORB_FractalConfirm = true (applies to ALL streams),   |
//|       BUY_STOP is held until an N-bar (default 5) up-fractal      |
//|       confirms on M5 with high > range_high (BUY break level).    |
//|       Same for SELL with down-fractal < range_low.                |
//|     - Each side fires independently when its same-side fractal    |
//|       confirms. If neither side confirms before pending_expire,   |
//|       the session is abandoned.                                   |
//|     - When _ORB_FractalConfirm = false, behavior is identical     |
//|       to v4.                                                      |
//|     - _ORB_FractalWidth must be odd >=3 (3 or 5).                 |
//|   Rationale: 2026-05-23 4-window WFO on may23 windows showed all  |
//|   top-20 robust candidates use fractal_confirm=true width=5,      |
//|   delivering +58% NP and +132% NP/DD$ vs baseline with the same   |
//|   ORB params. See output/wfo_orb_v2_may23/.                       |
//|                                                                   |
//| v4 changes vs v3 (2026-05-20):                                    |
//|   v3 used single-TP reverse hedge (TP = tp_mult * sl_dist).       |
//|   v4 uses SMART-TP: two-stage partial close at BE then profit.    |
//|     - Stage 1 (alpha fraction): TP at break-even of combined loss |
//|     - Stage 2 (1-alpha fraction): TP at profit_mult * combined    |
//|     - Geometry: tp1_dist = sl_dist * sl_mult / alpha              |
//|                 tp2_dist = sl_dist * sl_mult * pm / (1 - alpha)   |
//|     - Validity: alpha > 1 / (profit_mult + 1)                     |
//|                                                                   |
//| REVERSE HEDGE rationale (backed by 2026-05-11..14 backfill sim    |
//| in scripts/backfill_reverse_hedge.py):                            |
//|   On TIGHT/NORMAL regime days, parent SLs are followed by deep    |
//|   continuation past parent entry. Reverse-hedge captures that.    |
//|   Backfill recovery: +$35,160 of $60,301 parent SL losses = 58%   |
//|   recovery Mon-Thu (NORMAL/TIGHT days). WIDE regime days = hedge  |
//|   NO_FIRE (price never retraces to entry); doesn't help / hurt.   |
//|                                                                   |
//| LOGIC:                                                            |
//|   Parent BUY  SL'd → place SELL_LIMIT at parent_entry             |
//|                       (entry is ABOVE current price since BUY     |
//|                        SL'd below entry → valid SELL_LIMIT)       |
//|                       Hedge SL = entry + sl_dist (above entry)    |
//|                       Hedge TP = entry - tp_mult*sl_dist (below)  |
//|                                                                   |
//|   Parent SELL SL'd → place BUY_LIMIT at parent_entry              |
//|                       (entry is BELOW current price since SELL    |
//|                        SL'd above entry → valid BUY_LIMIT)        |
//|                       Hedge SL = entry - sl_dist (below entry)    |
//|                       Hedge TP = entry + tp_mult*sl_dist (above)  |
//|                                                                   |
//|   Hedge fires when price RETRACES BACK to parent's original       |
//|   entry, then profits if price RESUMES SL-direction movement.     |
//|                                                                   |
//| Hedge magic numbers: 8111/8222/8333/8444/8555/8666 (smart-TP).    |
//|   Parents stay 1111-6666. v2.1_h retry-hedge magics 7111-7666     |
//|   reserved for legacy. v4 smart-TP reverse-hedge uses 8xxx (each  |
//|   stream places TWO limits per SL: _s1 BE-stage, _s2 profit-stage)|
//|                                                                   |
//| v2.1 PATCH (2026-05-12): TWO timezone fixes for Vantage at UTC+3. |
//|                                                                   |
//| FIX 1 (range loop): iTime() returns broker-time epoch, but        |
//|   range_start was built from TimeGMT() (real UTC). Range loop     |
//|   used to accumulate bars from a window 3h EARLIER than intended. |
//|   Now: iTime values are converted to real UTC via                 |
//|   `bt - (TimeCurrent() - TimeGMT())` before comparison.           |
//|                                                                   |
//| FIX 2 (pending expiry): MT5 server stores pending expire in       |
//|   broker-time scale, but EA was passing pending_expire as real    |
//|   UTC epoch. Result: pendings expired 3h EARLIER than configured  |
//|   PendingExpireMinutes. (Today's pre-fix data: setfile says       |
//|   240min, actual armed = 60min.) Now: expire is converted to      |
//|   broker scale via `pending_expire + broker_offset` before        |
//|   passing to OrderSend.                                           |
//|                                                                   |
//| Result: setfile session hours now mean real UTC hours directly,   |
//| pending expiry duration matches PendingExpireMinutes. To trade    |
//| real UTC 04:00 LDN (Session A), use LDN_StartHour=4. To trade     |
//| real UTC 07:00 LDN (Session B), use LDN_StartHour=7.              |
//|                                                                   |
//|                                                                   |
//| Weekly cadence (Sat Malaysia time reopt):                         |
//|   S1/S2/S3: top 3 ranks from PREVIOUS-week WFO (proven, aging)    |
//|   S4/S5/S6: top 3 ranks from CURRENT-week WFO (fresh)             |
//|                                                                   |
//| Magics: 1111/2222/3333 (previous) + 4444/5555/6666 (current)      |
//| Avoids 5111/5222/5333 (reserved for hedge EA).                    |
//|                                                                   |
//| Risk allocation: setfile _RiskPct = total_risk / 6 so total       |
//| per-setup exposure = _RiskPct × 6 when all streams enter together.|
//+------------------------------------------------------------------+
#property copyright "ZGB Trading"
#property version   "6.00"
#property strict

#include <Trade/Trade.mqh>

CTrade g_trade;

//====================================================================
// Global account inputs
//====================================================================
input double  _CapitalProtectionAmount = 0.0;    // Stop ALL trading below this equity (0=off)
input double  _RiskPct                 = 3.0;    // Risk % per trade (shared)
input int     _LotMode                 = 1;      // 0=fixed LotStep, 1=risk%, 2=tiered
input int     TierBase                 = 2000;
input double  LotStep                  = 0.01;
input int     _MaxSpreadPts            = 50;     // Skip new orders if quoted spread > this (0=off)

//====================================================================
// ORB shared params (sessions + entry TF + broker offset + range filter)
// Vantage broker time = GMT+2 (winter) / GMT+3 (summer DST). Update at DST flips.
//====================================================================
input ENUM_TIMEFRAMES  _ORB_EntryTF             = PERIOD_M5;
input int              _BrokerGMTOffsetHours    = 0;    // DEPRECATED: no longer used (EA uses TimeGMT() since 2026-05-07). Kept for setfile compat.
// _ORB_BufferPts removed 2026-05-19 — WFO consistently picked 0; entries now
// placed exactly at range_high (BUY_STOP) / range_low (SELL_STOP). To re-add,
// reintroduce input + apply (range_high + buffer * _Point) at lines below.
input int              _ORB_MinRangePts         = 200;
input int              _ORB_MaxRangePts         = 5000;
input bool             _ORB_LDN_Enabled         = true;
input int              _ORB_LDN_StartHour       = 7;    // UTC
input bool             _ORB_NY_Enabled          = true;
input int              _ORB_NY_StartHour        = 13;   // UTC
// v5: V2 fractal-confirmed entry — global toggle applied to ALL streams.
// When true, pending stops only arm after a same-side M5 fractal confirms
// past the break level. Width must be odd >=3 (3 or 5).
input bool             _ORB_FractalConfirm      = false;
input int              _ORB_FractalWidth        = 5;

//====================================================================
// ORB_S1 — rank 1 stream (magic 1111)
//====================================================================
input bool    _ORB_S1_Enabled              = true;
input int     _ORB_S1_Magic                = 1111;
input string  _ORB_S1_Comment              = "ORB_S1";
input int     _ORB_S1_RangeMinutes         = 90;
input int     _ORB_S1_FixedSL_Pts          = 500;
input double  _ORB_S1_RR_Ratio             = 4.0;
input double  _ORB_S1_HalfTP_Ratio         = 0.25;
input int     _ORB_S1_PendingExpireMinutes = 240;
input double  _ORB_S1_DailyTargetPct       = 0.0;    // 0 = disabled
input double  _ORB_S1_DailyLossPct         = 0.0;    // 0 = disabled

//====================================================================
// ORB_S2 — rank 2 stream (magic 2222)
//====================================================================
input bool    _ORB_S2_Enabled              = true;
input int     _ORB_S2_Magic                = 2222;
input string  _ORB_S2_Comment              = "ORB_S2";
input int     _ORB_S2_RangeMinutes         = 90;
input int     _ORB_S2_FixedSL_Pts          = 400;
input double  _ORB_S2_RR_Ratio             = 4.0;
input double  _ORB_S2_HalfTP_Ratio         = 0.0;
input int     _ORB_S2_PendingExpireMinutes = 240;
input double  _ORB_S2_DailyTargetPct       = 0.0;
input double  _ORB_S2_DailyLossPct         = 0.0;

//====================================================================
// ORB_S3 — rank 3 stream (magic 3333)
//====================================================================
input bool    _ORB_S3_Enabled              = true;
input int     _ORB_S3_Magic                = 3333;
input string  _ORB_S3_Comment              = "ORB_S3";
input int     _ORB_S3_RangeMinutes         = 90;
input int     _ORB_S3_FixedSL_Pts          = 350;
input double  _ORB_S3_RR_Ratio             = 4.0;
input double  _ORB_S3_HalfTP_Ratio         = 0.5;
input int     _ORB_S3_PendingExpireMinutes = 240;
input double  _ORB_S3_DailyTargetPct       = 0.0;
input double  _ORB_S3_DailyLossPct         = 0.0;

//====================================================================
// ORB_S4 — previous-week rank 1 stream (magic 4444)
//====================================================================
input bool    _ORB_S4_Enabled              = true;
input int     _ORB_S4_Magic                = 4444;
input string  _ORB_S4_Comment              = "ORB_S4";
input int     _ORB_S4_RangeMinutes         = 90;
input int     _ORB_S4_FixedSL_Pts          = 500;
input double  _ORB_S4_RR_Ratio             = 4.0;
input double  _ORB_S4_HalfTP_Ratio         = 0.25;
input int     _ORB_S4_PendingExpireMinutes = 240;
input double  _ORB_S4_DailyTargetPct       = 0.0;
input double  _ORB_S4_DailyLossPct         = 0.0;

//====================================================================
// ORB_S5 — previous-week rank 2 stream (magic 5555)
//====================================================================
input bool    _ORB_S5_Enabled              = true;
input int     _ORB_S5_Magic                = 5555;
input string  _ORB_S5_Comment              = "ORB_S5";
input int     _ORB_S5_RangeMinutes         = 90;
input int     _ORB_S5_FixedSL_Pts          = 400;
input double  _ORB_S5_RR_Ratio             = 4.0;
input double  _ORB_S5_HalfTP_Ratio         = 0.0;
input int     _ORB_S5_PendingExpireMinutes = 240;
input double  _ORB_S5_DailyTargetPct       = 0.0;
input double  _ORB_S5_DailyLossPct         = 0.0;

//====================================================================
// ORB_S6 — previous-week rank 3 stream (magic 6666)
//====================================================================
input bool    _ORB_S6_Enabled              = true;
input int     _ORB_S6_Magic                = 6666;
input string  _ORB_S6_Comment              = "ORB_S6";
input int     _ORB_S6_RangeMinutes         = 90;
input int     _ORB_S6_FixedSL_Pts          = 350;
input double  _ORB_S6_RR_Ratio             = 4.0;
input double  _ORB_S6_HalfTP_Ratio         = 0.5;
input int     _ORB_S6_PendingExpireMinutes = 240;
input double  _ORB_S6_DailyTargetPct       = 0.0;
input double  _ORB_S6_DailyLossPct         = 0.0;

//====================================================================
// HEDGE_S1r — reverse hedge sub-stream for ORB_S1 (magic 8111)
// Fires SELL_LIMIT / BUY_LIMIT at parent's original entry (reversion-continuation play)
//====================================================================
input bool    _HEDGE_S1_Enabled                = true;
input int     _HEDGE_S1_Magic                  = 8111;
input string  _HEDGE_S1_Comment                = "ORB_S1r";
input int     _HEDGE_S1_ParentMagic            = 1111;
input int     _HEDGE_S1_FixedSL_Pts            = 500;
input double  _HEDGE_S1_RR_Ratio               = 4.0;
input int     _HEDGE_S1_ExpireMinutes          = 120;
input int     _HEDGE_S1_MaxSecondsAfterEntry   = 3600;
input double  _HEDGE_S1_PartialFraction        = 0.5;   // DEPRECATED in v6 (kept for setfile compat)
input double  _HEDGE_S1_ProfitMult              = 1.2;   // DEPRECATED in v6 (kept for setfile compat)
input double  _HEDGE_S1_SLMult                 = 1.0;
input int     _HEDGE_S1_ExtPts                 = 100;   // v6: points past parent SL for STOP-ext entry
input double  _HEDGE_S1_TPMult                 = 3.0;   // v6: TP = ExtPts * TPMult (in continuation direction)

//====================================================================
// HEDGE_S2r — STOP-ext hedge sub-stream for ORB_S2 (magic 8222)
//====================================================================
input bool    _HEDGE_S2_Enabled                = true;
input int     _HEDGE_S2_Magic                  = 8222;
input string  _HEDGE_S2_Comment                = "ORB_S2r";
input int     _HEDGE_S2_ParentMagic            = 2222;
input int     _HEDGE_S2_FixedSL_Pts            = 500;
input double  _HEDGE_S2_RR_Ratio               = 4.0;
input int     _HEDGE_S2_ExpireMinutes          = 120;
input int     _HEDGE_S2_MaxSecondsAfterEntry   = 3600;
input double  _HEDGE_S2_PartialFraction        = 0.5;   // DEPRECATED in v6
input double  _HEDGE_S2_ProfitMult              = 1.2;   // DEPRECATED in v6
input double  _HEDGE_S2_SLMult                 = 1.0;
input int     _HEDGE_S2_ExtPts                 = 100;
input double  _HEDGE_S2_TPMult                 = 3.0;

//====================================================================
// HEDGE_S3r — STOP-ext hedge sub-stream for ORB_S3 (magic 8333)
//====================================================================
input bool    _HEDGE_S3_Enabled                = true;
input int     _HEDGE_S3_Magic                  = 8333;
input string  _HEDGE_S3_Comment                = "ORB_S3r";
input int     _HEDGE_S3_ParentMagic            = 3333;
input int     _HEDGE_S3_FixedSL_Pts            = 500;
input double  _HEDGE_S3_RR_Ratio               = 4.0;
input int     _HEDGE_S3_ExpireMinutes          = 120;
input int     _HEDGE_S3_MaxSecondsAfterEntry   = 3600;
input double  _HEDGE_S3_PartialFraction        = 0.5;   // DEPRECATED in v6
input double  _HEDGE_S3_ProfitMult              = 1.2;   // DEPRECATED in v6
input double  _HEDGE_S3_SLMult                 = 1.0;
input int     _HEDGE_S3_ExtPts                 = 100;
input double  _HEDGE_S3_TPMult                 = 3.0;

//====================================================================
// HEDGE_S4r — STOP-ext hedge sub-stream for ORB_S4 (magic 8444)
//====================================================================
input bool    _HEDGE_S4_Enabled                = true;
input int     _HEDGE_S4_Magic                  = 8444;
input string  _HEDGE_S4_Comment                = "ORB_S4r";
input int     _HEDGE_S4_ParentMagic            = 4444;
input int     _HEDGE_S4_FixedSL_Pts            = 500;
input double  _HEDGE_S4_RR_Ratio               = 4.0;
input int     _HEDGE_S4_ExpireMinutes          = 120;
input int     _HEDGE_S4_MaxSecondsAfterEntry   = 3600;
input double  _HEDGE_S4_PartialFraction        = 0.5;   // DEPRECATED in v6
input double  _HEDGE_S4_ProfitMult              = 1.2;   // DEPRECATED in v6
input double  _HEDGE_S4_SLMult                 = 1.0;
input int     _HEDGE_S4_ExtPts                 = 100;
input double  _HEDGE_S4_TPMult                 = 3.0;

//====================================================================
// HEDGE_S5r — STOP-ext hedge sub-stream for ORB_S5 (magic 8555)
//====================================================================
input bool    _HEDGE_S5_Enabled                = true;
input int     _HEDGE_S5_Magic                  = 8555;
input string  _HEDGE_S5_Comment                = "ORB_S5r";
input int     _HEDGE_S5_ParentMagic            = 5555;
input int     _HEDGE_S5_FixedSL_Pts            = 500;
input double  _HEDGE_S5_RR_Ratio               = 4.0;
input int     _HEDGE_S5_ExpireMinutes          = 120;
input int     _HEDGE_S5_MaxSecondsAfterEntry   = 3600;
input double  _HEDGE_S5_PartialFraction        = 0.5;   // DEPRECATED in v6
input double  _HEDGE_S5_ProfitMult              = 1.2;   // DEPRECATED in v6
input double  _HEDGE_S5_SLMult                 = 1.0;
input int     _HEDGE_S5_ExtPts                 = 100;
input double  _HEDGE_S5_TPMult                 = 3.0;

//====================================================================
// HEDGE_S6r — STOP-ext hedge sub-stream for ORB_S6 (magic 8666)
//====================================================================
input bool    _HEDGE_S6_Enabled                = true;
input int     _HEDGE_S6_Magic                  = 8666;
input string  _HEDGE_S6_Comment                = "ORB_S6r";
input int     _HEDGE_S6_ParentMagic            = 6666;
input int     _HEDGE_S6_FixedSL_Pts            = 500;
input double  _HEDGE_S6_RR_Ratio               = 4.0;
input int     _HEDGE_S6_ExpireMinutes          = 120;
input int     _HEDGE_S6_MaxSecondsAfterEntry   = 3600;
input double  _HEDGE_S6_PartialFraction        = 0.5;   // DEPRECATED in v6
input double  _HEDGE_S6_ProfitMult              = 1.2;   // DEPRECATED in v6
input double  _HEDGE_S6_SLMult                 = 1.0;
input int     _HEDGE_S6_ExtPts                 = 100;
input double  _HEDGE_S6_TPMult                 = 3.0;

//====================================================================
// Globals
//====================================================================
datetime g_lastBar_M1 = 0;       // shared M1 bar tracker

// Per-stream config struct (built from inputs in OnInit)
struct ORBStreamCfg
{
   bool   enabled;
   int    magic;
   string comment;
   int    range_minutes;
   int    fixed_sl_pts;
   double rr_ratio;
   double half_tp_ratio;
   int    pending_expire_minutes;
   double daily_target_pct;
   double daily_loss_pct;
};
ORBStreamCfg g_cfg_s1;
ORBStreamCfg g_cfg_s2;
ORBStreamCfg g_cfg_s3;
ORBStreamCfg g_cfg_s4;
ORBStreamCfg g_cfg_s5;
ORBStreamCfg g_cfg_s6;

// ORB session state (per-day state — range, fired flag, pending expiry)
struct ORBSession
{
   bool       active;
   bool       fired;       // legacy flag: both sides done (kept for back-compat outside this struct)
   bool       fired_buy;   // v5: BUY_STOP pendings placed (or skipped for invalid setup)
   bool       fired_sell;  // v5: SELL_STOP pendings placed (or skipped for invalid setup)
   datetime   range_start;
   datetime   range_end;
   datetime   pending_expire;
   double     range_high;
   double     range_low;
};
// Each stream has its OWN LDN + NY session state (6 streams × 2 sessions = 12 states)
ORBSession g_s1_ldn = {0};   ORBSession g_s1_ny = {0};
ORBSession g_s2_ldn = {0};   ORBSession g_s2_ny = {0};
ORBSession g_s3_ldn = {0};   ORBSession g_s3_ny = {0};
ORBSession g_s4_ldn = {0};   ORBSession g_s4_ny = {0};
ORBSession g_s5_ldn = {0};   ORBSession g_s5_ny = {0};
ORBSession g_s6_ldn = {0};   ORBSession g_s6_ny = {0};

// Per-stream daily-cap state (independent rollover + lock per stream magic)
datetime g_s1_day = 0;   double g_s1_bal_start = 0.0;   bool g_s1_lock = false;
datetime g_s2_day = 0;   double g_s2_bal_start = 0.0;   bool g_s2_lock = false;
datetime g_s3_day = 0;   double g_s3_bal_start = 0.0;   bool g_s3_lock = false;
datetime g_s4_day = 0;   double g_s4_bal_start = 0.0;   bool g_s4_lock = false;
datetime g_s5_day = 0;   double g_s5_bal_start = 0.0;   bool g_s5_lock = false;
datetime g_s6_day = 0;   double g_s6_bal_start = 0.0;   bool g_s6_lock = false;

// Hedge sub-stream config struct (built from inputs in OnInit)
struct HedgeStreamCfg
{
   bool   enabled;
   int    magic;
   string comment;
   int    parent_magic;
   int    fixed_sl_pts;
   double rr_ratio;
   int    expire_minutes;
   int    max_seconds_after_entry;
   double sl_mult;            // hedge SL distance = parent_sl_dist * sl_mult
                              // hedge lots = parent_lots / sl_mult (risk-equalized)
   // v6 STOP-ext fields
   int    ext_pts;            // points past parent_SL_price for STOP entry
   double tp_mult;            // hedge TP distance = ext_pts * tp_mult (in continuation direction)
   // DEPRECATED in v6 (kept in struct so OnInit can still read setfile values without parse errors)
   double partial_fraction;
   double profit_mult;
};
HedgeStreamCfg g_hedge_s1, g_hedge_s2, g_hedge_s3, g_hedge_s4, g_hedge_s5, g_hedge_s6;

// Hedge scan tracker — keep last 128 seen exit-deal tickets so we don't
// re-process them every tick.
ulong g_hedge_seen_tickets[128];
int   g_hedge_seen_idx = 0;
datetime g_hedge_last_scan = 0;

bool HedgeWasSeen(ulong ticket)
{
   for(int i = 0; i < ArraySize(g_hedge_seen_tickets); i++)
      if(g_hedge_seen_tickets[i] == ticket) return true;
   return false;
}
void HedgeMarkSeen(ulong ticket)
{
   g_hedge_seen_tickets[g_hedge_seen_idx] = ticket;
   g_hedge_seen_idx = (g_hedge_seen_idx + 1) % ArraySize(g_hedge_seen_tickets);
}

//====================================================================
// Helpers
//====================================================================
ENUM_TIMEFRAMES TF(int tf) { return (ENUM_TIMEFRAMES)tf; }

bool IsNewBar(ENUM_TIMEFRAMES tf, datetime &lastTime)
{
   datetime t = iTime(_Symbol, tf, 0);
   if(t != lastTime) { lastTime = t; return true; }
   return false;
}

double NormPrice(double price)
{
   double ts = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   if(ts <= 0) return NormalizeDouble(price, _Digits);
   return NormalizeDouble(MathRound(price / ts) * ts, _Digits);
}

datetime DayStart(datetime now)
{
   MqlDateTime mt; TimeToStruct(now, mt);
   mt.hour = 0; mt.min = 0; mt.sec = 0;
   return StructToTime(mt);
}

double CalcLots(double slPoints)
{
   double lots = 0;
   if(_LotMode == 0) lots = LotStep;
   else if(_LotMode == 1)
   {
      double riskMoney = AccountInfoDouble(ACCOUNT_BALANCE) * _RiskPct / 100.0;
      double tickVal   = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
      double tickSize  = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
      if(tickVal <= 0 || tickSize <= 0 || slPoints <= 0) return 0;
      double slMoney = (slPoints * _Point / tickSize) * tickVal;
      if(slMoney <= 0) return 0;
      lots = riskMoney / slMoney;
   }
   else if(_LotMode == 2)
   {
      int tiers = (int)MathFloor(AccountInfoDouble(ACCOUNT_BALANCE) / TierBase);
      lots = MathMax(1, tiers) * LotStep;
   }
   double minL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if(step <= 0) step = 0.01;
   lots = MathMax(minL, MathMin(maxL, lots));
   lots = MathRound(lots / step) * step;
   return lots;
}

//====================================================================
// Magic-scoped position / order / deal queries
//====================================================================
int CountPositionsByMagicComment(int magic, const string comment)
{
   int count = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0 || !PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != magic) continue;
      if(comment != "" && PositionGetString(POSITION_COMMENT) != comment) continue;
      count++;
   }
   return count;
}

bool HasPendingByMagicComment(int magic, const string comment)
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0 || !OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != magic) continue;
      if(comment != "" && OrderGetString(ORDER_COMMENT) != comment) continue;
      return true;
   }
   return false;
}

void CancelPendingByMagic(int magic)
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0 || !OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != magic) continue;
      g_trade.OrderDelete(t);
   }
}

void ClosePositionsByMagic(int magic)
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0 || !PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != magic) continue;
      g_trade.PositionClose(t);
   }
}

double SumRealizedTodayByMagic(int magic, datetime dayStart)
{
   double total = 0.0;
   if(!HistorySelect(dayStart, TimeCurrent())) return 0.0;
   for(int i = HistoryDealsTotal() - 1; i >= 0; i--)
   {
      ulong tk = HistoryDealGetTicket(i);
      if(tk == 0) continue;
      if(HistoryDealGetString(tk, DEAL_SYMBOL) != _Symbol) continue;
      if((int)HistoryDealGetInteger(tk, DEAL_MAGIC) != magic) continue;
      if(HistoryDealGetInteger(tk, DEAL_ENTRY) != DEAL_ENTRY_OUT) continue;
      total += HistoryDealGetDouble(tk, DEAL_PROFIT);
      total += HistoryDealGetDouble(tk, DEAL_SWAP);
      total += HistoryDealGetDouble(tk, DEAL_COMMISSION);
   }
   return total;
}

double SumUnrealizedByMagic(int magic)
{
   double total = 0.0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong tk = PositionGetTicket(i);
      if(tk == 0 || !PositionSelectByTicket(tk)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != magic) continue;
      total += PositionGetDouble(POSITION_PROFIT);
      total += PositionGetDouble(POSITION_SWAP);
   }
   return total;
}

//====================================================================
// Spread sanity guard. Returns true if quoted spread is within tolerance.
// Called at the top of each ProcessXxx() to skip new orders when broker
// quote feed shows abnormally wide spread (Sunday open glitch, holiday,
// quote-feed disruption). Vantage XAUUSD max-observed = 32pt; 50pt = ~56% margin.
//====================================================================
bool SpreadOK()
{
   if(_MaxSpreadPts <= 0) return true;
   long spread_pts = SymbolInfoInteger(_Symbol, SYMBOL_SPREAD);
   return (spread_pts <= _MaxSpreadPts);
}

//====================================================================
// Generic per-stream daily cap check.
// Returns true when stream is locked for the day. Closes own positions
// and cancels own pendings on lock fire.
//====================================================================
bool StreamDailyCapsCheck(int magic, double targetPct, double lossPct,
                          datetime &dayState, double &balStart, bool &lockState)
{
   datetime today = DayStart(TimeCurrent());
   if(today != dayState)
   {
      dayState = today;
      balStart = AccountInfoDouble(ACCOUNT_BALANCE);
      lockState = false;
   }
   if(lockState) return true;
   if(targetPct <= 0 && lossPct <= 0) return false;
   if(balStart <= 0) return false;

   double pnl = SumRealizedTodayByMagic(magic, today) + SumUnrealizedByMagic(magic);

   if(targetPct > 0 && pnl >= balStart * targetPct / 100.0)
   {
      ClosePositionsByMagic(magic);
      CancelPendingByMagic(magic);
      lockState = true;
      PrintFormat("[DT818_pro] Magic %d daily TARGET hit at $%.2f (+%.2f%%), locking.",
                  magic, pnl, pnl / balStart * 100.0);
      return true;
   }
   if(lossPct > 0 && pnl <= -balStart * lossPct / 100.0)
   {
      ClosePositionsByMagic(magic);
      CancelPendingByMagic(magic);
      lockState = true;
      PrintFormat("[DT818_pro] Magic %d daily LOSS hit at $%.2f (%.2f%%), locking.",
                  magic, pnl, pnl / balStart * 100.0);
      return true;
   }
   return false;
}

//====================================================================
// ORB stream processor (parameterized for LDN + NY)
//====================================================================
void InitORBSession(ORBSession &s, const ORBStreamCfg &cfg, int startHourBroker, datetime now_broker)
{
   MqlDateTime mt; TimeToStruct(now_broker, mt);
   mt.hour = startHourBroker; mt.min = 0; mt.sec = 0;
   s.range_start = StructToTime(mt);
   s.range_end   = s.range_start + cfg.range_minutes * 60;
   s.pending_expire = s.range_end + cfg.pending_expire_minutes * 60;
   s.active = true;
   s.fired = false;
   s.fired_buy  = false;
   s.fired_sell = false;
   s.range_high = 0;
   s.range_low  = 0;
}

//====================================================================
// V5: M5 fractal confirmation helper.
// Returns true if any confirmed up-fractal (Bill Williams `width`-bar) exists
// in M5 history with high > break_level AND with the candidate bar's open
// timestamp >= range_end_utc. Mirrored for down-fractal.
//
// Fractal definition: at candidate bar index `i` (oldest bars have higher
// indices in MQL5 series), high[i] is an up-fractal iff strictly greater
// than high[i +/- 1..half]. Confirmation requires `half = width/2` bars on
// each side to be FULLY CLOSED.  Bar[0] is the currently-forming bar so the
// newest closed bar is bar[1]; the earliest valid candidate index is
// (half + 1) (e.g. bar[3] for width=5, bar[2] for width=3).
//====================================================================
bool FractalConfirmedAbove(double break_level, datetime range_end_utc, int width)
{
   if(width < 3 || (width % 2) == 0) return false;
   int half = width / 2;
   long broker_offset_s = (long)TimeCurrent() - (long)TimeGMT();
   for(int i = half + 1; i < 100; i++)
   {
      // Out of window? (range_end is real UTC; iTime returns broker time)
      datetime ct_broker = iTime(_Symbol, PERIOD_M5, i);
      if(ct_broker == 0) break;
      datetime ct_utc = (datetime)((long)ct_broker - broker_offset_s);
      if(ct_utc < range_end_utc) break;   // older bars also out of window
      double ch = iHigh(_Symbol, PERIOD_M5, i);
      if(ch <= break_level) continue;     // even if a fractal, doesn't beat the level
      bool is_fractal = true;
      for(int k = 1; k <= half; k++)
      {
         if(iHigh(_Symbol, PERIOD_M5, i - k) >= ch ||
            iHigh(_Symbol, PERIOD_M5, i + k) >= ch)
         { is_fractal = false; break; }
      }
      if(is_fractal) return true;
   }
   return false;
}

bool FractalConfirmedBelow(double break_level, datetime range_end_utc, int width)
{
   if(width < 3 || (width % 2) == 0) return false;
   int half = width / 2;
   long broker_offset_s = (long)TimeCurrent() - (long)TimeGMT();
   for(int i = half + 1; i < 100; i++)
   {
      datetime ct_broker = iTime(_Symbol, PERIOD_M5, i);
      if(ct_broker == 0) break;
      datetime ct_utc = (datetime)((long)ct_broker - broker_offset_s);
      if(ct_utc < range_end_utc) break;
      double cl = iLow(_Symbol, PERIOD_M5, i);
      if(cl >= break_level) continue;
      bool is_fractal = true;
      for(int k = 1; k <= half; k++)
      {
         if(iLow(_Symbol, PERIOD_M5, i - k) <= cl ||
            iLow(_Symbol, PERIOD_M5, i + k) <= cl)
         { is_fractal = false; break; }
      }
      if(is_fractal) return true;
   }
   return false;
}

void UpdateORBSession(ORBSession &s, const ORBStreamCfg &cfg, datetime now)
{
   if(!s.active) return;
   MqlDateTime mt_now, mt_range;
   TimeToStruct(now, mt_now);
   TimeToStruct(s.range_start, mt_range);
   if(mt_now.day != mt_range.day || mt_now.mon != mt_range.mon)
   {
      s.active = false;
      return;
   }

   if(now < s.range_end)
   {
      // v2.1 fix: iTime() returns broker-time epoch (Vantage at UTC+3).
      // range_start was built from TimeGMT() (real UTC).
      // Convert iTime to real UTC by subtracting broker offset, then compare.
      // broker_offset_s = TimeCurrent() - TimeGMT() (positive if broker ahead of GMT)
      long broker_offset_s = (long)TimeCurrent() - (long)TimeGMT();
      for(int i = 0; i < 50; i++)
      {
         datetime bt_broker = iTime(_Symbol, _ORB_EntryTF, i);
         datetime bt = (datetime)((long)bt_broker - broker_offset_s);  // bar open in real UTC
         if(bt < s.range_start) break;
         if(bt >= s.range_end) continue;
         double h = iHigh(_Symbol, _ORB_EntryTF, i);
         double l = iLow(_Symbol, _ORB_EntryTF, i);
         if(s.range_high == 0 || h > s.range_high) s.range_high = h;
         if(s.range_low  == 0 || l < s.range_low)  s.range_low  = l;
      }
      return;
   }

   if(!(s.fired_buy && s.fired_sell) && s.range_high > 0 && s.range_low > 0)
   {
      double range_pts = (s.range_high - s.range_low) / _Point;
      if(range_pts < _ORB_MinRangePts || range_pts > _ORB_MaxRangePts)
      { s.fired_buy = true; s.fired_sell = true; s.fired = true; return; }
      double sl_dist_pts = (cfg.fixed_sl_pts > 0) ? cfg.fixed_sl_pts : range_pts;
      double tp_dist_pts = sl_dist_pts * cfg.rr_ratio;
      double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
      double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);

      double totalLots = CalcLots(sl_dist_pts);
      if(totalLots <= 0) { s.fired_buy = true; s.fired_sell = true; s.fired = true; return; }
      double halfLots = totalLots;
      if(cfg.half_tp_ratio > 0)
      {
         double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
         double minL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
         if(step <= 0) step = 0.01;
         halfLots = MathRound(totalLots / 2.0 / step) * step;
         if(halfLots < minL) halfLots = minL;
      }

      g_trade.SetExpertMagicNumber(cfg.magic);

      // v2.1 fix #2: MT5 server stores pending expiration in broker-time scale,
      // but s.pending_expire was built from TimeGMT() (real UTC). Passing real-UTC
      // epoch to MT5 causes pendings to expire 3h EARLIER than intended (because
      // server compares its broker_clock to our real_UTC epoch, which is 3h
      // smaller). Convert to broker scale before passing.
      long _broker_offset_s = (long)TimeCurrent() - (long)TimeGMT();
      datetime expire_broker = (datetime)((long)s.pending_expire + _broker_offset_s);

      // v5: fractal-confirm gate (GLOBAL — applies to all streams). When
      // _ORB_FractalConfirm=true, BUY only arms after an M5 up-fractal confirms
      // with high > range_high (in [range_end, now]); SELL mirrored. Each side
      // independent — one side firing doesn't gate the other.
      bool gate_buy_ok  = !_ORB_FractalConfirm || FractalConfirmedAbove(s.range_high, s.range_end, _ORB_FractalWidth);
      bool gate_sell_ok = !_ORB_FractalConfirm || FractalConfirmedBelow(s.range_low,  s.range_end, _ORB_FractalWidth);

      if(!s.fired_buy && gate_buy_ok)
      {
         double buyEntry = NormPrice(s.range_high);   // buffer removed 2026-05-19
         double minBuy   = NormPrice(ask + stopsLvl * _Point);
         if(buyEntry < minBuy) buyEntry = minBuy;
         if(buyEntry > ask)
         {
            double sl = NormPrice(buyEntry - sl_dist_pts * _Point);
            double tp = NormPrice(buyEntry + tp_dist_pts * _Point);
            if(cfg.half_tp_ratio > 0)
            {
               double tpHalf = NormPrice(buyEntry + tp_dist_pts * cfg.half_tp_ratio * _Point);
               g_trade.BuyStop(halfLots, buyEntry, _Symbol, sl, tpHalf,
                               ORDER_TIME_SPECIFIED, expire_broker, cfg.comment);
               g_trade.BuyStop(halfLots, buyEntry, _Symbol, sl, tp,
                               ORDER_TIME_SPECIFIED, expire_broker, cfg.comment);
            }
            else
               g_trade.BuyStop(totalLots, buyEntry, _Symbol, sl, tp,
                               ORDER_TIME_SPECIFIED, expire_broker, cfg.comment);
         }
         s.fired_buy = true;
      }
      if(!s.fired_sell && gate_sell_ok)
      {
         double sellEntry = NormPrice(s.range_low);   // buffer removed 2026-05-19
         double maxSell   = NormPrice(bid - stopsLvl * _Point);
         if(sellEntry > maxSell) sellEntry = maxSell;
         if(sellEntry < bid)
         {
            double sl = NormPrice(sellEntry + sl_dist_pts * _Point);
            double tp = NormPrice(sellEntry - tp_dist_pts * _Point);
            if(cfg.half_tp_ratio > 0)
            {
               double tpHalf = NormPrice(sellEntry - tp_dist_pts * cfg.half_tp_ratio * _Point);
               g_trade.SellStop(halfLots, sellEntry, _Symbol, sl, tpHalf,
                                ORDER_TIME_SPECIFIED, expire_broker, cfg.comment);
               g_trade.SellStop(halfLots, sellEntry, _Symbol, sl, tp,
                                ORDER_TIME_SPECIFIED, expire_broker, cfg.comment);
            }
            else
               g_trade.SellStop(totalLots, sellEntry, _Symbol, sl, tp,
                                ORDER_TIME_SPECIFIED, expire_broker, cfg.comment);
         }
         s.fired_sell = true;
      }
      if(s.fired_buy && s.fired_sell) s.fired = true;
   }

   if(now >= s.pending_expire)
   {
      CancelPendingByMagic(cfg.magic);
      s.active = false;
   }
}

void ProcessORBStream(const ORBStreamCfg &cfg,
                       ORBSession &ldn_state, ORBSession &ny_state)
{
   if(!cfg.enabled) return;
   if(!SpreadOK()) return;
   // Use TimeGMT() so session timing is broker-tz-independent.
   // _ORB_LDN_StartHour / _ORB_NY_StartHour are UTC; no offset applied.
   // _BrokerGMTOffsetHours is retained as input for backward setfile
   // compatibility but is no longer used for session detection.
   // Bug fix 2026-05-07: previously TimeCurrent() (broker time) was used
   // with offset added; this caused 3hr-late firing whenever broker server
   // ran on UTC instead of GMT+3.
   datetime now = TimeGMT();
   MqlDateTime mt; TimeToStruct(now, mt);

   if(_ORB_LDN_Enabled)
   {
      int ldn_hour = _ORB_LDN_StartHour;
      if(!ldn_state.active && mt.hour == ldn_hour && mt.min < 5)
         InitORBSession(ldn_state, cfg, ldn_hour, now);
      UpdateORBSession(ldn_state, cfg, now);
   }
   if(_ORB_NY_Enabled)
   {
      int ny_hour = _ORB_NY_StartHour;
      if(!ny_state.active && mt.hour == ny_hour && mt.min < 5)
         InitORBSession(ny_state, cfg, ny_hour, now);
      UpdateORBSession(ny_state, cfg, now);
   }
}

//====================================================================
// ProcessHedgeStream — REVERSE hedge: opposite-direction LIMIT at parent's
// original entry, with mirrored SL/TP.
//
// Logic: when parent SL hits, look up parent's IN deal to find the original
// pending order. Place a NEW pending of the OPPOSITE direction (LIMIT) at
// the SAME entry price. Hedge SL/TP are MIRRORED across the entry from
// parent's SL/TP. Bet: after parent SL, price will retrace to entry then
// continue in the SL direction (reversion-continuation play).
//
//   Parent BUY  SL'd → SELL_LIMIT at parent_entry (above current price since
//                       BUY SL'd below entry → LIMIT valid above market)
//                       hedge SL = entry + sl_dist
//                       hedge TP = entry - tp_mult*sl_dist
//   Parent SELL SL'd → BUY_LIMIT  at parent_entry (below current price since
//                       SELL SL'd above entry → LIMIT valid below market)
//                       hedge SL = entry - sl_dist
//                       hedge TP = entry + tp_mult*sl_dist
//
// F1 filter (MaxSecondsAfterEntry): skip hedge if parent SL hit too LATE
// after entry. Fast SL = whipsaw (good reversion-continuation candidate).
// Slow SL = grinding trend (price already moved far → less likely to retrace
// all the way back to entry within expire window).
//
// Apply v2.1 FIX 2: pending expire passed to MT5 must be in broker scale.
//====================================================================
void ProcessHedgeStream(HedgeStreamCfg &hcfg)
{
   if(!hcfg.enabled) return;
   if(!SpreadOK()) return;

   datetime now_broker = TimeCurrent();
   datetime scan_from = (g_hedge_last_scan > 0) ? g_hedge_last_scan - 60 : now_broker - 3600;
   if(!HistorySelect(scan_from, now_broker)) return;

   // PASS 1: collect candidate OUT-deal tickets from the time-range selection.
   // We snapshot them so PASS 2 can call HistorySelectByPosition() (which replaces
   // the current selection) without invalidating outer-loop iteration.
   //
   // 2026-05-18 fix: was a single combined loop that nested HistorySelect-by-time
   // (outer) with same-collection IN-deal lookup (inner). The inner lookup failed
   // for every SL today ("no IN deal found"), because either the IN deal sits just
   // outside the time-range scan window or DEAL_POSITION_ID lookup is unreliable
   // across same-tick IN/OUT pairs on hedging-mode accounts. HistorySelectByPosition
   // is the canonical way to enumerate a position's deals; using it removes the
   // ordering/window race entirely.
   int total = HistoryDealsTotal();
   ulong out_tickets[];
   int out_count = 0;
   for(int i = 0; i < total; i++)
   {
      ulong dt = HistoryDealGetTicket(i);
      if(dt == 0) continue;
      long magic = HistoryDealGetInteger(dt, DEAL_MAGIC);
      if((int)magic != hcfg.parent_magic) continue;
      long entry_kind = HistoryDealGetInteger(dt, DEAL_ENTRY);
      if(entry_kind != DEAL_ENTRY_OUT) continue;
      string sym = HistoryDealGetString(dt, DEAL_SYMBOL);
      if(sym != _Symbol) continue;
      if(HedgeWasSeen(dt)) continue;
      string comment = HistoryDealGetString(dt, DEAL_COMMENT);
      if(StringFind(comment, "[sl") != 0) continue;
      ArrayResize(out_tickets, out_count + 1);
      out_tickets[out_count++] = dt;
   }

   // PASS 2: process each candidate. HistorySelectByPosition replaces the active
   // selection, so we re-select per position. Outer-loop state is the cached array.
   //
   // CRITICAL: after the first iteration's HistorySelectByPosition replaces the
   // active selection, dt (cached from pass 1) is no longer in scope for
   // HistoryDealGet* calls. HistoryDealSelect(dt) explicitly re-selects the
   // single deal by ticket so its properties remain queryable, independent of
   // any prior HistorySelect / HistorySelectByPosition state.
   for(int k = 0; k < out_count; k++)
   {
      ulong dt = out_tickets[k];

      if(!HistoryDealSelect(dt))
      {
         HedgeMarkSeen(dt);
         PrintFormat("[%s] hedge SKIPPED: HistoryDealSelect failed for OUT %I64u",
                     hcfg.comment, dt);
         continue;
      }

      // Re-select by position to enumerate this position's deals reliably.
      long pos_id = HistoryDealGetInteger(dt, DEAL_POSITION_ID);
      if(pos_id == 0)
      {
         HedgeMarkSeen(dt);
         PrintFormat("[%s] hedge SKIPPED: OUT deal %I64u has DEAL_POSITION_ID=0",
                     hcfg.comment, dt);
         continue;
      }
      if(!HistorySelectByPosition(pos_id))
      {
         HedgeMarkSeen(dt);
         PrintFormat("[%s] hedge SKIPPED: HistorySelectByPosition failed for %I64d",
                     hcfg.comment, pos_id);
         continue;
      }
      int pos_deals = HistoryDealsTotal();
      ulong in_deal = 0;
      datetime in_time = 0;
      for(int j = 0; j < pos_deals; j++)
      {
         ulong et = HistoryDealGetTicket(j);
         if(et == 0) continue;
         if(HistoryDealGetInteger(et, DEAL_ENTRY) == DEAL_ENTRY_IN)
         {
            in_deal = et;
            in_time = (datetime)HistoryDealGetInteger(et, DEAL_TIME);
            break;
         }
      }
      if(in_deal == 0)
      {
         HedgeMarkSeen(dt);
         PrintFormat("[%s] hedge SKIPPED: no IN deal found for position %I64d (pos_deals=%d)",
                     hcfg.comment, pos_id, pos_deals);
         continue;
      }

      // F1 filter
      if(hcfg.max_seconds_after_entry > 0)
      {
         datetime sl_time = (datetime)HistoryDealGetInteger(dt, DEAL_TIME);
         if((sl_time - in_time) > hcfg.max_seconds_after_entry)
         {
            HedgeMarkSeen(dt);
            PrintFormat("[%s] hedge SKIPPED (F1 filter: SL %ds after entry > %ds cutoff)",
                        hcfg.comment, (int)(sl_time - in_time),
                        hcfg.max_seconds_after_entry);
            continue;
         }
      }

      // Mark seen FIRST so we don't retry on transient errors
      HedgeMarkSeen(dt);

      // Look up the parent's ORIGINAL PENDING order via IN deal's DEAL_ORDER.
      ulong orig_order = (ulong)HistoryDealGetInteger(in_deal, DEAL_ORDER);
      if(orig_order == 0)
      {
         PrintFormat("[%s] hedge SKIPPED: IN deal has no order ticket", hcfg.comment);
         continue;
      }
      if(!HistoryOrderSelect(orig_order))
      {
         PrintFormat("[%s] hedge SKIPPED: could not select original order %I64u",
                     hcfg.comment, orig_order);
         continue;
      }

      long orig_type = HistoryOrderGetInteger(orig_order, ORDER_TYPE);
      double orig_entry = HistoryOrderGetDouble(orig_order, ORDER_PRICE_OPEN);
      double orig_sl    = HistoryOrderGetDouble(orig_order, ORDER_SL);
      double orig_tp    = HistoryOrderGetDouble(orig_order, ORDER_TP);
      double orig_lots  = HistoryDealGetDouble(in_deal, DEAL_VOLUME);

      // ===== v6 STOP-on-EXTENSION GEOMETRY (2026-05-24) =====
      //
      // Replaces v5 smart-TP LIMIT (which waited for retracement to parent
      // entry) with a STOP placed past the parent SL in the continuation
      // direction. Bet: parent SL was real, price will continue.
      //
      //   Parent BUY  SL'd → SELL_STOP at parent_SL - ExtPts × _Point
      //                       (need price to fall MORE to trigger)
      //                       hedge SL = entry + (parent_SL_dist × SLMult)
      //                                  (above entry; against SELL position)
      //                       hedge TP = entry - (ExtPts × TPMult × _Point)
      //                                  (below entry; in continuation direction)
      //
      //   Parent SELL SL'd → BUY_STOP at parent_SL + ExtPts × _Point
      //                       (need price to rise MORE to trigger)
      //                       hedge SL = entry - (parent_SL_dist × SLMult)
      //                       hedge TP = entry + (ExtPts × TPMult × _Point)
      //
      // Risk-equalized lots = orig_lots / SLMult (single position, no split).
      //
      // Sim winner (ac9ec64, may23 windows): ExtPts=100, TPMult=3.0, SLMult=1.0
      //   OOS NP $6,497  WR 75.8%  consistent across all 4 OOS folds.
      double sl_dist = MathAbs(orig_entry - orig_sl);  // parent_SL distance in price units
      double hedge_sl_dist = sl_dist * hcfg.sl_mult;
      double ext_price = hcfg.ext_pts * _Point;
      double tp_dist_price = ext_price * hcfg.tp_mult;

      double hedge_entry, hedge_sl, hedge_tp;
      ENUM_ORDER_TYPE hedge_type;
      if(orig_type == ORDER_TYPE_BUY_STOP)
      {
         // Parent was BUY → continuation is DOWN → SELL_STOP below parent SL
         hedge_type = ORDER_TYPE_SELL_STOP;
         hedge_entry = NormPrice(orig_sl - ext_price);          // below parent SL (chase down)
         hedge_sl    = NormPrice(hedge_entry + hedge_sl_dist);  // above entry (SELL SL)
         hedge_tp    = NormPrice(hedge_entry - tp_dist_price);  // below entry (continuation TP)
      }
      else if(orig_type == ORDER_TYPE_SELL_STOP)
      {
         // Parent was SELL → continuation is UP → BUY_STOP above parent SL
         hedge_type = ORDER_TYPE_BUY_STOP;
         hedge_entry = NormPrice(orig_sl + ext_price);          // above parent SL (chase up)
         hedge_sl    = NormPrice(hedge_entry - hedge_sl_dist);  // below entry (BUY SL)
         hedge_tp    = NormPrice(hedge_entry + tp_dist_price);  // above entry (continuation TP)
      }
      else
      {
         PrintFormat("[%s] stop-ext-hedge SKIPPED: parent order type %d not stop",
                     hcfg.comment, (int)orig_type);
         continue;
      }

      // Sanity: STOP needs to be on the correct side of current price.
      //   SELL_STOP must be BELOW current bid (we'll sell when price falls to entry).
      //   BUY_STOP  must be ABOVE current ask (we'll buy when price rises to entry).
      double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
      int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);
      double pt = _Point;
      if(hedge_type == ORDER_TYPE_SELL_STOP)
      {
         double maxSellStop = NormPrice(bid - stopsLvl * pt);
         if(hedge_entry > maxSellStop)
         {
            PrintFormat("[%s] stop-ext-hedge SKIPPED: SELL_STOP entry %.5f above maxSellStop %.5f (stops level)",
                        hcfg.comment, hedge_entry, maxSellStop);
            continue;
         }
         if(hedge_entry >= bid)
         {
            PrintFormat("[%s] stop-ext-hedge SKIPPED: SELL_STOP entry %.5f >= bid %.5f (already triggered)",
                        hcfg.comment, hedge_entry, bid);
            continue;
         }
      }
      else  // BUY_STOP
      {
         double minBuyStop = NormPrice(ask + stopsLvl * pt);
         if(hedge_entry < minBuyStop)
         {
            PrintFormat("[%s] stop-ext-hedge SKIPPED: BUY_STOP entry %.5f below minBuyStop %.5f (stops level)",
                        hcfg.comment, hedge_entry, minBuyStop);
            continue;
         }
         if(hedge_entry <= ask)
         {
            PrintFormat("[%s] stop-ext-hedge SKIPPED: BUY_STOP entry %.5f <= ask %.5f (already triggered)",
                        hcfg.comment, hedge_entry, ask);
            continue;
         }
      }

      // Lots: risk-equalized = orig_lots / sl_mult. Single position.
      double vstep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
      double vmin  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
      double vmax  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
      if(vstep <= 0) vstep = 0.01;
      double sl_mult_safe = (hcfg.sl_mult > 0) ? hcfg.sl_mult : 1.0;
      double lots_raw = orig_lots / sl_mult_safe;
      double lots = MathRound(lots_raw / vstep) * vstep;
      if(lots < vmin)
      {
         PrintFormat("[%s] stop-ext-hedge SKIPPED: lots %.3f below vmin %.3f",
                     hcfg.comment, lots, vmin);
         continue;
      }
      if(lots > vmax) lots = vmax;

      // Pending expire in broker scale (v2.1 FIX 2 still applies).
      long broker_offset_s = (long)TimeCurrent() - (long)TimeGMT();
      datetime expire_realutc = TimeGMT() + hcfg.expire_minutes * 60;
      datetime expire_broker  = (datetime)((long)expire_realutc + broker_offset_s);

      g_trade.SetExpertMagicNumber(hcfg.magic);
      bool ok = false;
      if(hedge_type == ORDER_TYPE_BUY_STOP)
         ok = g_trade.BuyStop(lots, hedge_entry, _Symbol, hedge_sl, hedge_tp,
                              ORDER_TIME_SPECIFIED, expire_broker, hcfg.comment);
      else
         ok = g_trade.SellStop(lots, hedge_entry, _Symbol, hedge_sl, hedge_tp,
                               ORDER_TIME_SPECIFIED, expire_broker, hcfg.comment);
      if(!ok)
      {
         PrintFormat("[%s] stop-ext-hedge FAILED: %s",
                     hcfg.comment, g_trade.ResultRetcodeDescription());
         continue;
      }
      PrintFormat("[%s] stop-ext-hedge %s placed entry=%.5f sl=%.5f tp=%.5f lots=%.2f "
                  "(parent pos_id=%I64d orig_order=%I64u parent_sl_pts=%.0f ext_pts=%d sl_mult=%.2f tp_mult=%.2f)",
                  hcfg.comment,
                  (hedge_type == ORDER_TYPE_BUY_STOP ? "BUY_STOP" : "SELL_STOP"),
                  hedge_entry, hedge_sl, hedge_tp, lots,
                  pos_id, orig_order, sl_dist / pt, hcfg.ext_pts, hcfg.sl_mult, hcfg.tp_mult);
   }
   g_hedge_last_scan = now_broker;
}

//====================================================================
// OnInit
//====================================================================
int OnInit()
{
   g_trade.SetExpertMagicNumber(_ORB_S1_Magic);
   g_trade.SetDeviationInPoints(50);

   // Build ORB stream cfgs from inputs
   g_cfg_s1.enabled                = _ORB_S1_Enabled;
   g_cfg_s1.magic                  = _ORB_S1_Magic;
   g_cfg_s1.comment                = _ORB_S1_Comment;
   g_cfg_s1.range_minutes          = _ORB_S1_RangeMinutes;
   g_cfg_s1.fixed_sl_pts           = _ORB_S1_FixedSL_Pts;
   g_cfg_s1.rr_ratio               = _ORB_S1_RR_Ratio;
   g_cfg_s1.half_tp_ratio          = _ORB_S1_HalfTP_Ratio;
   g_cfg_s1.pending_expire_minutes = _ORB_S1_PendingExpireMinutes;
   g_cfg_s1.daily_target_pct       = _ORB_S1_DailyTargetPct;
   g_cfg_s1.daily_loss_pct         = _ORB_S1_DailyLossPct;

   g_cfg_s2.enabled                = _ORB_S2_Enabled;
   g_cfg_s2.magic                  = _ORB_S2_Magic;
   g_cfg_s2.comment                = _ORB_S2_Comment;
   g_cfg_s2.range_minutes          = _ORB_S2_RangeMinutes;
   g_cfg_s2.fixed_sl_pts           = _ORB_S2_FixedSL_Pts;
   g_cfg_s2.rr_ratio               = _ORB_S2_RR_Ratio;
   g_cfg_s2.half_tp_ratio          = _ORB_S2_HalfTP_Ratio;
   g_cfg_s2.pending_expire_minutes = _ORB_S2_PendingExpireMinutes;
   g_cfg_s2.daily_target_pct       = _ORB_S2_DailyTargetPct;
   g_cfg_s2.daily_loss_pct         = _ORB_S2_DailyLossPct;

   g_cfg_s3.enabled                = _ORB_S3_Enabled;
   g_cfg_s3.magic                  = _ORB_S3_Magic;
   g_cfg_s3.comment                = _ORB_S3_Comment;
   g_cfg_s3.range_minutes          = _ORB_S3_RangeMinutes;
   g_cfg_s3.fixed_sl_pts           = _ORB_S3_FixedSL_Pts;
   g_cfg_s3.rr_ratio               = _ORB_S3_RR_Ratio;
   g_cfg_s3.half_tp_ratio          = _ORB_S3_HalfTP_Ratio;
   g_cfg_s3.pending_expire_minutes = _ORB_S3_PendingExpireMinutes;
   g_cfg_s3.daily_target_pct       = _ORB_S3_DailyTargetPct;
   g_cfg_s3.daily_loss_pct         = _ORB_S3_DailyLossPct;

   g_cfg_s4.enabled                = _ORB_S4_Enabled;
   g_cfg_s4.magic                  = _ORB_S4_Magic;
   g_cfg_s4.comment                = _ORB_S4_Comment;
   g_cfg_s4.range_minutes          = _ORB_S4_RangeMinutes;
   g_cfg_s4.fixed_sl_pts           = _ORB_S4_FixedSL_Pts;
   g_cfg_s4.rr_ratio               = _ORB_S4_RR_Ratio;
   g_cfg_s4.half_tp_ratio          = _ORB_S4_HalfTP_Ratio;
   g_cfg_s4.pending_expire_minutes = _ORB_S4_PendingExpireMinutes;
   g_cfg_s4.daily_target_pct       = _ORB_S4_DailyTargetPct;
   g_cfg_s4.daily_loss_pct         = _ORB_S4_DailyLossPct;

   g_cfg_s5.enabled                = _ORB_S5_Enabled;
   g_cfg_s5.magic                  = _ORB_S5_Magic;
   g_cfg_s5.comment                = _ORB_S5_Comment;
   g_cfg_s5.range_minutes          = _ORB_S5_RangeMinutes;
   g_cfg_s5.fixed_sl_pts           = _ORB_S5_FixedSL_Pts;
   g_cfg_s5.rr_ratio               = _ORB_S5_RR_Ratio;
   g_cfg_s5.half_tp_ratio          = _ORB_S5_HalfTP_Ratio;
   g_cfg_s5.pending_expire_minutes = _ORB_S5_PendingExpireMinutes;
   g_cfg_s5.daily_target_pct       = _ORB_S5_DailyTargetPct;
   g_cfg_s5.daily_loss_pct         = _ORB_S5_DailyLossPct;

   g_cfg_s6.enabled                = _ORB_S6_Enabled;
   g_cfg_s6.magic                  = _ORB_S6_Magic;
   g_cfg_s6.comment                = _ORB_S6_Comment;
   g_cfg_s6.range_minutes          = _ORB_S6_RangeMinutes;
   g_cfg_s6.fixed_sl_pts           = _ORB_S6_FixedSL_Pts;
   g_cfg_s6.rr_ratio               = _ORB_S6_RR_Ratio;
   g_cfg_s6.half_tp_ratio          = _ORB_S6_HalfTP_Ratio;
   g_cfg_s6.pending_expire_minutes = _ORB_S6_PendingExpireMinutes;
   g_cfg_s6.daily_target_pct       = _ORB_S6_DailyTargetPct;
   g_cfg_s6.daily_loss_pct         = _ORB_S6_DailyLossPct;

   // Build hedge stream cfgs
   #define BUILD_HEDGE_CFG(N) \
      g_hedge_s##N.enabled                = _HEDGE_S##N##_Enabled; \
      g_hedge_s##N.magic                  = _HEDGE_S##N##_Magic; \
      g_hedge_s##N.comment                = _HEDGE_S##N##_Comment; \
      g_hedge_s##N.parent_magic           = _HEDGE_S##N##_ParentMagic; \
      g_hedge_s##N.fixed_sl_pts           = _HEDGE_S##N##_FixedSL_Pts; \
      g_hedge_s##N.rr_ratio               = _HEDGE_S##N##_RR_Ratio; \
      g_hedge_s##N.expire_minutes         = _HEDGE_S##N##_ExpireMinutes; \
      g_hedge_s##N.max_seconds_after_entry= _HEDGE_S##N##_MaxSecondsAfterEntry; \
      g_hedge_s##N.sl_mult                = _HEDGE_S##N##_SLMult; \
      g_hedge_s##N.ext_pts                = _HEDGE_S##N##_ExtPts; \
      g_hedge_s##N.tp_mult                = _HEDGE_S##N##_TPMult; \
      g_hedge_s##N.partial_fraction       = _HEDGE_S##N##_PartialFraction; \
      g_hedge_s##N.profit_mult            = _HEDGE_S##N##_ProfitMult
   BUILD_HEDGE_CFG(1);
   BUILD_HEDGE_CFG(2);
   BUILD_HEDGE_CFG(3);
   BUILD_HEDGE_CFG(4);
   BUILD_HEDGE_CFG(5);
   BUILD_HEDGE_CFG(6);
   #undef BUILD_HEDGE_CFG

   ArrayInitialize(g_hedge_seen_tickets, 0);
   g_hedge_seen_idx = 0;
   g_hedge_last_scan = 0;

   g_lastBar_M1 = iTime(_Symbol, PERIOD_M1, 0);

   double bal = AccountInfoDouble(ACCOUNT_BALANCE);
   g_s1_bal_start = bal;
   g_s2_bal_start = bal;
   g_s3_bal_start = bal;
   g_s4_bal_start = bal;
   g_s5_bal_start = bal;
   g_s6_bal_start = bal;

   long broker_off = (long)TimeCurrent() - (long)TimeGMT();
   PrintFormat("[DT818_pro_v6] Init. Parents S1=%d S2=%d S3=%d S4=%d S5=%d S6=%d  "
               "STOP-ext Hedges H1=%d H2=%d H3=%d H4=%d H5=%d H6=%d  "
               "Sessions LDN_hr=%d NY_hr=%d  FractalConfirm=%s width=%d  "
               "(real UTC, broker=%+.1fh vs UTC; hedge type=STOP-on-extension)",
               _ORB_S1_Enabled, _ORB_S2_Enabled, _ORB_S3_Enabled,
               _ORB_S4_Enabled, _ORB_S5_Enabled, _ORB_S6_Enabled,
               _HEDGE_S1_Enabled, _HEDGE_S2_Enabled, _HEDGE_S3_Enabled,
               _HEDGE_S4_Enabled, _HEDGE_S5_Enabled, _HEDGE_S6_Enabled,
               _ORB_LDN_StartHour, _ORB_NY_StartHour,
               (_ORB_FractalConfirm ? "true" : "false"), _ORB_FractalWidth,
               broker_off / 3600.0);
   // Per-stream parent params — confirms which setfile is actually loaded
   PrintFormat("[DT818_pro_v6] Parents: "
               "S1[cm=%s SL=%d RR=%.1f HTP=%.2f Exp=%d]  "
               "S2[cm=%s SL=%d RR=%.1f HTP=%.2f Exp=%d]  "
               "S3[cm=%s SL=%d RR=%.1f HTP=%.2f Exp=%d]",
               _ORB_S1_Comment, _ORB_S1_FixedSL_Pts, _ORB_S1_RR_Ratio, _ORB_S1_HalfTP_Ratio, _ORB_S1_PendingExpireMinutes,
               _ORB_S2_Comment, _ORB_S2_FixedSL_Pts, _ORB_S2_RR_Ratio, _ORB_S2_HalfTP_Ratio, _ORB_S2_PendingExpireMinutes,
               _ORB_S3_Comment, _ORB_S3_FixedSL_Pts, _ORB_S3_RR_Ratio, _ORB_S3_HalfTP_Ratio, _ORB_S3_PendingExpireMinutes);
   PrintFormat("[DT818_pro_v6] Parents: "
               "S4[cm=%s SL=%d RR=%.1f HTP=%.2f Exp=%d]  "
               "S5[cm=%s SL=%d RR=%.1f HTP=%.2f Exp=%d]  "
               "S6[cm=%s SL=%d RR=%.1f HTP=%.2f Exp=%d]",
               _ORB_S4_Comment, _ORB_S4_FixedSL_Pts, _ORB_S4_RR_Ratio, _ORB_S4_HalfTP_Ratio, _ORB_S4_PendingExpireMinutes,
               _ORB_S5_Comment, _ORB_S5_FixedSL_Pts, _ORB_S5_RR_Ratio, _ORB_S5_HalfTP_Ratio, _ORB_S5_PendingExpireMinutes,
               _ORB_S6_Comment, _ORB_S6_FixedSL_Pts, _ORB_S6_RR_Ratio, _ORB_S6_HalfTP_Ratio, _ORB_S6_PendingExpireMinutes);
   // Per-stream hedge params — confirms STOP-ext config
   PrintFormat("[DT818_pro_v6] STOP-ext Hedges: "
               "H1[cm=%s ExtPts=%d TPx%.1f SLx%.1f F1=%ds Exp=%dm]  "
               "H2[cm=%s ExtPts=%d TPx%.1f SLx%.1f F1=%ds Exp=%dm]  "
               "H3[cm=%s ExtPts=%d TPx%.1f SLx%.1f F1=%ds Exp=%dm]",
               _HEDGE_S1_Comment, _HEDGE_S1_ExtPts, _HEDGE_S1_TPMult, _HEDGE_S1_SLMult, _HEDGE_S1_MaxSecondsAfterEntry, _HEDGE_S1_ExpireMinutes,
               _HEDGE_S2_Comment, _HEDGE_S2_ExtPts, _HEDGE_S2_TPMult, _HEDGE_S2_SLMult, _HEDGE_S2_MaxSecondsAfterEntry, _HEDGE_S2_ExpireMinutes,
               _HEDGE_S3_Comment, _HEDGE_S3_ExtPts, _HEDGE_S3_TPMult, _HEDGE_S3_SLMult, _HEDGE_S3_MaxSecondsAfterEntry, _HEDGE_S3_ExpireMinutes);
   PrintFormat("[DT818_pro_v6] STOP-ext Hedges: "
               "H4[cm=%s ExtPts=%d TPx%.1f SLx%.1f F1=%ds Exp=%dm]  "
               "H5[cm=%s ExtPts=%d TPx%.1f SLx%.1f F1=%ds Exp=%dm]  "
               "H6[cm=%s ExtPts=%d TPx%.1f SLx%.1f F1=%ds Exp=%dm]",
               _HEDGE_S4_Comment, _HEDGE_S4_ExtPts, _HEDGE_S4_TPMult, _HEDGE_S4_SLMult, _HEDGE_S4_MaxSecondsAfterEntry, _HEDGE_S4_ExpireMinutes,
               _HEDGE_S5_Comment, _HEDGE_S5_ExtPts, _HEDGE_S5_TPMult, _HEDGE_S5_SLMult, _HEDGE_S5_MaxSecondsAfterEntry, _HEDGE_S5_ExpireMinutes,
               _HEDGE_S6_Comment, _HEDGE_S6_ExtPts, _HEDGE_S6_TPMult, _HEDGE_S6_SLMult, _HEDGE_S6_MaxSecondsAfterEntry, _HEDGE_S6_ExpireMinutes);
   return INIT_SUCCEEDED;
}

//====================================================================
// OnDeinit
//====================================================================
void OnDeinit(const int reason)
{
}

//====================================================================
// OnTick
//====================================================================
void OnTick()
{
   if(_CapitalProtectionAmount > 0 &&
      AccountInfoDouble(ACCOUNT_EQUITY) < _CapitalProtectionAmount)
      return;

   // Per-stream daily-cap checks (each magic tracked independently)
   bool s1_locked = StreamDailyCapsCheck(_ORB_S1_Magic, _ORB_S1_DailyTargetPct,
                                          _ORB_S1_DailyLossPct,
                                          g_s1_day, g_s1_bal_start, g_s1_lock);
   bool s2_locked = StreamDailyCapsCheck(_ORB_S2_Magic, _ORB_S2_DailyTargetPct,
                                          _ORB_S2_DailyLossPct,
                                          g_s2_day, g_s2_bal_start, g_s2_lock);
   bool s3_locked = StreamDailyCapsCheck(_ORB_S3_Magic, _ORB_S3_DailyTargetPct,
                                          _ORB_S3_DailyLossPct,
                                          g_s3_day, g_s3_bal_start, g_s3_lock);
   bool s4_locked = StreamDailyCapsCheck(_ORB_S4_Magic, _ORB_S4_DailyTargetPct,
                                          _ORB_S4_DailyLossPct,
                                          g_s4_day, g_s4_bal_start, g_s4_lock);
   bool s5_locked = StreamDailyCapsCheck(_ORB_S5_Magic, _ORB_S5_DailyTargetPct,
                                          _ORB_S5_DailyLossPct,
                                          g_s5_day, g_s5_bal_start, g_s5_lock);
   bool s6_locked = StreamDailyCapsCheck(_ORB_S6_Magic, _ORB_S6_DailyTargetPct,
                                          _ORB_S6_DailyLossPct,
                                          g_s6_day, g_s6_bal_start, g_s6_lock);

   // Each stream processes both LDN and NY sessions internally.
   if(!s1_locked) ProcessORBStream(g_cfg_s1, g_s1_ldn, g_s1_ny);
   if(!s2_locked) ProcessORBStream(g_cfg_s2, g_s2_ldn, g_s2_ny);
   if(!s3_locked) ProcessORBStream(g_cfg_s3, g_s3_ldn, g_s3_ny);
   if(!s4_locked) ProcessORBStream(g_cfg_s4, g_s4_ldn, g_s4_ny);
   if(!s5_locked) ProcessORBStream(g_cfg_s5, g_s5_ldn, g_s5_ny);
   if(!s6_locked) ProcessORBStream(g_cfg_s6, g_s6_ldn, g_s6_ny);

   // Hedge sub-streams: each watches its parent magic for SL exits and
   // fires opposite-direction LIMIT orders for reversion-continuation.
   ProcessHedgeStream(g_hedge_s1);
   ProcessHedgeStream(g_hedge_s2);
   ProcessHedgeStream(g_hedge_s3);
   ProcessHedgeStream(g_hedge_s4);
   ProcessHedgeStream(g_hedge_s5);
   ProcessHedgeStream(g_hedge_s6);
}
//+------------------------------------------------------------------+

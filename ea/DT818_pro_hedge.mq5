//+------------------------------------------------------------------+
//| DT818_pro_hedge.mq5                                               |
//| ORB EA: 3-stream rank-portfolio (S1 + S2 + S3) + per-stream HEDGE |
//|                                                                   |
//| Variant of DT818_pro.mq5 with hedge sub-streams (HEDGE_S1/S2/S3   |
//| at magics 5111/5222/5333). On each parent SL hit, places opposite-|
//| direction LIMIT order at sl_price ± buffer with own SL/TP/expire. |
//|                                                                   |
//| Parents (unchanged from DT818_pro):                                |
//|   ORB_S1 (magic 1111): rank 1 cfg                                 |
//|   ORB_S2 (magic 2222): rank 2 cfg                                 |
//|   ORB_S3 (magic 3333): rank 3 cfg                                 |
//| Hedges (per-stream WFO winners, sim_wfo_hedge.py May 2):          |
//|   HEDGE_S1 (5111): buf=350 SL=500 RR=4.0 exp=30  (S1 wide-SL fit) |
//|   HEDGE_S2 (5222): buf=100 SL=500 RR=4.0 exp=120                  |
//|   HEDGE_S3 (5333): buf=100 SL=500 RR=4.0 exp=120                  |
//|                                                                   |
//| Risk allocation: parent + hedge each at _RiskPct per stream.      |
//| Combined max-stop exposure = 2 × setfile name (e.g. 3% setfile    |
//| -> 6% if all 3 parents + 3 hedges stop on same day). Phase E      |
//| validates realised DD stays well below max because hedges win on  |
//| days parents lose.                                                 |
//+------------------------------------------------------------------+
#property copyright "ZGB Trading"
#property version   "2.00"
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
input int              _ORB_BufferPts           = 0;
input int              _ORB_MinRangePts         = 200;
input int              _ORB_MaxRangePts         = 5000;
input bool             _ORB_LDN_Enabled         = true;
input int              _ORB_LDN_StartHour       = 7;    // UTC
input bool             _ORB_NY_Enabled          = true;
input int              _ORB_NY_StartHour        = 13;   // UTC

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
// HEDGE_S1 — global hedge sub-stream for ORB_S1 (magic 5111)
// Fires opposite-direction LIMIT after parent SL hit. Same params for S1/S2/S3
// per global-hedge WFO (sim_wfo_hedge_global.py).
//====================================================================
// Per-stream defaults from sim_wfo_hedge.py May 2 (S1 differs from S2/S3).
// S1 winner: buf=350/h_sl=500/h_rr=4.0/exp=30 (wider parent SL=500 needs wide buffer + short window)
input bool    _HEDGE_S1_Enabled       = true;
input int     _HEDGE_S1_Magic         = 5111;
input string  _HEDGE_S1_Comment       = "ORB_S1h";
input int     _HEDGE_S1_ParentMagic   = 1111;
input int     _HEDGE_S1_BufferPts     = 350;
input int     _HEDGE_S1_FixedSL_Pts   = 500;
input double  _HEDGE_S1_RR_Ratio      = 4.0;
input int     _HEDGE_S1_ExpireMinutes = 30;
input double  _HEDGE_S1_RiskPct       = 1.0;
input double  _HEDGE_S1_DailyLossPct  = 0.0;
// F1 fast-SL filter: only fire hedge if parent SL hit <= this seconds after entry.
// 3600 = 60min (validated 2026-05-08 vs always-on; mean NP/DD$ +2.97 vs +2.78).
// 0 = disable filter (always fire = old always-on behavior).
input int     _HEDGE_S1_MaxSecondsAfterEntry = 3600;

//====================================================================
// HEDGE_S2 — global hedge for ORB_S2 (magic 5222)
//====================================================================
input bool    _HEDGE_S2_Enabled       = true;
input int     _HEDGE_S2_Magic         = 5222;
input string  _HEDGE_S2_Comment       = "ORB_S2h";
input int     _HEDGE_S2_ParentMagic   = 2222;
input int     _HEDGE_S2_BufferPts     = 100;
input int     _HEDGE_S2_FixedSL_Pts   = 500;
input double  _HEDGE_S2_RR_Ratio      = 4.0;
input int     _HEDGE_S2_ExpireMinutes = 120;
input double  _HEDGE_S2_RiskPct       = 1.0;
input double  _HEDGE_S2_DailyLossPct  = 0.0;
input int     _HEDGE_S2_MaxSecondsAfterEntry = 3600;

//====================================================================
// HEDGE_S3 — global hedge for ORB_S3 (magic 5333)
//====================================================================
input bool    _HEDGE_S3_Enabled       = true;
input int     _HEDGE_S3_Magic         = 5333;
input string  _HEDGE_S3_Comment       = "ORB_S3h";
input int     _HEDGE_S3_ParentMagic   = 3333;
input int     _HEDGE_S3_BufferPts     = 100;
input int     _HEDGE_S3_FixedSL_Pts   = 500;
input double  _HEDGE_S3_RR_Ratio      = 4.0;
input int     _HEDGE_S3_ExpireMinutes = 120;
input double  _HEDGE_S3_RiskPct       = 1.0;
input double  _HEDGE_S3_DailyLossPct  = 0.0;
input int     _HEDGE_S3_MaxSecondsAfterEntry = 3600;

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

// ORB session state (per-day state — range, fired flag, pending expiry)
struct ORBSession
{
   bool       active;
   bool       fired;
   datetime   range_start;
   datetime   range_end;
   datetime   pending_expire;
   double     range_high;
   double     range_low;
};
// Each stream has its OWN LDN + NY session state (3 streams × 2 sessions = 6 states)
ORBSession g_s1_ldn = {0};   ORBSession g_s1_ny = {0};
ORBSession g_s2_ldn = {0};   ORBSession g_s2_ny = {0};
ORBSession g_s3_ldn = {0};   ORBSession g_s3_ny = {0};

// Per-stream daily-cap state (independent rollover + lock per stream magic)
datetime g_s1_day = 0;   double g_s1_bal_start = 0.0;   bool g_s1_lock = false;
datetime g_s2_day = 0;   double g_s2_bal_start = 0.0;   bool g_s2_lock = false;
datetime g_s3_day = 0;   double g_s3_bal_start = 0.0;   bool g_s3_lock = false;

// Hedge stream cfg
struct HedgeStreamCfg
{
   bool   enabled;
   int    magic;
   int    parent_magic;
   string comment;
   int    buffer_pts;
   int    fixed_sl_pts;
   double rr_ratio;
   int    expire_minutes;
   double risk_pct;
   double daily_loss_pct;
   int    max_seconds_after_entry;  // F1 filter: hedge only if parent SL <= N seconds post-entry; 0 = disabled
};
HedgeStreamCfg g_hedge_s1, g_hedge_s2, g_hedge_s3;

// Hedge daily-cap state
datetime g_hs1_day = 0;  double g_hs1_bal_start = 0.0;  bool g_hs1_lock = false;
datetime g_hs2_day = 0;  double g_hs2_bal_start = 0.0;  bool g_hs2_lock = false;
datetime g_hs3_day = 0;  double g_hs3_bal_start = 0.0;  bool g_hs3_lock = false;

// Track last-processed deal time per hedge stream so we don't fire on the same parent SL twice.
datetime g_hedge_last_scan = 0;

// Recently-processed deal tickets (rotating buffer) — prevents duplicate hedge fires
// if multiple ticks scan the same history range. Holds last 64 deal tickets.
ulong g_hedge_seen_tickets[64];
int   g_hedge_seen_idx = 0;

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
      PrintFormat("[DT818_pro_hedge] Magic %d daily TARGET hit at $%.2f (+%.2f%%), locking.",
                  magic, pnl, pnl / balStart * 100.0);
      return true;
   }
   if(lossPct > 0 && pnl <= -balStart * lossPct / 100.0)
   {
      ClosePositionsByMagic(magic);
      CancelPendingByMagic(magic);
      lockState = true;
      PrintFormat("[DT818_pro_hedge] Magic %d daily LOSS hit at $%.2f (%.2f%%), locking.",
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
   s.range_high = 0;
   s.range_low  = 0;
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
      for(int i = 0; i < 50; i++)
      {
         datetime bt = iTime(_Symbol, _ORB_EntryTF, i);
         if(bt < s.range_start) break;
         if(bt >= s.range_end) continue;
         double h = iHigh(_Symbol, _ORB_EntryTF, i);
         double l = iLow(_Symbol, _ORB_EntryTF, i);
         if(s.range_high == 0 || h > s.range_high) s.range_high = h;
         if(s.range_low  == 0 || l < s.range_low)  s.range_low  = l;
      }
      return;
   }

   if(!s.fired && s.range_high > 0 && s.range_low > 0)
   {
      double range_pts = (s.range_high - s.range_low) / _Point;
      if(range_pts < _ORB_MinRangePts || range_pts > _ORB_MaxRangePts)
      { s.fired = true; return; }
      double sl_dist_pts = (cfg.fixed_sl_pts > 0) ? cfg.fixed_sl_pts : range_pts;
      double tp_dist_pts = sl_dist_pts * cfg.rr_ratio;
      double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
      double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);

      double totalLots = CalcLots(sl_dist_pts);
      if(totalLots <= 0) { s.fired = true; return; }
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

      double buyEntry = NormPrice(s.range_high + _ORB_BufferPts * _Point);
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
                            ORDER_TIME_SPECIFIED, s.pending_expire, cfg.comment);
            g_trade.BuyStop(halfLots, buyEntry, _Symbol, sl, tp,
                            ORDER_TIME_SPECIFIED, s.pending_expire, cfg.comment);
         }
         else
            g_trade.BuyStop(totalLots, buyEntry, _Symbol, sl, tp,
                            ORDER_TIME_SPECIFIED, s.pending_expire, cfg.comment);
      }
      double sellEntry = NormPrice(s.range_low - _ORB_BufferPts * _Point);
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
                             ORDER_TIME_SPECIFIED, s.pending_expire, cfg.comment);
            g_trade.SellStop(halfLots, sellEntry, _Symbol, sl, tp,
                             ORDER_TIME_SPECIFIED, s.pending_expire, cfg.comment);
         }
         else
            g_trade.SellStop(totalLots, sellEntry, _Symbol, sl, tp,
                             ORDER_TIME_SPECIFIED, s.pending_expire, cfg.comment);
      }
      s.fired = true;
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
   // _BrokerGMTOffsetHours retained for backward setfile compatibility
   // but no longer used. Bug fix 2026-05-07.
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
// ProcessHedgeStream — fires opposite-direction LIMIT after parent SL hit.
//
// Called once per OnTick per hedge stream. Walks history for new exit deals
// matching the parent magic; if the exit was an SL hit (comment starts with
// "[sl"), places a pending opposite-direction LIMIT at sl_price ± buffer_pts
// with own SL/TP/expire.
//
// Hedge direction = exit deal type (closing a BUY = SELL exit = SELL hedge;
// closing a SELL = BUY exit = BUY hedge — both equal "opposite of original").
//====================================================================
void ProcessHedgeStream(HedgeStreamCfg &hcfg)
{
   if(!hcfg.enabled) return;
   if(_MaxSpreadPts > 0)
   {
      long spread = SymbolInfoInteger(_Symbol, SYMBOL_SPREAD);
      if(spread > _MaxSpreadPts) return;
   }

   // Scan history from (last scan time - 60s grace) to now for new exit deals
   datetime now = TimeCurrent();
   datetime scan_from = (g_hedge_last_scan > 0) ? g_hedge_last_scan - 60 : now - 3600;
   if(!HistorySelect(scan_from, now)) return;

   int total = HistoryDealsTotal();
   for(int i = 0; i < total; i++)
   {
      ulong dt = HistoryDealGetTicket(i);
      if(dt == 0) continue;
      long magic = HistoryDealGetInteger(dt, DEAL_MAGIC);
      if((int)magic != hcfg.parent_magic) continue;
      long entry_kind = HistoryDealGetInteger(dt, DEAL_ENTRY);
      if(entry_kind != DEAL_ENTRY_OUT) continue; // only exit deals
      string sym = HistoryDealGetString(dt, DEAL_SYMBOL);
      if(sym != _Symbol) continue;
      if(HedgeWasSeen(dt)) continue;

      // Check it was an SL exit (comment usually "[sl 4595.96]")
      string comment = HistoryDealGetString(dt, DEAL_COMMENT);
      if(StringFind(comment, "[sl") != 0) continue;

      // F1 filter: skip hedge if parent SL hit too late after entry (= trend continuation, not overshoot).
      // Validated 2026-05-08: 60min cutoff gives mean NP/DD$ +2.97 vs +2.78 always-on.
      if(hcfg.max_seconds_after_entry > 0)
      {
         long pos_id = HistoryDealGetInteger(dt, DEAL_POSITION_ID);
         datetime entry_time = 0;
         // Look up the parent's IN deal (entry) for this position
         for(int j = 0; j < total; j++)
         {
            ulong et = HistoryDealGetTicket(j);
            if(et == 0) continue;
            if(HistoryDealGetInteger(et, DEAL_POSITION_ID) == pos_id &&
               HistoryDealGetInteger(et, DEAL_ENTRY) == DEAL_ENTRY_IN)
            {
               entry_time = (datetime)HistoryDealGetInteger(et, DEAL_TIME);
               break;
            }
         }
         datetime sl_time = (datetime)HistoryDealGetInteger(dt, DEAL_TIME);
         if(entry_time > 0 && (sl_time - entry_time) > hcfg.max_seconds_after_entry)
         {
            // SLOW SL — likely trend continuation, hedge would get clipped. Skip.
            HedgeMarkSeen(dt);  // mark so we don't keep checking this deal
            PrintFormat("[%s] hedge SKIPPED (F1 filter: SL %ds after entry > %ds cutoff)",
                        hcfg.comment,
                        (int)(sl_time - entry_time),
                        hcfg.max_seconds_after_entry);
            continue;
         }
         // else: fast SL (overshoot) — proceed to place hedge
      }

      // Mark seen FIRST so we don't retry on transient errors
      HedgeMarkSeen(dt);

      // Hedge direction = exit deal type (opposite of parent's original direction)
      long deal_type = HistoryDealGetInteger(dt, DEAL_TYPE);
      double sl_price = HistoryDealGetDouble(dt, DEAL_PRICE);
      ENUM_ORDER_TYPE order_type;
      double entry_px, h_sl, h_tp;
      double pt = _Point;
      if(deal_type == DEAL_TYPE_SELL)
      {
         // Closing a BUY position — original direction was BUY; hedge = SELL LIMIT above SL
         order_type = ORDER_TYPE_SELL_LIMIT;
         entry_px = NormPrice(sl_price + hcfg.buffer_pts * pt);
         h_sl     = NormPrice(entry_px + hcfg.fixed_sl_pts * pt);
         h_tp     = NormPrice(entry_px - hcfg.fixed_sl_pts * hcfg.rr_ratio * pt);
      }
      else if(deal_type == DEAL_TYPE_BUY)
      {
         // Closing a SELL position — original was SELL; hedge = BUY LIMIT below SL
         order_type = ORDER_TYPE_BUY_LIMIT;
         entry_px = NormPrice(sl_price - hcfg.buffer_pts * pt);
         h_sl     = NormPrice(entry_px - hcfg.fixed_sl_pts * pt);
         h_tp     = NormPrice(entry_px + hcfg.fixed_sl_pts * hcfg.rr_ratio * pt);
      }
      else continue;

      // Lot sizing: hcfg.risk_pct of balance / fixed_sl_pts (in account currency per pt)
      double balance = AccountInfoDouble(ACCOUNT_BALANCE);
      double risk_money = balance * hcfg.risk_pct / 100.0;
      double tick_size = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
      double tick_value = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
      double sl_money_per_lot = (hcfg.fixed_sl_pts * pt / tick_size) * tick_value;
      if(sl_money_per_lot <= 0) continue;
      double lots = risk_money / sl_money_per_lot;
      double vstep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
      double vmin  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
      double vmax  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
      if(vstep <= 0) vstep = 0.01;
      lots = MathRound(lots / vstep) * vstep;
      if(lots < vmin) lots = vmin;
      if(lots > vmax) lots = vmax;
      if(lots <= 0) continue;

      datetime expire = now + hcfg.expire_minutes * 60;
      g_trade.SetExpertMagicNumber(hcfg.magic);
      bool ok = false;
      if(order_type == ORDER_TYPE_SELL_LIMIT)
         ok = g_trade.SellLimit(lots, entry_px, _Symbol, h_sl, h_tp,
                                ORDER_TIME_SPECIFIED, expire, hcfg.comment);
      else
         ok = g_trade.BuyLimit(lots, entry_px, _Symbol, h_sl, h_tp,
                               ORDER_TIME_SPECIFIED, expire, hcfg.comment);
      if(!ok)
         PrintFormat("[%s] hedge order failed (parent magic=%d ticket=%I64u): %s",
                     hcfg.comment, hcfg.parent_magic, dt, g_trade.ResultRetcodeDescription());
      else
         PrintFormat("[%s] hedge %s lots=%.2f @ %.2f SL=%.2f TP=%.2f exp=%dmin (parent SL @ %.2f)",
                     hcfg.comment,
                     (order_type == ORDER_TYPE_SELL_LIMIT ? "SELL_LIMIT" : "BUY_LIMIT"),
                     lots, entry_px, h_sl, h_tp, hcfg.expire_minutes, sl_price);
   }
   g_hedge_last_scan = now;
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

   // Build hedge stream cfgs from inputs
   g_hedge_s1.enabled = _HEDGE_S1_Enabled;   g_hedge_s1.magic = _HEDGE_S1_Magic;
   g_hedge_s1.parent_magic = _HEDGE_S1_ParentMagic;  g_hedge_s1.comment = _HEDGE_S1_Comment;
   g_hedge_s1.buffer_pts = _HEDGE_S1_BufferPts;      g_hedge_s1.fixed_sl_pts = _HEDGE_S1_FixedSL_Pts;
   g_hedge_s1.rr_ratio = _HEDGE_S1_RR_Ratio;          g_hedge_s1.expire_minutes = _HEDGE_S1_ExpireMinutes;
   g_hedge_s1.risk_pct = _HEDGE_S1_RiskPct;           g_hedge_s1.daily_loss_pct = _HEDGE_S1_DailyLossPct;
   g_hedge_s1.max_seconds_after_entry = _HEDGE_S1_MaxSecondsAfterEntry;

   g_hedge_s2.enabled = _HEDGE_S2_Enabled;   g_hedge_s2.magic = _HEDGE_S2_Magic;
   g_hedge_s2.parent_magic = _HEDGE_S2_ParentMagic;  g_hedge_s2.comment = _HEDGE_S2_Comment;
   g_hedge_s2.buffer_pts = _HEDGE_S2_BufferPts;      g_hedge_s2.fixed_sl_pts = _HEDGE_S2_FixedSL_Pts;
   g_hedge_s2.rr_ratio = _HEDGE_S2_RR_Ratio;          g_hedge_s2.expire_minutes = _HEDGE_S2_ExpireMinutes;
   g_hedge_s2.risk_pct = _HEDGE_S2_RiskPct;           g_hedge_s2.daily_loss_pct = _HEDGE_S2_DailyLossPct;
   g_hedge_s2.max_seconds_after_entry = _HEDGE_S2_MaxSecondsAfterEntry;

   g_hedge_s3.enabled = _HEDGE_S3_Enabled;   g_hedge_s3.magic = _HEDGE_S3_Magic;
   g_hedge_s3.parent_magic = _HEDGE_S3_ParentMagic;  g_hedge_s3.comment = _HEDGE_S3_Comment;
   g_hedge_s3.buffer_pts = _HEDGE_S3_BufferPts;      g_hedge_s3.fixed_sl_pts = _HEDGE_S3_FixedSL_Pts;
   g_hedge_s3.rr_ratio = _HEDGE_S3_RR_Ratio;          g_hedge_s3.expire_minutes = _HEDGE_S3_ExpireMinutes;
   g_hedge_s3.risk_pct = _HEDGE_S3_RiskPct;           g_hedge_s3.daily_loss_pct = _HEDGE_S3_DailyLossPct;
   g_hedge_s3.max_seconds_after_entry = _HEDGE_S3_MaxSecondsAfterEntry;

   ArrayInitialize(g_hedge_seen_tickets, 0);
   g_hedge_last_scan = TimeCurrent();

   g_lastBar_M1 = iTime(_Symbol, PERIOD_M1, 0);

   double bal = AccountInfoDouble(ACCOUNT_BALANCE);
   g_s1_bal_start = bal;
   g_s2_bal_start = bal;
   g_s3_bal_start = bal;
   g_hs1_bal_start = bal;  g_hs2_bal_start = bal;  g_hs3_bal_start = bal;

   PrintFormat("[DT818_pro_hedge] Init. Streams S1=%d S2=%d S3=%d  Sessions LDN=%d NY=%d  "
               "Caps S1=%.1f/%.1f S2=%.1f/%.1f S3=%.1f/%.1f",
               _ORB_S1_Enabled, _ORB_S2_Enabled, _ORB_S3_Enabled,
               _ORB_LDN_Enabled, _ORB_NY_Enabled,
               _ORB_S1_DailyTargetPct, _ORB_S1_DailyLossPct,
               _ORB_S2_DailyTargetPct, _ORB_S2_DailyLossPct,
               _ORB_S3_DailyTargetPct, _ORB_S3_DailyLossPct);
   PrintFormat("[DT818_pro_hedge] Hedge sub-streams h1=%d h2=%d h3=%d  "
               "global cfg buf=%d/%d/%d sl=%d/%d/%d rr=%.1f/%.1f/%.1f exp=%d/%d/%d",
               _HEDGE_S1_Enabled, _HEDGE_S2_Enabled, _HEDGE_S3_Enabled,
               _HEDGE_S1_BufferPts, _HEDGE_S2_BufferPts, _HEDGE_S3_BufferPts,
               _HEDGE_S1_FixedSL_Pts, _HEDGE_S2_FixedSL_Pts, _HEDGE_S3_FixedSL_Pts,
               _HEDGE_S1_RR_Ratio, _HEDGE_S2_RR_Ratio, _HEDGE_S3_RR_Ratio,
               _HEDGE_S1_ExpireMinutes, _HEDGE_S2_ExpireMinutes, _HEDGE_S3_ExpireMinutes);
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

   // Each stream processes both LDN and NY sessions internally.
   if(!s1_locked) ProcessORBStream(g_cfg_s1, g_s1_ldn, g_s1_ny);
   if(!s2_locked) ProcessORBStream(g_cfg_s2, g_s2_ldn, g_s2_ny);
   if(!s3_locked) ProcessORBStream(g_cfg_s3, g_s3_ldn, g_s3_ny);

   // Hedge sub-streams: scan history for parent SL hits and place opposite-direction
   // LIMIT orders. Each hedge has independent magic + daily-cap state.
   bool h1_locked = StreamDailyCapsCheck(_HEDGE_S1_Magic, 0.0, _HEDGE_S1_DailyLossPct,
                                          g_hs1_day, g_hs1_bal_start, g_hs1_lock);
   bool h2_locked = StreamDailyCapsCheck(_HEDGE_S2_Magic, 0.0, _HEDGE_S2_DailyLossPct,
                                          g_hs2_day, g_hs2_bal_start, g_hs2_lock);
   bool h3_locked = StreamDailyCapsCheck(_HEDGE_S3_Magic, 0.0, _HEDGE_S3_DailyLossPct,
                                          g_hs3_day, g_hs3_bal_start, g_hs3_lock);
   if(!h1_locked) ProcessHedgeStream(g_hedge_s1);
   if(!h2_locked) ProcessHedgeStream(g_hedge_s2);
   if(!h3_locked) ProcessHedgeStream(g_hedge_s3);
}
//+------------------------------------------------------------------+

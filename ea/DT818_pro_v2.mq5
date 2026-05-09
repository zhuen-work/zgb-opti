//+------------------------------------------------------------------+
//| DT818_pro_v2.mq5                                                  |
//| ORB EA: 6-stream rank-portfolio (S1..S6, no hedge)                |
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
#property version   "1.00"
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
   bool       fired;
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

   g_lastBar_M1 = iTime(_Symbol, PERIOD_M1, 0);

   double bal = AccountInfoDouble(ACCOUNT_BALANCE);
   g_s1_bal_start = bal;
   g_s2_bal_start = bal;
   g_s3_bal_start = bal;
   g_s4_bal_start = bal;
   g_s5_bal_start = bal;
   g_s6_bal_start = bal;

   PrintFormat("[DT818_pro_v2] Init. Streams S1=%d S2=%d S3=%d S4=%d S5=%d S6=%d  "
               "Sessions LDN=%d NY=%d",
               _ORB_S1_Enabled, _ORB_S2_Enabled, _ORB_S3_Enabled,
               _ORB_S4_Enabled, _ORB_S5_Enabled, _ORB_S6_Enabled,
               _ORB_LDN_Enabled, _ORB_NY_Enabled);
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
}
//+------------------------------------------------------------------+

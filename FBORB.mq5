//+------------------------------------------------------------------+
//| FBORB.mq5                                                         |
//| FBO + ORB combined EA. 3 streams sharing a single account:        |
//|   FBO S1 (M30 fractal breakout + SMA filter, magic 1000 cmt FBO_A)|
//|   FBO S2 (M15 fractal breakout + SMA filter, magic 1000 cmt FBO_B)|
//|   ORB     (M5 session opening-range breakout,  magic 2000 cmt ORB)|
//| Account-level daily target/loss caps shared across all streams.   |
//+------------------------------------------------------------------+
#property copyright "ZGB Trading"
#property version   "1.00"
#property strict

#include <Trade/Trade.mqh>

CTrade g_trade;

//====================================================================
// Global Inputs
//====================================================================
input int     _BaseMagic               = 1000;   // Base magic number
input double  _CapitalProtectionAmount = 0.0;    // Stop trading below this equity (0=off)
input double  _RiskPct                 = 3.0;    // Risk % per trade
input int     _LotMode                 = 1;      // 0=fixed LotStep, 1=risk%, 2=tiered
input int     TierBase                 = 2000;   // Tier base for tiered lot mode
input double  LotStep                  = 0.01;   // Fixed lot / lot step
// Per-stream pending expiry (in signal-TF bars). FBO S1 + S2 may want
// different values since their TFs differ; keeping a global was a quirk
// of the old EA design.
input int     _PendingExpireBars1      = 2;      // FBO S1 expiry (S1 TF bars)
input int     _PendingExpireBars2      = 4;      // FBO S2 expiry (S2 TF bars)

//====================================================================
// FBO Stream 1
//====================================================================
input int     _FBO1                    = 1;       // Enable FBO stream 1 (1=on, 0=off)
input string  _OrderComment            = "FBO_A";
input int     _time_frame              = 30;      // Timeframe (MT5 enum value)
input int     _take_profit             = 15000;   // Take profit (points)
input int     _stop_loss               = 5000;    // Stop loss (points)
input int     _Bars                    = 4;       // Lookback bars for high/low
input int     _EMA_Period1             = 5;       // SMA period (trend filter)
input double  _HalfTP1                 = 0.6;    // Split TP ratio (0=off)

//====================================================================
// FBO Stream 2
//====================================================================
input int     _FBO2                    = 1;
input string  _OrderComment2           = "FBO_B";
input int     _time_frame2             = 16388;   // H4
input int     _take_profit2            = 15000;
input int     _stop_loss2              = 10000;
input int     _Bars2                   = 6;
input int     _EMA_Period2             = 25;
input double  _HalfTP2                 = 0.8;

// (FBO Stream 3 removed Apr 25 — failed WFO robustness gate.)
// (FVG S1+S2 removed Apr 26 — failed gate / dominated.)

//====================================================================
// ORB Stream (session opening-range breakout)
//====================================================================
input bool             _ORB_Enabled             = true;
input int              _ORB_Magic               = 2000;
input string           _ORB_Comment             = "ORB";
input ENUM_TIMEFRAMES  _ORB_EntryTF             = PERIOD_M5;
// Session start hours are in UTC. EA converts to BROKER time using
// _BrokerGMTOffsetHours (broker-time only — no dependency on VPS clock).
// Vantage: 2 (winter EET) / 3 (summer EEST DST).
// **Update this manually at DST changes:** Oct 26 → 2, Mar 28 → 3.
input int              _BrokerGMTOffsetHours    = 3;     // Hours broker is ahead of UTC
input bool             _ORB_LDN_Enabled         = true;
input int              _ORB_LDN_StartHour       = 7;     // UTC (London open)
input bool             _ORB_NY_Enabled          = true;
input int              _ORB_NY_StartHour        = 13;    // UTC (NY open)
input int              _ORB_RangeMinutes        = 60;
input int              _ORB_BufferPts           = 0;
input int              _ORB_MinRangePts         = 200;
input int              _ORB_MaxRangePts         = 5000;
input int              _ORB_FixedSL_Pts         = 400;
input double           _ORB_RR_Ratio            = 3.0;
input double           _ORB_HalfTP_Ratio        = 0.0;
input int              _ORB_PendingExpireMinutes = 240;

//====================================================================
// Daily target / loss caps — applies ONLY to ORB stream.
// FBO streams trade unconstrained (they were WFO'd without caps).
//====================================================================
input double           _DailyTargetPct          = 9.0;   // ORB only
input double           _DailyLossPct            = 6.0;   // ORB only

//====================================================================
// Globals
//====================================================================
datetime g_lastBar_FBO_M1 = 0;  // shared M1 bar tracker for all FBO + ORB

int g_emaHandle_FBO1 = INVALID_HANDLE;
int g_emaHandle_FBO2 = INVALID_HANDLE;

// ORB session state
struct ORBSession
{
   bool       active;
   bool       fired;
   bool       traded;
   datetime   range_start;
   datetime   range_end;
   datetime   pending_expire;
   double     range_high;
   double     range_low;
};
ORBSession g_orb_ldn = {0};
ORBSession g_orb_ny  = {0};

// Daily cap state (account-level)
datetime g_session_day = 0;
double   g_balance_day_start = 0.0;
bool     g_daily_lock = false;

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

double GetEMA(int handle, int shift)
{
   if(handle == INVALID_HANDLE) return 0;
   double buf[];
   ArraySetAsSeries(buf, true);
   if(CopyBuffer(handle, 0, shift, 1, buf) != 1) return 0;
   return buf[0];
}

//====================================================================
// Lot Sizing
//====================================================================
double CalcLots(double slPoints)
{
   double lots = 0;

   if(_LotMode == 0)
   {
      lots = LotStep;
   }
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

   double minL  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxL  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double step  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if(step <= 0) step = 0.01;

   lots = MathMax(minL, MathMin(maxL, lots));
   lots = MathRound(lots / step) * step;
   return lots;
}

//====================================================================
// Position / Order Queries
//====================================================================
int CountPositions(const string comment)
{
   int count = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0) continue;
      if(!PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _BaseMagic) continue;
      if(PositionGetString(POSITION_COMMENT) != comment) continue;
      count++;
   }
   return count;
}

bool HasPending(const string comment)
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0) continue;
      if(!OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != _BaseMagic) continue;
      if(OrderGetString(ORDER_COMMENT) != comment) continue;
      return true;
   }
   return false;
}

void DeletePending(const string comment)
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0) continue;
      if(!OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != _BaseMagic) continue;
      if(OrderGetString(ORDER_COMMENT) != comment) continue;
      g_trade.OrderDelete(t);
   }
}

//====================================================================
// Custom Fractal: Find last fractal level (custom period)
// _Bars = number of bars on each side. Fractal UP at bar i means
// high[i] is higher than high of _Bars bars on each side.
// Search from bar _Bars (earliest confirmed) outward.
//====================================================================
double GetLastFractal(ENUM_TIMEFRAMES tf, bool wantUp, int period)
{
   int maxSearch = 200;

   for(int i = period; i < maxSearch; i++)
   {
      bool valid = true;

      if(wantUp)
      {
         double hi = iHigh(_Symbol, tf, i);
         for(int j = 1; j <= period; j++)
         {
            if(iHigh(_Symbol, tf, i - j) >= hi || iHigh(_Symbol, tf, i + j) >= hi)
            { valid = false; break; }
         }
         if(valid) return hi;
      }
      else
      {
         double lo = iLow(_Symbol, tf, i);
         for(int j = 1; j <= period; j++)
         {
            if(iLow(_Symbol, tf, i - j) <= lo || iLow(_Symbol, tf, i + j) <= lo)
            { valid = false; break; }
         }
         if(valid) return lo;
      }
   }
   return 0;
}

//====================================================================
// FBO: Process one stream — called on every M1 bar
// Custom fractals on signal TF for breakout levels.
// SMA on signal TF for direction. Bid price for comparison.
// Split entry: one at HalfTP% of TP, one at full TP.
// Orders expire naturally via per-stream pebBars.
//====================================================================
void ProcessFBO(ENUM_TIMEFRAMES tf, int bars, int tpPts, int slPts,
                double halfTP, const string comment, int emaHandle, int pebBars)
{
   if(_CapitalProtectionAmount > 0 &&
      AccountInfoDouble(ACCOUNT_EQUITY) < _CapitalProtectionAmount)
      return;

   // Only place if stream is idle (no pending and no open positions)
   if(HasPending(comment)) return;
   if(CountPositions(comment) > 0) return;

   // Price above EMA → buy only. Price below EMA → sell only.
   double ema = GetEMA(emaHandle, 1);
   if(ema <= 0) return;

   double price = SymbolInfoDouble(_Symbol, SYMBOL_BID);

   bool wantBuy  = (price > ema);
   bool wantSell = (price < ema);

   g_trade.SetExpertMagicNumber(_BaseMagic);

   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);

   // Expiry based on signal TF bars (per-stream)
   datetime expiry = iTime(_Symbol, tf, 0) + pebBars * PeriodSeconds(tf);

   // Lot calculation
   double totalLots = CalcLots((double)slPts);
   if(totalLots <= 0) return;

   double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   double minLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   if(lotStep <= 0) lotStep = 0.01;

   double halfLots = MathRound(totalLots / 2.0 / lotStep) * lotStep;
   if(halfLots < minLot) halfLots = minLot;

   if(wantBuy)
   {
      double entry = GetLastFractal(tf, true, bars);
      if(entry <= 0) return;
      entry = NormPrice(entry);

      double minE = NormPrice(ask + stopsLvl * _Point);
      if(entry < minE) entry = minE;
      if(entry <= ask) return;

      double sl     = NormPrice(entry - slPts * _Point);
      double tpFull = NormPrice(entry + tpPts * _Point);

      if(halfTP > 0)
      {
         double tpHalf = NormPrice(entry + tpPts * halfTP * _Point);
         g_trade.BuyStop(halfLots, entry, _Symbol, sl, tpHalf,
                         ORDER_TIME_SPECIFIED, expiry, comment);
      }
      double remainLots = (halfTP > 0) ? halfLots : totalLots;
      g_trade.BuyStop(remainLots, entry, _Symbol, sl, tpFull,
                      ORDER_TIME_SPECIFIED, expiry, comment);
   }

   if(wantSell)
   {
      double entry = GetLastFractal(tf, false, bars);
      if(entry <= 0) return;
      entry = NormPrice(entry);

      double maxE = NormPrice(bid - stopsLvl * _Point);
      if(entry > maxE) entry = maxE;
      if(entry >= bid) return;

      double sl     = NormPrice(entry + slPts * _Point);
      double tpFull = NormPrice(entry - tpPts * _Point);

      if(halfTP > 0)
      {
         double tpHalf = NormPrice(entry - tpPts * halfTP * _Point);
         g_trade.SellStop(halfLots, entry, _Symbol, sl, tpHalf,
                          ORDER_TIME_SPECIFIED, expiry, comment);
      }
      double remainLots = (halfTP > 0) ? halfLots : totalLots;
      g_trade.SellStop(remainLots, entry, _Symbol, sl, tpFull,
                       ORDER_TIME_SPECIFIED, expiry, comment);
   }
}

//====================================================================
// ORB stream — port of Scalper_v2 ProcessORB (account-level magic _ORB_Magic)
//====================================================================
double CalcLotsFor(double slPoints)
{
   if(_LotMode == 0) return LotStep;
   double riskMoney = AccountInfoDouble(ACCOUNT_BALANCE) * _RiskPct / 100.0;
   double tickVal   = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickSize  = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   if(tickVal <= 0 || tickSize <= 0 || slPoints <= 0) return 0;
   double slMoney = (slPoints * _Point / tickSize) * tickVal;
   if(slMoney <= 0) return 0;
   double lots = riskMoney / slMoney;
   double minL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if(step <= 0) step = 0.01;
   lots = MathMax(minL, MathMin(maxL, lots));
   lots = MathRound(lots / step) * step;
   return lots;
}

// startHourBroker = broker-time hour when session begins (already converted from UTC).
void InitORBSession(ORBSession &s, int startHourBroker, datetime now_broker)
{
   MqlDateTime mt;
   TimeToStruct(now_broker, mt);
   mt.hour = startHourBroker; mt.min = 0; mt.sec = 0;
   s.range_start = StructToTime(mt);
   s.range_end   = s.range_start + _ORB_RangeMinutes * 60;
   s.pending_expire = s.range_end + _ORB_PendingExpireMinutes * 60;
   s.active = true;
   s.fired = false;
   s.traded = false;
   s.range_high = 0;
   s.range_low  = 0;
}

void UpdateORBSession(ORBSession &s, datetime now)
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
      // Track range high/low from M5 bars in window
      for(int i = 0; i < 50; i++)
      {
         datetime bt = iTime(_Symbol, _ORB_EntryTF, i);
         if(bt < s.range_start) break;
         if(bt >= s.range_end) continue;
         double h = iHigh(_Symbol, _ORB_EntryTF, i);
         double l = iLow(_Symbol, _ORB_EntryTF, i);
         if(s.range_high == 0 || h > s.range_high) s.range_high = h;
         if(s.range_low == 0 || l < s.range_low) s.range_low = l;
      }
      return;
   }

   if(!s.fired && s.range_high > 0 && s.range_low > 0)
   {
      double range_pts = (s.range_high - s.range_low) / _Point;
      if(range_pts < _ORB_MinRangePts || range_pts > _ORB_MaxRangePts)
      {
         s.fired = true;
         return;
      }
      double sl_dist_pts = (_ORB_FixedSL_Pts > 0) ? _ORB_FixedSL_Pts : range_pts;
      double tp_dist_pts = sl_dist_pts * _ORB_RR_Ratio;
      double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
      double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);

      double totalLots = CalcLotsFor(sl_dist_pts);
      if(totalLots <= 0) { s.fired = true; return; }

      double halfLots = totalLots;
      if(_ORB_HalfTP_Ratio > 0)
      {
         double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
         double minL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
         if(step <= 0) step = 0.01;
         halfLots = MathRound(totalLots / 2.0 / step) * step;
         if(halfLots < minL) halfLots = minL;
      }

      g_trade.SetExpertMagicNumber(_ORB_Magic);

      // BuyStop above range_high
      double buyEntry = NormPrice(s.range_high + _ORB_BufferPts * _Point);
      double minBuy = NormPrice(ask + stopsLvl * _Point);
      if(buyEntry < minBuy) buyEntry = minBuy;
      if(buyEntry > ask)
      {
         double sl = NormPrice(buyEntry - sl_dist_pts * _Point);
         double tp = NormPrice(buyEntry + tp_dist_pts * _Point);
         if(_ORB_HalfTP_Ratio > 0)
         {
            double tpHalf = NormPrice(buyEntry + tp_dist_pts * _ORB_HalfTP_Ratio * _Point);
            g_trade.BuyStop(halfLots, buyEntry, _Symbol, sl, tpHalf,
                            ORDER_TIME_SPECIFIED, s.pending_expire, _ORB_Comment);
            g_trade.BuyStop(halfLots, buyEntry, _Symbol, sl, tp,
                            ORDER_TIME_SPECIFIED, s.pending_expire, _ORB_Comment);
         }
         else
         {
            g_trade.BuyStop(totalLots, buyEntry, _Symbol, sl, tp,
                            ORDER_TIME_SPECIFIED, s.pending_expire, _ORB_Comment);
         }
      }
      // SellStop below range_low
      double sellEntry = NormPrice(s.range_low - _ORB_BufferPts * _Point);
      double maxSell = NormPrice(bid - stopsLvl * _Point);
      if(sellEntry > maxSell) sellEntry = maxSell;
      if(sellEntry < bid)
      {
         double sl = NormPrice(sellEntry + sl_dist_pts * _Point);
         double tp = NormPrice(sellEntry - tp_dist_pts * _Point);
         if(_ORB_HalfTP_Ratio > 0)
         {
            double tpHalf = NormPrice(sellEntry - tp_dist_pts * _ORB_HalfTP_Ratio * _Point);
            g_trade.SellStop(halfLots, sellEntry, _Symbol, sl, tpHalf,
                             ORDER_TIME_SPECIFIED, s.pending_expire, _ORB_Comment);
            g_trade.SellStop(halfLots, sellEntry, _Symbol, sl, tp,
                             ORDER_TIME_SPECIFIED, s.pending_expire, _ORB_Comment);
         }
         else
         {
            g_trade.SellStop(totalLots, sellEntry, _Symbol, sl, tp,
                             ORDER_TIME_SPECIFIED, s.pending_expire, _ORB_Comment);
         }
      }
      s.fired = true;
   }

   // Past pending expire: cleanup
   if(now >= s.pending_expire)
   {
      // Cancel ORB-magic pendings
      for(int i = OrdersTotal() - 1; i >= 0; i--)
      {
         ulong tk = OrderGetTicket(i);
         if(tk == 0) continue;
         if(!OrderSelect(tk)) continue;
         if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
         if((int)OrderGetInteger(ORDER_MAGIC) != _ORB_Magic) continue;
         g_trade.OrderDelete(tk);
      }
      s.active = false;
   }
}

void ProcessORB()
{
   datetime now = TimeCurrent();   // broker server time (no VPS-clock dependency)
   MqlDateTime mt;
   TimeToStruct(now, mt);

   // Convert UTC StartHour inputs to BROKER time using manual offset input.
   int ldn_broker_hour = (_ORB_LDN_StartHour + _BrokerGMTOffsetHours) % 24;
   int ny_broker_hour  = (_ORB_NY_StartHour  + _BrokerGMTOffsetHours) % 24;

   if(_ORB_LDN_Enabled)
   {
      if(!g_orb_ldn.active && mt.hour == ldn_broker_hour && mt.min < 5)
         InitORBSession(g_orb_ldn, ldn_broker_hour, now);
      UpdateORBSession(g_orb_ldn, now);
   }
   if(_ORB_NY_Enabled)
   {
      if(!g_orb_ny.active && mt.hour == ny_broker_hour && mt.min < 5)
         InitORBSession(g_orb_ny, ny_broker_hour, now);
      UpdateORBSession(g_orb_ny, now);
   }
}

//====================================================================
// Daily target / loss enforcement — ORB STREAM ONLY.
// Aggregates only ORB-magic deals; closes only ORB positions/pendings.
// FBO streams unaffected — they continue trading regardless of ORB lock.
//====================================================================
double SumRealizedTodayORB(datetime dayStart)
{
   double total = 0.0;
   HistorySelect(dayStart, TimeCurrent());
   for(int i = HistoryDealsTotal() - 1; i >= 0; i--)
   {
      ulong tk = HistoryDealGetTicket(i);
      if(tk == 0) continue;
      if(HistoryDealGetString(tk, DEAL_SYMBOL) != _Symbol) continue;
      if((int)HistoryDealGetInteger(tk, DEAL_MAGIC) != _ORB_Magic) continue;
      if(HistoryDealGetInteger(tk, DEAL_ENTRY) != DEAL_ENTRY_OUT) continue;
      total += HistoryDealGetDouble(tk, DEAL_PROFIT);
      total += HistoryDealGetDouble(tk, DEAL_SWAP);
      total += HistoryDealGetDouble(tk, DEAL_COMMISSION);
   }
   return total;
}

double SumUnrealizedORB()
{
   double total = 0.0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong tk = PositionGetTicket(i);
      if(tk == 0) continue;
      if(!PositionSelectByTicket(tk)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _ORB_Magic) continue;
      total += PositionGetDouble(POSITION_PROFIT);
      total += PositionGetDouble(POSITION_SWAP);
   }
   return total;
}

void CloseORBAndCancelORBPending()
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong tk = PositionGetTicket(i);
      if(tk == 0) continue;
      if(!PositionSelectByTicket(tk)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _ORB_Magic) continue;
      g_trade.PositionClose(tk);
   }
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong tk = OrderGetTicket(i);
      if(tk == 0) continue;
      if(!OrderSelect(tk)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != _ORB_Magic) continue;
      g_trade.OrderDelete(tk);
   }
}

// Returns true when ORB is locked for the day (FBO unaffected).
bool ORBDailyCapsCheck()
{
   datetime now = TimeCurrent();
   MqlDateTime mt;
   TimeToStruct(now, mt);
   mt.hour = 0; mt.min = 0; mt.sec = 0;
   datetime today = StructToTime(mt);

   if(today != g_session_day)
   {
      g_session_day = today;
      g_balance_day_start = AccountInfoDouble(ACCOUNT_BALANCE);
      g_daily_lock = false;
   }

   if(g_daily_lock) return true;
   if(g_balance_day_start <= 0) return false;

   double orb_pnl_today = SumRealizedTodayORB(today) + SumUnrealizedORB();

   if(_DailyTargetPct > 0 && orb_pnl_today >= g_balance_day_start * _DailyTargetPct / 100.0)
   {
      CloseORBAndCancelORBPending();
      g_daily_lock = true;
      return true;
   }
   if(_DailyLossPct > 0 && orb_pnl_today <= -g_balance_day_start * _DailyLossPct / 100.0)
   {
      CloseORBAndCancelORBPending();
      g_daily_lock = true;
      return true;
   }
   return false;
}

//====================================================================
// OnInit
//====================================================================
int OnInit()
{
   g_trade.SetExpertMagicNumber(_BaseMagic);
   g_trade.SetDeviationInPoints(50);

   if(_FBO1)
   {
      g_emaHandle_FBO1 = iMA(_Symbol, TF(_time_frame), _EMA_Period1,
                              0, MODE_SMA, PRICE_CLOSE);
      if(g_emaHandle_FBO1 == INVALID_HANDLE) return INIT_FAILED;
   }
   if(_FBO2)
   {
      g_emaHandle_FBO2 = iMA(_Symbol, TF(_time_frame2), _EMA_Period2,
                              0, MODE_SMA, PRICE_CLOSE);
      if(g_emaHandle_FBO2 == INVALID_HANDLE) return INIT_FAILED;
   }
   g_lastBar_FBO_M1 = iTime(_Symbol, PERIOD_M1, 0);
   g_session_day = 0;
   g_balance_day_start = AccountInfoDouble(ACCOUNT_BALANCE);
   g_daily_lock = false;

   return INIT_SUCCEEDED;
}

//====================================================================
// OnDeinit
//====================================================================
void OnDeinit(const int reason)
{
   if(g_emaHandle_FBO1  != INVALID_HANDLE) IndicatorRelease(g_emaHandle_FBO1);
   if(g_emaHandle_FBO2  != INVALID_HANDLE) IndicatorRelease(g_emaHandle_FBO2);
}

//====================================================================
// OnTick
//====================================================================
void OnTick()
{
   // ORB daily-cap check on every tick (closes/locks ORB only).
   bool orb_locked = ORBDailyCapsCheck();

   // Stream processing only on new M1 bar
   if(!IsNewBar(PERIOD_M1, g_lastBar_FBO_M1)) return;

   // FBO streams always trade — daily caps don't apply to them.
   if(_FBO1)
      ProcessFBO(TF(_time_frame), _Bars, _take_profit, _stop_loss,
                 _HalfTP1, _OrderComment, g_emaHandle_FBO1, _PendingExpireBars1);

   if(_FBO2)
      ProcessFBO(TF(_time_frame2), _Bars2, _take_profit2, _stop_loss2,
                 _HalfTP2, _OrderComment2, g_emaHandle_FBO2, _PendingExpireBars2);

   // ORB only runs if not locked for the day.
   if(_ORB_Enabled && !orb_locked)
      ProcessORB();
}
//+------------------------------------------------------------------+

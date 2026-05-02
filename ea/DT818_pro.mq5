//+------------------------------------------------------------------+
//| DT818_pro.mq5                                                     |
//| Combined 3-strategy EA: FBO + ORB + LSFVG                         |
//|                                                                   |
//| Streams (each fully independent — magic isolation + own daily caps)|
//|   FBO   (magic 1000): S1 M30 fractal + S2 M15 fractal, SMA filter |
//|   ORB   (magic 2000): M5 session opening-range breakout (LDN+NY)  |
//|   LSFVG (magic 3000): M15 liquidity sweep + 3-bar fair-value gap  |
//|                                                                   |
//| Daily targets/loss caps are STREAM-LOCAL:                         |
//|   - Each stream has its own _<X>_DailyTargetPct / _<X>_DailyLossPct|
//|   - Each cap counts only its own magic's deals (realized + unreal)|
//|   - When one stream's cap fires, ONLY it closes positions & locks |
//|   - Other streams continue trading unaffected                     |
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

//====================================================================
// FBO streams (magic 1000 — both S1 & S2 share this magic + daily cap)
//====================================================================
input int     _FBO_Magic               = 1000;
input int     _PendingExpireBars1      = 2;      // FBO S1 expiry (S1 TF bars)
input int     _PendingExpireBars2      = 4;      // FBO S2 expiry (S2 TF bars)
input double  _FBO_DailyTargetPct      = 0.0;    // 0 = disabled (FBO unconstrained per WFO)
input double  _FBO_DailyLossPct        = 0.0;    // 0 = disabled

// FBO Stream 1
input int     _FBO1                    = 1;
input string  _FBO1_Comment            = "FBO_A";
input int     _FBO1_TF                 = 30;     // M30
input int     _FBO1_TP                 = 25000;
input int     _FBO1_SL                 = 10000;
input int     _FBO1_Bars               = 8;
input int     _FBO1_SMA                = 10;
input double  _FBO1_HalfTP             = 0.3;

// FBO Stream 2
input int     _FBO2                    = 1;
input string  _FBO2_Comment            = "FBO_B";
input int     _FBO2_TF                 = 15;     // M15
input int     _FBO2_TP                 = 4000;
input int     _FBO2_SL                 = 4000;
input int     _FBO2_Bars               = 8;
input int     _FBO2_SMA                = 50;
input double  _FBO2_HalfTP             = 0.6;

//====================================================================
// ORB stream (magic 2000)
//====================================================================
input bool             _ORB_Enabled             = true;
input int              _ORB_Magic               = 2000;
input string           _ORB_Comment             = "ORB";
input ENUM_TIMEFRAMES  _ORB_EntryTF             = PERIOD_M5;
// Vantage broker time = GMT+2 (winter) / GMT+3 (summer DST). Update at DST flips.
input int              _BrokerGMTOffsetHours    = 3;
input bool             _ORB_LDN_Enabled         = true;
input int              _ORB_LDN_StartHour       = 7;     // UTC
input bool             _ORB_NY_Enabled          = true;
input int              _ORB_NY_StartHour        = 13;    // UTC
input int              _ORB_RangeMinutes        = 60;
input int              _ORB_BufferPts           = 0;
input int              _ORB_MinRangePts         = 200;
input int              _ORB_MaxRangePts         = 5000;
input int              _ORB_FixedSL_Pts         = 400;
input double           _ORB_RR_Ratio            = 3.0;
input double           _ORB_HalfTP_Ratio        = 0.0;
input int              _ORB_PendingExpireMinutes = 240;
input double           _ORB_DailyTargetPct      = 27.0;  // 9% × 3 (3 streams scaling)
input double           _ORB_DailyLossPct        = 18.0;

//====================================================================
// LSFVG stream (magic 3000) — Liquidity Sweep + Fair Value Gap
//====================================================================
input bool    _LSFVG_Enabled           = true;
input int     _LSFVG_Magic             = 3000;
input string  _LSFVG_Comment           = "LSFVG";
input int     _LSFVG_SignalTF          = 15;     // M15
input int     _LSFVG_LookbackBars      = 10;
input int     _LSFVG_MinFVGPts         = 20;
input int     _LSFVG_MaxFVGPts         = 5000;
input int     _LSFVG_SweepBufferPts    = 30;
input double  _LSFVG_RR_Ratio          = 2.0;
input double  _LSFVG_HalfTP_Ratio      = 0.5;
input int     _LSFVG_PendingExpireBars = 4;
input bool    _LSFVG_UseEMAFilter      = false;
input int     _LSFVG_EMA_Period        = 50;
input double  _LSFVG_DailyTargetPct    = 0.0;    // 0 = disabled
input double  _LSFVG_DailyLossPct      = 0.0;    // 0 = disabled

//====================================================================
// EMAPullback stream (magic 4000) — pullback-to-EMA bounce
// W70 WFO winner: EMA=50, Look=3, Band=150, SL=30, RR=2.0, Tgt=0/Loss=6
//====================================================================
input bool    _EMP_Enabled             = true;
input int     _EMP_Magic               = 4000;
input string  _EMP_Comment             = "EMAPullback";
input int     _EMP_SignalTF            = 15;     // M15
input int     _EMP_EMA_Period          = 50;
input int     _EMP_LookbackBars        = 3;
input int     _EMP_PullbackBandPts     = 150;
input int     _EMP_EntryBufferPts      = 0;
input int     _EMP_SLBufferPts         = 30;
input double  _EMP_RR_Ratio            = 2.0;
input double  _EMP_HalfTP_Ratio        = 0.0;
input int     _EMP_PendingExpireBars   = 3;
input double  _EMP_DailyTargetPct      = 0.0;    // 0 = disabled
input double  _EMP_DailyLossPct        = 6.0;    // % of day-start balance

//====================================================================
// Globals
//====================================================================
datetime g_lastBar_M1 = 0;       // shared M1 bar tracker (FBO + LSFVG bar-event timing)
int      g_emaHandle_FBO1 = INVALID_HANDLE;
int      g_emaHandle_FBO2 = INVALID_HANDLE;
int      g_emaHandle_LSFVG = INVALID_HANDLE;
int      g_emaHandle_EMP   = INVALID_HANDLE;

// ORB session state
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
ORBSession g_orb_ldn = {0};
ORBSession g_orb_ny  = {0};

// Per-stream daily-cap state (independent rollover + lock per stream)
datetime g_fbo_day  = 0;   double g_fbo_bal_start  = 0.0;  bool g_fbo_lock  = false;
datetime g_orb_day  = 0;   double g_orb_bal_start  = 0.0;  bool g_orb_lock  = false;
datetime g_lsf_day  = 0;   double g_lsf_bal_start  = 0.0;  bool g_lsf_lock  = false;
datetime g_emp_day  = 0;   double g_emp_bal_start  = 0.0;  bool g_emp_lock  = false;

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
// Custom Fractal (FBO breakout level)
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
            if(iHigh(_Symbol, tf, i - j) >= hi || iHigh(_Symbol, tf, i + j) >= hi)
            { valid = false; break; }
         if(valid) return hi;
      }
      else
      {
         double lo = iLow(_Symbol, tf, i);
         for(int j = 1; j <= period; j++)
            if(iLow(_Symbol, tf, i - j) <= lo || iLow(_Symbol, tf, i + j) <= lo)
            { valid = false; break; }
         if(valid) return lo;
      }
   }
   return 0;
}

//====================================================================
// FBO stream processor (one substream — S1 or S2)
//====================================================================
void ProcessFBO(ENUM_TIMEFRAMES tf, int bars, int tpPts, int slPts,
                double halfTP, const string comment, int emaHandle, int pebBars)
{
   if(HasPendingByMagicComment(_FBO_Magic, comment)) return;
   if(CountPositionsByMagicComment(_FBO_Magic, comment) > 0) return;

   double ema = GetEMA(emaHandle, 1);
   if(ema <= 0) return;
   double price = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   bool wantBuy  = (price > ema);
   bool wantSell = (price < ema);

   g_trade.SetExpertMagicNumber(_FBO_Magic);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);
   datetime expiry = iTime(_Symbol, tf, 0) + pebBars * PeriodSeconds(tf);

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
// ORB stream processor (port from FBORB.mq5)
//====================================================================
void InitORBSession(ORBSession &s, int startHourBroker, datetime now_broker)
{
   MqlDateTime mt; TimeToStruct(now_broker, mt);
   mt.hour = startHourBroker; mt.min = 0; mt.sec = 0;
   s.range_start = StructToTime(mt);
   s.range_end   = s.range_start + _ORB_RangeMinutes * 60;
   s.pending_expire = s.range_end + _ORB_PendingExpireMinutes * 60;
   s.active = true;
   s.fired = false;
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
      double sl_dist_pts = (_ORB_FixedSL_Pts > 0) ? _ORB_FixedSL_Pts : range_pts;
      double tp_dist_pts = sl_dist_pts * _ORB_RR_Ratio;
      double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
      double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);

      double totalLots = CalcLots(sl_dist_pts);
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

      double buyEntry = NormPrice(s.range_high + _ORB_BufferPts * _Point);
      double minBuy   = NormPrice(ask + stopsLvl * _Point);
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
            g_trade.BuyStop(totalLots, buyEntry, _Symbol, sl, tp,
                            ORDER_TIME_SPECIFIED, s.pending_expire, _ORB_Comment);
      }
      double sellEntry = NormPrice(s.range_low - _ORB_BufferPts * _Point);
      double maxSell   = NormPrice(bid - stopsLvl * _Point);
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
            g_trade.SellStop(totalLots, sellEntry, _Symbol, sl, tp,
                             ORDER_TIME_SPECIFIED, s.pending_expire, _ORB_Comment);
      }
      s.fired = true;
   }

   if(now >= s.pending_expire)
   {
      CancelPendingByMagic(_ORB_Magic);
      s.active = false;
   }
}

void ProcessORB()
{
   datetime now = TimeCurrent();
   MqlDateTime mt; TimeToStruct(now, mt);
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
// LSFVG stream processor (port from LSFVG.mq5)
// Detection on signal-TF close: liquidity sweep (recent wick beyond pool)
// + 3-bar bearish/bullish FVG → SellLimit / BuyLimit at FVG edge.
//====================================================================
void ProcessLSFVG()
{
   ENUM_TIMEFRAMES tf = TF(_LSFVG_SignalTF);
   int needed = 3 + _LSFVG_LookbackBars + 1;
   if(Bars(_Symbol, tf) < needed) return;
   if(HasPendingByMagicComment(_LSFVG_Magic, _LSFVG_Comment)) return;
   if(CountPositionsByMagicComment(_LSFVG_Magic, _LSFVG_Comment) > 0) return;

   double ema = 0.0;
   if(_LSFVG_UseEMAFilter)
   {
      ema = GetEMA(g_emaHandle_LSFVG, 1);
      if(ema <= 0) return;
   }

   double pool_high = 0.0, pool_low = DBL_MAX;
   for(int i = 3; i < 3 + _LSFVG_LookbackBars; i++)
   {
      double hi = iHigh(_Symbol, tf, i);
      double lo = iLow(_Symbol, tf, i);
      if(hi > pool_high) pool_high = hi;
      if(lo < pool_low)  pool_low  = lo;
   }
   if(pool_high <= 0 || pool_low >= DBL_MAX) return;

   double h1 = iHigh(_Symbol, tf, 1), l1 = iLow(_Symbol, tf, 1), c1 = iClose(_Symbol, tf, 1);
   double h2 = iHigh(_Symbol, tf, 2), l2 = iLow(_Symbol, tf, 2);
   double h3 = iHigh(_Symbol, tf, 3), l3 = iLow(_Symbol, tf, 3);
   double sweep_high = MathMax(h1, h2);
   double sweep_low  = MathMin(l1, l2);

   datetime expiry = iTime(_Symbol, tf, 0) + _LSFVG_PendingExpireBars * PeriodSeconds(tf);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);
   double stops_pad = stopsLvl * _Point;

   g_trade.SetExpertMagicNumber(_LSFVG_Magic);

   // ===== BEARISH (SellLimit) =====
   bool bearish_ok = (sweep_high > pool_high) && (c1 < pool_high) && (l3 > h1)
                  && (!_LSFVG_UseEMAFilter || c1 < ema);
   if(bearish_ok)
   {
      double fvg_pts = (l3 - h1) / _Point;
      if(fvg_pts >= _LSFVG_MinFVGPts && fvg_pts <= _LSFVG_MaxFVGPts)
      {
         double entry = NormPrice(l3);
         double sl    = NormPrice(sweep_high + _LSFVG_SweepBufferPts * _Point);
         double sl_dist = sl - entry;
         if(sl_dist > 0 && entry >= bid + stops_pad)
         {
            double tp = NormPrice(entry - sl_dist * _LSFVG_RR_Ratio);
            double sl_pts = sl_dist / _Point;
            double totalLots = CalcLots(sl_pts);
            if(totalLots > 0)
            {
               double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
               double minLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
               if(lotStep <= 0) lotStep = 0.01;
               double halfLots = MathRound(totalLots / 2.0 / lotStep) * lotStep;
               if(halfLots < minLot) halfLots = minLot;

               if(_LSFVG_HalfTP_Ratio > 0)
               {
                  double tp_half = NormPrice(entry - sl_dist * _LSFVG_RR_Ratio * _LSFVG_HalfTP_Ratio);
                  g_trade.SellLimit(halfLots, entry, _Symbol, sl, tp_half,
                                    ORDER_TIME_SPECIFIED, expiry, _LSFVG_Comment);
                  g_trade.SellLimit(halfLots, entry, _Symbol, sl, tp,
                                    ORDER_TIME_SPECIFIED, expiry, _LSFVG_Comment);
               }
               else
                  g_trade.SellLimit(totalLots, entry, _Symbol, sl, tp,
                                    ORDER_TIME_SPECIFIED, expiry, _LSFVG_Comment);
               return;
            }
         }
      }
   }

   // ===== BULLISH (BuyLimit) =====
   bool bullish_ok = (sweep_low < pool_low) && (c1 > pool_low) && (h3 < l1)
                  && (!_LSFVG_UseEMAFilter || c1 > ema);
   if(bullish_ok)
   {
      double fvg_pts = (l1 - h3) / _Point;
      if(fvg_pts >= _LSFVG_MinFVGPts && fvg_pts <= _LSFVG_MaxFVGPts)
      {
         double entry = NormPrice(h3);
         double sl    = NormPrice(sweep_low - _LSFVG_SweepBufferPts * _Point);
         double sl_dist = entry - sl;
         if(sl_dist > 0 && entry <= ask - stops_pad)
         {
            double tp = NormPrice(entry + sl_dist * _LSFVG_RR_Ratio);
            double sl_pts = sl_dist / _Point;
            double totalLots = CalcLots(sl_pts);
            if(totalLots > 0)
            {
               double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
               double minLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
               if(lotStep <= 0) lotStep = 0.01;
               double halfLots = MathRound(totalLots / 2.0 / lotStep) * lotStep;
               if(halfLots < minLot) halfLots = minLot;

               if(_LSFVG_HalfTP_Ratio > 0)
               {
                  double tp_half = NormPrice(entry + sl_dist * _LSFVG_RR_Ratio * _LSFVG_HalfTP_Ratio);
                  g_trade.BuyLimit(halfLots, entry, _Symbol, sl, tp_half,
                                   ORDER_TIME_SPECIFIED, expiry, _LSFVG_Comment);
                  g_trade.BuyLimit(halfLots, entry, _Symbol, sl, tp,
                                   ORDER_TIME_SPECIFIED, expiry, _LSFVG_Comment);
               }
               else
                  g_trade.BuyLimit(totalLots, entry, _Symbol, sl, tp,
                                   ORDER_TIME_SPECIFIED, expiry, _LSFVG_Comment);
            }
         }
      }
   }
}

//====================================================================
// EMAPullback stream processor (port from ema_pullback.py)
// On signal-TF bar close (M15): if trend (close[1] > EMA[1]) and pullback
// (min(low[1..lookback]) <= EMA + band) and bullish reversal (close > open
// on bar 1), place BuyStop at high[1] + entry_buffer with SL at win_lo -
// sl_buffer and TP at entry + (entry-sl)*RR. Mirror for shorts.
//====================================================================
void ProcessEMAPullback()
{
   ENUM_TIMEFRAMES tf = TF(_EMP_SignalTF);
   int needed = _EMP_LookbackBars + 2;
   if(Bars(_Symbol, tf) < needed) return;
   if(HasPendingByMagicComment(_EMP_Magic, _EMP_Comment)) return;
   if(CountPositionsByMagicComment(_EMP_Magic, _EMP_Comment) > 0) return;

   double ema = GetEMA(g_emaHandle_EMP, 1);
   if(ema <= 0) return;

   double o1 = iOpen(_Symbol, tf, 1);
   double h1 = iHigh(_Symbol, tf, 1);
   double l1 = iLow(_Symbol, tf, 1);
   double c1 = iClose(_Symbol, tf, 1);

   // Window low/high over bars [1..lookback]
   double win_lo =  DBL_MAX;
   double win_hi = -DBL_MAX;
   for(int i = 1; i <= _EMP_LookbackBars; i++)
   {
      double hi = iHigh(_Symbol, tf, i);
      double lo = iLow(_Symbol, tf, i);
      if(hi > win_hi) win_hi = hi;
      if(lo < win_lo) win_lo = lo;
   }
   if(win_lo == DBL_MAX || win_hi == -DBL_MAX) return;

   datetime expiry = iTime(_Symbol, tf, 0) + _EMP_PendingExpireBars * PeriodSeconds(tf);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);
   double stops_pad = stopsLvl * _Point;

   g_trade.SetExpertMagicNumber(_EMP_Magic);

   double band = _EMP_PullbackBandPts * _Point;

   // ===== LONG (BuyStop) =====
   bool long_ok = (c1 > ema) && (c1 > o1) && (win_lo <= ema + band);
   if(long_ok)
   {
      double entry = NormPrice(h1 + _EMP_EntryBufferPts * _Point);
      double sl    = NormPrice(win_lo - _EMP_SLBufferPts * _Point);
      if(sl < entry && entry > ask + stops_pad)
      {
         double sl_dist = entry - sl;
         double sl_pts  = sl_dist / _Point;
         double tp = NormPrice(entry + sl_dist * _EMP_RR_Ratio);
         double totalLots = CalcLots(sl_pts);
         if(totalLots > 0)
         {
            double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
            double minLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
            if(lotStep <= 0) lotStep = 0.01;
            double halfLots = MathRound(totalLots / 2.0 / lotStep) * lotStep;
            if(halfLots < minLot) halfLots = minLot;

            if(_EMP_HalfTP_Ratio > 0)
            {
               double tp_half = NormPrice(entry + sl_dist * _EMP_RR_Ratio * _EMP_HalfTP_Ratio);
               g_trade.BuyStop(halfLots, entry, _Symbol, sl, tp_half,
                               ORDER_TIME_SPECIFIED, expiry, _EMP_Comment);
               g_trade.BuyStop(halfLots, entry, _Symbol, sl, tp,
                               ORDER_TIME_SPECIFIED, expiry, _EMP_Comment);
            }
            else
               g_trade.BuyStop(totalLots, entry, _Symbol, sl, tp,
                               ORDER_TIME_SPECIFIED, expiry, _EMP_Comment);
            return;
         }
      }
   }

   // ===== SHORT (SellStop) =====
   bool short_ok = (c1 < ema) && (c1 < o1) && (win_hi >= ema - band);
   if(short_ok)
   {
      double entry = NormPrice(l1 - _EMP_EntryBufferPts * _Point);
      double sl    = NormPrice(win_hi + _EMP_SLBufferPts * _Point);
      if(sl > entry && entry < bid - stops_pad)
      {
         double sl_dist = sl - entry;
         double sl_pts  = sl_dist / _Point;
         double tp = NormPrice(entry - sl_dist * _EMP_RR_Ratio);
         double totalLots = CalcLots(sl_pts);
         if(totalLots > 0)
         {
            double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
            double minLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
            if(lotStep <= 0) lotStep = 0.01;
            double halfLots = MathRound(totalLots / 2.0 / lotStep) * lotStep;
            if(halfLots < minLot) halfLots = minLot;

            if(_EMP_HalfTP_Ratio > 0)
            {
               double tp_half = NormPrice(entry - sl_dist * _EMP_RR_Ratio * _EMP_HalfTP_Ratio);
               g_trade.SellStop(halfLots, entry, _Symbol, sl, tp_half,
                                ORDER_TIME_SPECIFIED, expiry, _EMP_Comment);
               g_trade.SellStop(halfLots, entry, _Symbol, sl, tp,
                                ORDER_TIME_SPECIFIED, expiry, _EMP_Comment);
            }
            else
               g_trade.SellStop(totalLots, entry, _Symbol, sl, tp,
                                ORDER_TIME_SPECIFIED, expiry, _EMP_Comment);
         }
      }
   }
}

//====================================================================
// OnInit
//====================================================================
int OnInit()
{
   g_trade.SetExpertMagicNumber(_FBO_Magic);
   g_trade.SetDeviationInPoints(50);

   if(_FBO1)
   {
      g_emaHandle_FBO1 = iMA(_Symbol, TF(_FBO1_TF), _FBO1_SMA, 0, MODE_SMA, PRICE_CLOSE);
      if(g_emaHandle_FBO1 == INVALID_HANDLE) return INIT_FAILED;
   }
   if(_FBO2)
   {
      g_emaHandle_FBO2 = iMA(_Symbol, TF(_FBO2_TF), _FBO2_SMA, 0, MODE_SMA, PRICE_CLOSE);
      if(g_emaHandle_FBO2 == INVALID_HANDLE) return INIT_FAILED;
   }
   if(_LSFVG_Enabled && _LSFVG_UseEMAFilter)
   {
      g_emaHandle_LSFVG = iMA(_Symbol, TF(_LSFVG_SignalTF), _LSFVG_EMA_Period,
                              0, MODE_SMA, PRICE_CLOSE);
      if(g_emaHandle_LSFVG == INVALID_HANDLE) return INIT_FAILED;
   }
   if(_EMP_Enabled)
   {
      g_emaHandle_EMP = iMA(_Symbol, TF(_EMP_SignalTF), _EMP_EMA_Period,
                            0, MODE_EMA, PRICE_CLOSE);
      if(g_emaHandle_EMP == INVALID_HANDLE) return INIT_FAILED;
   }

   g_lastBar_M1 = iTime(_Symbol, PERIOD_M1, 0);

   double bal = AccountInfoDouble(ACCOUNT_BALANCE);
   g_fbo_bal_start = bal;
   g_orb_bal_start = bal;
   g_lsf_bal_start = bal;
   g_emp_bal_start = bal;

   PrintFormat("[DT818_pro] Init complete. FBO=%d/%d ORB=%d LSFVG=%d EMP=%d  "
               "DailyCaps: FBO=%.1f/%.1f ORB=%.1f/%.1f LSFVG=%.1f/%.1f EMP=%.1f/%.1f",
               _FBO1, _FBO2, _ORB_Enabled, _LSFVG_Enabled, _EMP_Enabled,
               _FBO_DailyTargetPct, _FBO_DailyLossPct,
               _ORB_DailyTargetPct, _ORB_DailyLossPct,
               _LSFVG_DailyTargetPct, _LSFVG_DailyLossPct,
               _EMP_DailyTargetPct, _EMP_DailyLossPct);
   return INIT_SUCCEEDED;
}

//====================================================================
// OnDeinit
//====================================================================
void OnDeinit(const int reason)
{
   if(g_emaHandle_FBO1  != INVALID_HANDLE) IndicatorRelease(g_emaHandle_FBO1);
   if(g_emaHandle_FBO2  != INVALID_HANDLE) IndicatorRelease(g_emaHandle_FBO2);
   if(g_emaHandle_LSFVG != INVALID_HANDLE) IndicatorRelease(g_emaHandle_LSFVG);
   if(g_emaHandle_EMP   != INVALID_HANDLE) IndicatorRelease(g_emaHandle_EMP);
}

//====================================================================
// OnTick
//====================================================================
void OnTick()
{
   if(_CapitalProtectionAmount > 0 &&
      AccountInfoDouble(ACCOUNT_EQUITY) < _CapitalProtectionAmount)
      return;

   // Independent per-stream daily-cap checks. Each one only touches its own magic.
   bool fbo_locked = StreamDailyCapsCheck(_FBO_Magic, _FBO_DailyTargetPct, _FBO_DailyLossPct,
                                          g_fbo_day, g_fbo_bal_start, g_fbo_lock);
   bool orb_locked = StreamDailyCapsCheck(_ORB_Magic, _ORB_DailyTargetPct, _ORB_DailyLossPct,
                                          g_orb_day, g_orb_bal_start, g_orb_lock);
   bool lsf_locked = StreamDailyCapsCheck(_LSFVG_Magic, _LSFVG_DailyTargetPct, _LSFVG_DailyLossPct,
                                          g_lsf_day, g_lsf_bal_start, g_lsf_lock);
   bool emp_locked = StreamDailyCapsCheck(_EMP_Magic, _EMP_DailyTargetPct, _EMP_DailyLossPct,
                                          g_emp_day, g_emp_bal_start, g_emp_lock);

   // Stream processing only on new M1 bar (FBO + LSFVG + EMP bar-event timing)
   if(!IsNewBar(PERIOD_M1, g_lastBar_M1)) return;

   if(!fbo_locked)
   {
      if(_FBO1)
         ProcessFBO(TF(_FBO1_TF), _FBO1_Bars, _FBO1_TP, _FBO1_SL,
                    _FBO1_HalfTP, _FBO1_Comment, g_emaHandle_FBO1, _PendingExpireBars1);
      if(_FBO2)
         ProcessFBO(TF(_FBO2_TF), _FBO2_Bars, _FBO2_TP, _FBO2_SL,
                    _FBO2_HalfTP, _FBO2_Comment, g_emaHandle_FBO2, _PendingExpireBars2);
   }

   if(_ORB_Enabled && !orb_locked)
      ProcessORB();

   if(_LSFVG_Enabled && !lsf_locked)
      ProcessLSFVG();

   if(_EMP_Enabled && !emp_locked)
      ProcessEMAPullback();
}
//+------------------------------------------------------------------+

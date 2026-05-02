//+------------------------------------------------------------------+
//| LSFVG.mq5                                                         |
//| Liquidity Sweep + Fair Value Gap (SMC-style stop-hunt + imbalance)|
//|                                                                   |
//| Setup (bearish, mirror for bullish):                              |
//|   1. Liquidity pool = highest high of bars [3..3+lookback]        |
//|   2. Sweep      = max(high[1], high[2]) > pool_high               |
//|                   AND close[1] < pool_high  (wick hunted, closed  |
//|                   back inside)                                    |
//|   3. FVG        = low[3] > high[1]  (3-bar bearish imbalance)     |
//|   4. Entry      = SellLimit at low[3] (top of FVG)                |
//|   5. SL         = max(high[1], high[2]) + buffer                  |
//|   6. TP         = entry - (SL-entry) * RR                         |
//|                                                                   |
//| Pending expires after _PendingExpireBars signal-TF bars.          |
//| One position per direction; stream idle while position open.      |
//+------------------------------------------------------------------+
#property copyright "ZGB Trading"
#property version   "1.00"
#property strict

#include <Trade/Trade.mqh>

CTrade g_trade;

//====================================================================
// Inputs
//====================================================================
input int     _BaseMagic               = 3000;   // Magic (avoid 1000=FBO, 2000=ORB)
input double  _CapitalProtectionAmount = 0.0;
input double  _RiskPct                 = 3.0;    // Risk % per trade
input int     _LotMode                 = 1;      // 0=fixed, 1=risk%, 2=tiered
input int     TierBase                 = 2000;
input double  LotStep                  = 0.01;

input string  _OrderComment            = "LSFVG";
input int     _SignalTF                = 15;     // M15
input int     _LookbackBars            = 15;     // Liquidity pool lookback (bars before sweep window)
input int     _MinFVGPts               = 100;    // Skip FVG smaller than this (filter noise)
input int     _MaxFVGPts               = 3000;   // Skip FVG larger than this (likely news spike, untradable)
input int     _SweepBufferPts          = 30;     // SL = sweep_high + buffer
input double  _RR_Ratio                = 2.0;    // TP = SL_dist * RR
input double  _HalfTP_Ratio            = 0.5;    // Split TP at this fraction (0=off)
input int     _PendingExpireBars       = 4;      // Pending order expiry (signal-TF bars)

// Optional EMA trend filter (skip counter-trend setups)
input bool    _UseEMAFilter            = false;
input int     _EMA_Period              = 50;     // Only sell below EMA, buy above

// Daily caps (account-level, applies to LSFVG only)
input double  _DailyTargetPct          = 0.0;    // 0 disables
input double  _DailyLossPct            = 0.0;    // 0 disables

//====================================================================
// Globals
//====================================================================
datetime g_lastBar = 0;
int      g_emaHandle = INVALID_HANDLE;

datetime g_day_start = 0;
double   g_balance_day_start = 0;
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
// Position / Order Queries
//====================================================================
int CountPositions()
{
   int count = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0 || !PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _BaseMagic) continue;
      if(PositionGetString(POSITION_COMMENT) != _OrderComment) continue;
      count++;
   }
   return count;
}

bool HasPending()
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0 || !OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != _BaseMagic) continue;
      if(OrderGetString(ORDER_COMMENT) != _OrderComment) continue;
      return true;
   }
   return false;
}

void CancelAllPending()
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0 || !OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != _BaseMagic) continue;
      if(OrderGetString(ORDER_COMMENT) != _OrderComment) continue;
      g_trade.OrderDelete(t);
   }
}

void CloseAllPositions()
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0 || !PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _BaseMagic) continue;
      if(PositionGetString(POSITION_COMMENT) != _OrderComment) continue;
      g_trade.PositionClose(t);
   }
}

//====================================================================
// Daily caps (LSFVG-only — only counts deals/positions of this stream)
//====================================================================
double LSFVGRealizedToday()
{
   if(!HistorySelect(g_day_start, TimeCurrent())) return 0.0;
   double sum = 0.0;
   int total = HistoryDealsTotal();
   for(int i = 0; i < total; i++)
   {
      ulong tk = HistoryDealGetTicket(i);
      if(tk == 0) continue;
      if((int)HistoryDealGetInteger(tk, DEAL_MAGIC) != _BaseMagic) continue;
      if(HistoryDealGetString(tk, DEAL_SYMBOL) != _Symbol) continue;
      if(HistoryDealGetString(tk, DEAL_COMMENT) != _OrderComment &&
         StringFind(HistoryDealGetString(tk, DEAL_COMMENT), _OrderComment) < 0) continue;
      sum += HistoryDealGetDouble(tk, DEAL_PROFIT)
           + HistoryDealGetDouble(tk, DEAL_SWAP)
           + HistoryDealGetDouble(tk, DEAL_COMMISSION);
   }
   return sum;
}

double LSFVGUnrealized()
{
   double sum = 0.0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0 || !PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _BaseMagic) continue;
      if(PositionGetString(POSITION_COMMENT) != _OrderComment) continue;
      sum += PositionGetDouble(POSITION_PROFIT)
           + PositionGetDouble(POSITION_SWAP);
   }
   return sum;
}

bool DailyCapsCheck()
{
   datetime now = TimeCurrent();
   MqlDateTime mt; TimeToStruct(now, mt);
   mt.hour = 0; mt.min = 0; mt.sec = 0;
   datetime today_start = StructToTime(mt);

   if(today_start != g_day_start)
   {
      g_day_start = today_start;
      g_balance_day_start = AccountInfoDouble(ACCOUNT_BALANCE);
      g_daily_lock = false;
   }

   if(g_daily_lock) return true;
   if(_DailyTargetPct <= 0 && _DailyLossPct <= 0) return false;

   double pnl_today = LSFVGRealizedToday() + LSFVGUnrealized();
   double pct = (g_balance_day_start > 0) ? (pnl_today / g_balance_day_start * 100.0) : 0.0;

   bool target_hit = (_DailyTargetPct > 0 && pct >= _DailyTargetPct);
   bool loss_hit   = (_DailyLossPct   > 0 && pct <= -_DailyLossPct);

   if(target_hit || loss_hit)
   {
      CloseAllPositions();
      CancelAllPending();
      g_daily_lock = true;
      PrintFormat("[LSFVG] Daily %s hit at %.2f%%, locking for the day.",
                  target_hit ? "target" : "loss", pct);
      return true;
   }
   return false;
}

//====================================================================
// Detection: Liquidity Sweep + FVG on signal TF
// Called on every new signal-TF bar close. Looks at bars [1..3+lookback].
//
//   bar idx:   0    1    2    3    4 ...  3+lookback
//              |    |    |    |    |
//             new  disp swp  ref  pool lookback range
//
// Bearish setup:
//   pool_high = max(high[3..3+lookback])
//   sweep:    max(high[1], high[2]) > pool_high AND close[1] < pool_high
//   FVG:      low[3] > high[1] AND (low[3]-high[1]) within [_MinFVGPts, _MaxFVGPts]
//   entry:    SellLimit at low[3]
//   SL:       max(high[1], high[2]) + _SweepBufferPts
//
// Bullish setup mirrors.
//====================================================================
void ProcessLSFVG()
{
   ENUM_TIMEFRAMES tf = TF(_SignalTF);
   int needed = 3 + _LookbackBars + 1;
   if(Bars(_Symbol, tf) < needed) return;

   // EMA filter
   double ema = 0.0;
   if(_UseEMAFilter)
   {
      ema = GetEMA(g_emaHandle, 1);
      if(ema <= 0) return;
   }

   // ---- Pool levels (bars 3..3+lookback) ----
   double pool_high = 0.0, pool_low = DBL_MAX;
   for(int i = 3; i < 3 + _LookbackBars; i++)
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

   datetime expiry = iTime(_Symbol, tf, 0) + _PendingExpireBars * PeriodSeconds(tf);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);
   double stops_pad = stopsLvl * _Point;

   g_trade.SetExpertMagicNumber(_BaseMagic);

   // ===== BEARISH (sell) =====
   bool bearish_ok = (sweep_high > pool_high) && (c1 < pool_high)
                  && (l3 > h1)
                  && (!_UseEMAFilter || c1 < ema);
   if(bearish_ok)
   {
      double fvg_pts = (l3 - h1) / _Point;
      if(fvg_pts >= _MinFVGPts && fvg_pts <= _MaxFVGPts)
      {
         double entry = NormPrice(l3);
         double sl    = NormPrice(sweep_high + _SweepBufferPts * _Point);
         double sl_dist = sl - entry;
         // SellLimit must be placed ABOVE bid + stops_level (otherwise broker rejects)
         if(sl_dist > 0 && entry >= bid + stops_pad)
         {
            double tp = NormPrice(entry - sl_dist * _RR_Ratio);
            double sl_pts = sl_dist / _Point;
            double totalLots = CalcLots(sl_pts);
            if(totalLots > 0)
            {
               double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
               double minLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
               if(lotStep <= 0) lotStep = 0.01;
               double halfLots = MathRound(totalLots / 2.0 / lotStep) * lotStep;
               if(halfLots < minLot) halfLots = minLot;

               if(_HalfTP_Ratio > 0)
               {
                  double tp_half = NormPrice(entry - sl_dist * _RR_Ratio * _HalfTP_Ratio);
                  g_trade.SellLimit(halfLots, entry, _Symbol, sl, tp_half,
                                    ORDER_TIME_SPECIFIED, expiry, _OrderComment);
                  g_trade.SellLimit(halfLots, entry, _Symbol, sl, tp,
                                    ORDER_TIME_SPECIFIED, expiry, _OrderComment);
               }
               else
               {
                  g_trade.SellLimit(totalLots, entry, _Symbol, sl, tp,
                                    ORDER_TIME_SPECIFIED, expiry, _OrderComment);
               }
               return;  // one direction per bar
            }
         }
      }
   }

   // ===== BULLISH (buy) =====
   bool bullish_ok = (sweep_low < pool_low) && (c1 > pool_low)
                  && (h3 < l1)
                  && (!_UseEMAFilter || c1 > ema);
   if(bullish_ok)
   {
      double fvg_pts = (l1 - h3) / _Point;
      if(fvg_pts >= _MinFVGPts && fvg_pts <= _MaxFVGPts)
      {
         double entry = NormPrice(h3);
         double sl    = NormPrice(sweep_low - _SweepBufferPts * _Point);
         double sl_dist = entry - sl;
         if(sl_dist > 0 && entry <= ask - stops_pad)  // BuyLimit must be below ask
         {
            double tp = NormPrice(entry + sl_dist * _RR_Ratio);
            double sl_pts = sl_dist / _Point;
            double totalLots = CalcLots(sl_pts);
            if(totalLots > 0)
            {
               double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
               double minLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
               if(lotStep <= 0) lotStep = 0.01;
               double halfLots = MathRound(totalLots / 2.0 / lotStep) * lotStep;
               if(halfLots < minLot) halfLots = minLot;

               if(_HalfTP_Ratio > 0)
               {
                  double tp_half = NormPrice(entry + sl_dist * _RR_Ratio * _HalfTP_Ratio);
                  g_trade.BuyLimit(halfLots, entry, _Symbol, sl, tp_half,
                                   ORDER_TIME_SPECIFIED, expiry, _OrderComment);
                  g_trade.BuyLimit(halfLots, entry, _Symbol, sl, tp,
                                   ORDER_TIME_SPECIFIED, expiry, _OrderComment);
               }
               else
               {
                  g_trade.BuyLimit(totalLots, entry, _Symbol, sl, tp,
                                   ORDER_TIME_SPECIFIED, expiry, _OrderComment);
               }
            }
         }
      }
   }
}

//====================================================================
// OnInit
//====================================================================
int OnInit()
{
   g_trade.SetExpertMagicNumber(_BaseMagic);
   g_trade.SetDeviationInPoints(50);

   if(_UseEMAFilter)
   {
      g_emaHandle = iMA(_Symbol, TF(_SignalTF), _EMA_Period, 0, MODE_SMA, PRICE_CLOSE);
      if(g_emaHandle == INVALID_HANDLE) return INIT_FAILED;
   }

   g_lastBar = iTime(_Symbol, TF(_SignalTF), 0);
   g_balance_day_start = AccountInfoDouble(ACCOUNT_BALANCE);

   PrintFormat("[LSFVG] Init: TF=M%d Lookback=%d MinFVG=%dpts MaxFVG=%dpts RR=%.1f Risk=%.1f%%",
               _SignalTF, _LookbackBars, _MinFVGPts, _MaxFVGPts, _RR_Ratio, _RiskPct);
   return INIT_SUCCEEDED;
}

//====================================================================
// OnDeinit
//====================================================================
void OnDeinit(const int reason)
{
   if(g_emaHandle != INVALID_HANDLE) IndicatorRelease(g_emaHandle);
}

//====================================================================
// OnTick
//====================================================================
void OnTick()
{
   bool locked = DailyCapsCheck();
   if(locked) return;

   if(_CapitalProtectionAmount > 0 &&
      AccountInfoDouble(ACCOUNT_EQUITY) < _CapitalProtectionAmount)
      return;

   if(!IsNewBar(TF(_SignalTF), g_lastBar)) return;

   // Stream is single-direction-at-a-time: skip if already engaged
   if(HasPending()) return;
   if(CountPositions() > 0) return;

   ProcessLSFVG();
}
//+------------------------------------------------------------------+

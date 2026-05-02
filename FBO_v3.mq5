//+------------------------------------------------------------------+
//| FBO_v3.mq5                                                        |
//| Fractal Breakout multi-stream EA                                  |
//| 2 FBO streams (stop orders @ custom fractal + SMA filter)         |
//| Renamed from FBO_FVG_v2.mq5 Apr 26 — FVG removed (failed WFO).    |
//+------------------------------------------------------------------+
#property copyright "ZGB Trading"
#property version   "3.00"
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
// (FVG S1+S2 removed Apr 26 — FVG S1 failed gate; FVG S2 dominated by raising
//  FBO risk_pct (DD-matched comparison: FBO@4.5% beats FBO+FVG@3% by $789).)

//====================================================================
// Globals
//====================================================================
datetime g_lastBar_FBO_M1 = 0;  // shared M1 bar tracker for all FBO

int g_emaHandle_FBO1 = INVALID_HANDLE;
int g_emaHandle_FBO2 = INVALID_HANDLE;


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
// OnInit
//====================================================================
int OnInit()
{
   g_trade.SetExpertMagicNumber(_BaseMagic);
   g_trade.SetDeviationInPoints(50);

   // SMA handles on signal TF for FBO direction
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
   // FBO streams — all process on every M1 bar
   if(IsNewBar(PERIOD_M1, g_lastBar_FBO_M1))
   {
      if(_FBO1)
         ProcessFBO(TF(_time_frame), _Bars, _take_profit, _stop_loss,
                    _HalfTP1, _OrderComment, g_emaHandle_FBO1, _PendingExpireBars1);

      if(_FBO2)
         ProcessFBO(TF(_time_frame2), _Bars2, _take_profit2, _stop_loss2,
                    _HalfTP2, _OrderComment2, g_emaHandle_FBO2, _PendingExpireBars2);
   }
}
//+------------------------------------------------------------------+

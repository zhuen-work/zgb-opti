//+------------------------------------------------------------------+
//| XAU_TPB_Cleaned_NoATR_NoGlobalBE.mq5                             |
//| MA Pullback + optional modules (Fractal, BB Exact)               |
//| CLEANED: global ATR stoploss and global break-even removed       |
//+------------------------------------------------------------------+
#property strict

#include <Trade/Trade.mqh>
CTrade trade;

//====================================================================
// Inputs
//====================================================================
// Main trading (GLOBAL)
input ulong   InpMagic                 = 250121;

//---------------- Strategy 1: SMA Pullback (Limit) -------------------
input bool    InpEnable_S1             = true;   // Strategy 1 (SMA pullback)
input double  InpRiskPercent_S1        = 2.0;
input double  InpRR_S1                 = 1.5;
input int     InpMaxSpreadPoints_S1    = 80;

// Signal timeframe (S1)
input ENUM_TIMEFRAMES InpSignalTF_S1   = PERIOD_M5;

// MA logic (S1)
input int     InpSMA_Fast_S1           = 7;
input int     InpSMA_Mid_S1            = 20;
input int     InpSMA_Slow_S1           = 54;

// Structure SL (S1)
input int     InpSwingLookbackBars_S1  = 20;
input int     InpSL_BufferPoints_S1    = 30;
input int     InpMinSLPoints_S1        = 80;
input int     InpMaxSLPoints_S1        = 2000;

// Order handling (S1)
input int     InpEntryOffsetPoints_S1     = 0;
input bool    InpRefreshPendingEachBar_S1 = true;

// Comment (S1)
input string  InpTradeComment_S1       = "XAU_SMA_Pullback_Limit";

//---------------- Strategy 2: Reverse Stop Module (optional) ----------
input bool    InpEnableReverseStops_S2 = false;
input ulong   InpReverseMagic_S2       = 250122;

input double  InpReverseRiskPercent_S2 = 1.0;
input double  InpReverseRR_S2          = 1.2;

// Structure SL (S2)
input int     InpReverseSwingLookback_S2   = 20;
input int     InpReverseSL_BufferPoints_S2 = 30;
input int     InpReverseMinSLPoints_S2     = 80;
input int     InpReverseMaxSLPoints_S2     = 2000;

// Comment (S2)
input string  InpReverseComment_S2     = "XAU_ReverseStops";

//---------------- Strategy 3: Fractal Stops + SMA Filter (optional) ----
input bool    InpEnableFractalStops_S3 = false;
input ulong   InpFractalMagic_S3       = 250123;

input double  InpFractalRiskPercent_S3 = 2.0;
input double  InpFractalRR_S3          = 1.5;
input int     InpFractalMaxSpreadPts_S3 = 80;

// Signal timeframe (S3)
input ENUM_TIMEFRAMES InpFractalSignalTF_S3 = PERIOD_M5;

// MA filter (S3)
input int     InpFractalSMA_FilterPeriod_S3 = 54;

// Structure SL (S3)
input int     InpFractalSwingLookbackBars_S3 = 20;
input int     InpFractalSL_BufferPoints_S3   = 30;
input int     InpFractalMinSLPoints_S3       = 80;
input int     InpFractalMaxSLPoints_S3       = 2000;

// Entry / pending handling (S3)
input int     InpFractalEntryOffsetPoints_S3     = 0;
input bool    InpFractalRefreshPendingEachBar_S3 = true;
input int     InpFractalSearchBars_S3            = 200;

// Comment (S3)
input string  InpFractalTradeComment_S3      = "XAU_FractalStops_MA";

//---------------- Strategy 4: BB MID Pullback (Limit @ MID) ---------------------
input bool    InpEnableBBMid_S4           = false;
input ulong   InpBBMidMagic_S4            = 250124;

input double  InpBBMidRiskPercent_S4      = 2.0;
input double  InpBBMidRR_S4               = 1.5;
input int     InpBBMidMaxSpreadPts_S4     = 80;

// Signal timeframe (BB strategy)
input ENUM_TIMEFRAMES InpBBMidSignalTF_S4 = PERIOD_M5;

// Bollinger parameters
input int     InpBB_Period_S4             = 20;
input double  InpBB_Deviation_S4          = 2.0;
input ENUM_APPLIED_PRICE InpBB_Price_S4   = PRICE_CLOSE;

// Structure SL
input int     InpBBMidSwingLookback_S4    = 20;
input int     InpBBMidSL_BufferPts_S4     = 30;
input int     InpBBMidMinSLPoints_S4      = 80;
input int     InpBBMidMaxSLPoints_S4      = 2000;

// Entry / pending handling
input int     InpBBMidEntryOffsetPts_S4   = 0;
input bool    InpBBMidRefreshEachBar_S4   = true;

input string  InpBBMidComment_S4          = "ZZBB_S4";

//---------------- Strategy 5: BB MID Pullback (Inverse / Stop @ MID) -------------
input bool    InpEnableBBMidInv_S5         = false;
input ulong   InpBBMidInvMagic_S5          = 250125;

input double  InpBBMidInvRiskPercent_S5    = 1.0;
input double  InpBBMidInvRR_S5             = 1.2;
input int     InpBBMidInvMaxSpreadPts_S5   = 80;

// Signal timeframe (BB inverse)
input ENUM_TIMEFRAMES InpBBMidSignalTF_S5  = PERIOD_M5;

// Structure SL
input int     InpBBMidInvSwingLookback_S5  = 20;
input int     InpBBMidInvSL_BufferPts_S5   = 30;
input int     InpBBMidInvMinSLPoints_S5    = 80;
input int     InpBBMidInvMaxSLPoints_S5    = 2000;

// Entry / pending handling
input int     InpBBMidInvEntryOffsetPts_S5 = 0;
input bool    InpBBMidInvRefreshEachBar_S5 = true;

input string  InpBBMidInvComment_S5        = "ZZBB_S5";

//---------------- Regime Filter: BB Width ----------------------------
input bool            InpEnableBBWidthFilter   = false;       // Pause S1/S2/S3/S5 when market trending
input ENUM_TIMEFRAMES InpBBWidth_TF            = PERIOD_M15;  // Timeframe for regime BB
input int             InpBBWidth_Period        = 50;          // Period for regime BB
input double          InpBBWidth_Deviation     = 2.0;         // Deviation for regime BB
input int             InpBBWidthMaxPoints      = 2000;        // Max BB width (points); above = trending, strategies paused

//====================================================================
// Globals
//====================================================================
datetime g_lastSignalBarTime_S1  = 0;
datetime g_lastFractalBarTime_S3 = 0;

// Strategy 3 fractals
int g_fractalsHandle = INVALID_HANDLE;

// Strategy 4/5 bollinger handle (shared)
datetime g_lastBBMidBarTime_S4 = 0;
int g_bbandsHandle_S4 = INVALID_HANDLE;

// Regime filter BB handle
int g_bbwidthHandle = INVALID_HANDLE;

//====================================================================
// Regime Filter
//====================================================================
bool IsRegimeTrending()
{
   if(!InpEnableBBWidthFilter) return false;
   if(g_bbwidthHandle == INVALID_HANDLE) return false;

   double upper[], lower[];
   ArraySetAsSeries(upper, true);
   ArraySetAsSeries(lower, true);
   if(CopyBuffer(g_bbwidthHandle, 0, 1, 1, upper) != 1) return false;
   if(CopyBuffer(g_bbwidthHandle, 2, 1, 1, lower) != 1) return false;

   double widthPoints = (upper[0] - lower[0]) / _Point;
   return widthPoints > InpBBWidthMaxPoints;
}

//====================================================================
// Helpers
//====================================================================
bool IsNewBar(const ENUM_TIMEFRAMES tf, datetime &lastBarTime)
{
   datetime t = iTime(_Symbol, tf, 0);
   if(t != lastBarTime)
   {
      lastBarTime = t;
      return true;
   }
   return false;
}

double GetSpreadPoints()
{
   return (SymbolInfoDouble(_Symbol, SYMBOL_ASK) - SymbolInfoDouble(_Symbol, SYMBOL_BID)) / _Point;
}

double SMA_TF(const ENUM_TIMEFRAMES tf, const int period, const int shift)
{
   int h = iMA(_Symbol, tf, period, 0, MODE_SMA, PRICE_CLOSE);
   if(h == INVALID_HANDLE) return 0;

   double b[];
   ArraySetAsSeries(b, true);

   double v = 0;
   if(CopyBuffer(h, 0, shift, 1, b) == 1)
      v = b[0];

   IndicatorRelease(h);
   return v;
}

double SMA_S1(const int period, const int shift)
{
   return SMA_TF(InpSignalTF_S1, period, shift);
}

double BB_Mid_FromHandle(const int handle, const int shift)
{
   if(handle == INVALID_HANDLE) return 0;
   double mid[];
   ArraySetAsSeries(mid, true);
   // iBands buffers: 0=upper, 1=middle, 2=lower
   if(CopyBuffer(handle, 1, shift, 1, mid) != 1) return 0;
   return mid[0];
}

double LowestLowTF(const ENUM_TIMEFRAMES tf, const int bars)
{
   double v = DBL_MAX;
   for(int i=1; i<=bars; i++)
      v = MathMin(v, iLow(_Symbol, tf, i));
   return v;
}

double HighestHighTF(const ENUM_TIMEFRAMES tf, const int bars)
{
   double v = -DBL_MAX;
   for(int i=1; i<=bars; i++)
      v = MathMax(v, iHigh(_Symbol, tf, i));
   return v;
}

bool HasOpenPositionByMagic(const ulong magic)
{
   for(int i=PositionsTotal()-1; i>=0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t==0) continue;

      if(PositionSelectByTicket(t) &&
         PositionGetString(POSITION_SYMBOL)==_Symbol &&
         (ulong)PositionGetInteger(POSITION_MAGIC)==magic)
         return true;
   }
   return false;
}

bool HasPendingByMagic(const ulong magic)
{
   for(int i=OrdersTotal()-1; i>=0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t==0) continue;

      if(OrderSelect(t) &&
         OrderGetString(ORDER_SYMBOL)==_Symbol &&
         (ulong)OrderGetInteger(ORDER_MAGIC)==magic)
         return true;
   }
   return false;
}

void DeletePendingByMagic(const ulong magic)
{
   for(int i=OrdersTotal()-1; i>=0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t==0) continue;

      if(OrderSelect(t) &&
         OrderGetString(ORDER_SYMBOL)==_Symbol &&
         (ulong)OrderGetInteger(ORDER_MAGIC)==magic)
         trade.OrderDelete(t);
   }
}

double CalcLotsEx(const double entry, const double sl, const double riskPercent, const int minSLPts, const int maxSLPts)
{
   double riskMoney = AccountInfoDouble(ACCOUNT_EQUITY) * riskPercent / 100.0;

   double tv = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
   double ts = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);

   double dist = MathAbs(entry - sl);
   double pts  = dist / _Point;

   if(pts < minSLPts || pts > maxSLPts) return 0;

   double lots = riskMoney / ((dist / ts) * tv);

   double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   double minL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);

   lots = MathMax(minL, MathMin(maxL, lots));
   return MathFloor(lots / step) * step;
}

//====================================================================
// Strategy 1: SMA Pullback Limit + optional Strategy 2 Reverse Stops
//====================================================================
void PlaceMainAndReverse_S1S2()
{
   if(IsRegimeTrending()) return;
   if(GetSpreadPoints() > InpMaxSpreadPoints_S1) return;

   // Single trade policy (main OR reverse)
   if(HasOpenPositionByMagic(InpMagic) || (InpEnableReverseStops_S2 && HasOpenPositionByMagic(InpReverseMagic_S2)))
      return;

   // Signal on closed bar 1
   double c1  = iClose(_Symbol, InpSignalTF_S1, 1);

   double s7  = SMA_S1(InpSMA_Fast_S1, 1);
   double s20 = SMA_S1(InpSMA_Mid_S1,  1);
   double s54 = SMA_S1(InpSMA_Slow_S1, 1);
   if(s7 == 0 || s20 == 0 || s54 == 0) return;

   bool buySignal  = (c1 > s7 && c1 > s20 && c1 > s54);
   bool sellSignal = (c1 < s7 && c1 < s20 && c1 < s54);

   // Pending handling
   if(InpRefreshPendingEachBar_S1)
   {
      DeletePendingByMagic(InpMagic);
      if(InpEnableReverseStops_S2) DeletePendingByMagic(InpReverseMagic_S2);
   }
   else
   {
      if(HasPendingByMagic(InpMagic)) return;
      if(InpEnableReverseStops_S2 && HasPendingByMagic(InpReverseMagic_S2)) return;
   }

   // BUY setup
   if(buySignal)
   {
      trade.SetExpertMagicNumber(InpMagic);

      double entry = s7 - InpEntryOffsetPoints_S1 * _Point;
      double sl    = LowestLowTF(InpSignalTF_S1, InpSwingLookbackBars_S1) - InpSL_BufferPoints_S1 * _Point;
      double tp    = entry + InpRR_S1 * (entry - sl);
      double lots  = CalcLotsEx(entry, sl, InpRiskPercent_S1, InpMinSLPoints_S1, InpMaxSLPoints_S1);

      if(lots > 0 && entry < SymbolInfoDouble(_Symbol, SYMBOL_BID))
         trade.BuyLimit(lots, entry, _Symbol, sl, tp, ORDER_TIME_GTC, 0, InpTradeComment_S1);

      if(InpEnableReverseStops_S2 && !IsRegimeTrending())
      {
         trade.SetExpertMagicNumber(InpReverseMagic_S2);

         double rEntry = entry;
         double rSL    = HighestHighTF(InpSignalTF_S1, InpReverseSwingLookback_S2) + InpReverseSL_BufferPoints_S2 * _Point;
         double rTP    = rEntry - InpReverseRR_S2 * (rSL - rEntry);
         double rLots  = CalcLotsEx(rEntry, rSL, InpReverseRiskPercent_S2, InpReverseMinSLPoints_S2, InpReverseMaxSLPoints_S2);

         if(rLots > 0 && rEntry < SymbolInfoDouble(_Symbol, SYMBOL_BID))
            trade.SellStop(rLots, rEntry, _Symbol, rSL, rTP, ORDER_TIME_GTC, 0, InpReverseComment_S2);
      }
   }

   // SELL setup
   if(sellSignal)
   {
      trade.SetExpertMagicNumber(InpMagic);

      double entry = s7 + InpEntryOffsetPoints_S1 * _Point;
      double sl    = HighestHighTF(InpSignalTF_S1, InpSwingLookbackBars_S1) + InpSL_BufferPoints_S1 * _Point;
      double tp    = entry - InpRR_S1 * (sl - entry);
      double lots  = CalcLotsEx(entry, sl, InpRiskPercent_S1, InpMinSLPoints_S1, InpMaxSLPoints_S1);

      if(lots > 0 && entry > SymbolInfoDouble(_Symbol, SYMBOL_ASK))
         trade.SellLimit(lots, entry, _Symbol, sl, tp, ORDER_TIME_GTC, 0, InpTradeComment_S1);

      if(InpEnableReverseStops_S2 && !IsRegimeTrending())
      {
         trade.SetExpertMagicNumber(InpReverseMagic_S2);

         double rEntry = entry;
         double rSL    = LowestLowTF(InpSignalTF_S1, InpReverseSwingLookback_S2) - InpReverseSL_BufferPoints_S2 * _Point;
         double rTP    = rEntry + InpReverseRR_S2 * (rEntry - rSL);
         double rLots  = CalcLotsEx(rEntry, rSL, InpReverseRiskPercent_S2, InpReverseMinSLPoints_S2, InpReverseMaxSLPoints_S2);

         if(rLots > 0 && rEntry > SymbolInfoDouble(_Symbol, SYMBOL_ASK))
            trade.BuyStop(rLots, rEntry, _Symbol, rSL, rTP, ORDER_TIME_GTC, 0, InpReverseComment_S2);
      }
   }
}

//====================================================================
// Strategy 3: Fractal Stops + SMA Filter
//====================================================================
double GetLastFractal_S3(const bool wantUp)
{
   if(g_fractalsHandle == INVALID_HANDLE) return 0;

   double buf[];
   ArraySetAsSeries(buf, true);

   int need = MathMax(InpFractalSearchBars_S3, 10);
   if(CopyBuffer(g_fractalsHandle, wantUp ? 0 : 1, 0, need, buf) != need)
      return 0;

   // Skip the latest 2 bars because fractals confirm with 2-bar delay
   for(int i=2; i<need; i++)
      if(buf[i] != 0 && buf[i] != EMPTY_VALUE)
         return buf[i];

   return 0;
}

void PlaceFractalStops_S3()
{
   if(!InpEnableFractalStops_S3) return;
   if(IsRegimeTrending()) return;

   if(GetSpreadPoints() > InpFractalMaxSpreadPts_S3) return;

   if(HasOpenPositionByMagic(InpFractalMagic_S3)) return;

   if(InpFractalRefreshPendingEachBar_S3)
      DeletePendingByMagic(InpFractalMagic_S3);
   else if(HasPendingByMagic(InpFractalMagic_S3))
      return;

   double c1 = iClose(_Symbol, InpFractalSignalTF_S3, 1);
   double ma = SMA_TF(InpFractalSignalTF_S3, InpFractalSMA_FilterPeriod_S3, 1);
   if(ma <= 0) return;

   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   int stops  = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);

   trade.SetExpertMagicNumber(InpFractalMagic_S3);

   // BUY STOP when close(1) above MA
   if(c1 > ma)
   {
      double f = GetLastFractal_S3(true);
      if(f > ask)
      {
         double entry = f + InpFractalEntryOffsetPoints_S3 * _Point;
         if(stops > 0 && (entry - ask) < stops * _Point)
            entry = ask + stops * _Point;

         double sl   = LowestLowTF(InpFractalSignalTF_S3, InpFractalSwingLookbackBars_S3) - InpFractalSL_BufferPoints_S3 * _Point;
         double tp   = entry + InpFractalRR_S3 * (entry - sl);
         double lots = CalcLotsEx(entry, sl, InpFractalRiskPercent_S3, InpFractalMinSLPoints_S3, InpFractalMaxSLPoints_S3);
         if(lots > 0)
            trade.BuyStop(lots, entry, _Symbol, sl, tp, ORDER_TIME_GTC, 0, InpFractalTradeComment_S3);
      }
   }

   // SELL STOP when close(1) below MA
   if(c1 < ma)
   {
      double f = GetLastFractal_S3(false);
      if(f < bid)
      {
         double entry = f - InpFractalEntryOffsetPoints_S3 * _Point;
         if(stops > 0 && (bid - entry) < stops * _Point)
            entry = bid - stops * _Point;

         double sl   = HighestHighTF(InpFractalSignalTF_S3, InpFractalSwingLookbackBars_S3) + InpFractalSL_BufferPoints_S3 * _Point;
         double tp   = entry - InpFractalRR_S3 * (sl - entry);
         double lots = CalcLotsEx(entry, sl, InpFractalRiskPercent_S3, InpFractalMinSLPoints_S3, InpFractalMaxSLPoints_S3);
         if(lots > 0)
            trade.SellStop(lots, entry, _Symbol, sl, tp, ORDER_TIME_GTC, 0, InpFractalTradeComment_S3);
      }
   }
}

//==================== ZZBB-Exact Helpers (S4/S5) ====================
bool HasOpenPositionByMagic_ZZBB(const ulong magic)
{
   for(int i=PositionsTotal()-1; i>=0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t==0) continue;

      if(PositionSelectByTicket(t) &&
         PositionGetString(POSITION_SYMBOL)==_Symbol &&
         (ulong)PositionGetInteger(POSITION_MAGIC)==magic)
         return true;
   }
   return false;
}

bool HasPendingByMagic_ZZBB(const ulong magic)
{
   for(int i=OrdersTotal()-1; i>=0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t==0) continue;

      if(OrderSelect(t) &&
         OrderGetString(ORDER_SYMBOL)==_Symbol &&
         (ulong)OrderGetInteger(ORDER_MAGIC)==magic)
         return true;
   }
   return false;
}

void DeletePendingByMagic_ZZBB(const ulong magic)
{
   for(int i=OrdersTotal()-1; i>=0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t==0) continue;

      if(OrderSelect(t) &&
         OrderGetString(ORDER_SYMBOL)==_Symbol &&
         (ulong)OrderGetInteger(ORDER_MAGIC)==magic)
         trade.OrderDelete(t);
   }
}

double CalcLotsEx_ZZBB(const double entry, const double sl, const double riskPercent,
                       const int minSLPts, const int maxSLPts)
{
   return CalcLotsEx(entry, sl, riskPercent, minSLPts, maxSLPts);
}

double BB_Mid_TF_ZZBB(const int shift)
{
   return BB_Mid_FromHandle(g_bbandsHandle_S4, shift);
}

//====================================================================
// Strategy 4/5: ZZBB-Exact BB MID Pullback (Main Limit + optional Inverse Stop)
//====================================================================
void PlaceBBMid_ZZBBExact()
{
   if(!InpEnableBBMid_S4 && !InpEnableBBMidInv_S5) return;

   if(GetSpreadPoints() > InpBBMidMaxSpreadPts_S4) return;

   // Single trade policy (main OR inverse)
   if( (InpEnableBBMid_S4 && HasOpenPositionByMagic_ZZBB(InpBBMidMagic_S4)) ||
       (InpEnableBBMidInv_S5 && HasOpenPositionByMagic_ZZBB(InpBBMidInvMagic_S5)) )
      return;

   double c1    = iClose(_Symbol, InpBBMidSignalTF_S4, 1);
   double bbMid = BB_Mid_TF_ZZBB(1);
   if(bbMid <= 0) return;

   bool wantBuy  = (c1 > bbMid);
   bool wantSell = (c1 < bbMid);

   // Pending handling (refresh deletes both legs)
   if(InpBBMidRefreshEachBar_S4)
   {
      if(InpEnableBBMid_S4)    DeletePendingByMagic_ZZBB(InpBBMidMagic_S4);
      if(InpEnableBBMidInv_S5) DeletePendingByMagic_ZZBB(InpBBMidInvMagic_S5);
   }
   else
   {
      if(InpEnableBBMid_S4 && HasPendingByMagic_ZZBB(InpBBMidMagic_S4)) return;
      if(InpEnableBBMidInv_S5 && HasPendingByMagic_ZZBB(InpBBMidInvMagic_S5)) return;
   }

   double bid   = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double ask   = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);

   if(wantBuy && InpEnableBBMid_S4)
   {
      trade.SetExpertMagicNumber(InpBBMidMagic_S4);

      double entry = bbMid - InpBBMidEntryOffsetPts_S4 * _Point;
      if(stopsLvl > 0 && (ask - entry) < stopsLvl * _Point)
         entry = ask - stopsLvl * _Point;

      double sl   = LowestLowTF(InpBBMidSignalTF_S4, InpBBMidSwingLookback_S4) - InpBBMidSL_BufferPts_S4 * _Point;
      double tp   = entry + InpBBMidRR_S4 * (entry - sl);
      double lots = CalcLotsEx_ZZBB(entry, sl, InpBBMidRiskPercent_S4, InpBBMidMinSLPoints_S4, InpBBMidMaxSLPoints_S4);

      if(lots > 0 && entry < ask)
         trade.BuyLimit(lots, entry, _Symbol, sl, tp, ORDER_TIME_GTC, 0, InpBBMidComment_S4);

      if(InpEnableBBMidInv_S5 && !IsRegimeTrending())
      {
         trade.SetExpertMagicNumber(InpBBMidInvMagic_S5);

         double rEntry = entry;
         if(stopsLvl > 0 && (bid - rEntry) < stopsLvl * _Point)
            rEntry = bid - stopsLvl * _Point;

         double rSL   = HighestHighTF(InpBBMidSignalTF_S4, InpBBMidInvSwingLookback_S5) + InpBBMidInvSL_BufferPts_S5 * _Point;
         double rTP   = rEntry - InpBBMidInvRR_S5 * (rSL - rEntry);
         double rLots = CalcLotsEx_ZZBB(rEntry, rSL, InpBBMidInvRiskPercent_S5, InpBBMidInvMinSLPoints_S5, InpBBMidInvMaxSLPoints_S5);

         if(rLots > 0 && rEntry < bid)
            trade.SellStop(rLots, rEntry, _Symbol, rSL, rTP, ORDER_TIME_GTC, 0, InpBBMidInvComment_S5);
      }
   }

   if(wantSell && InpEnableBBMid_S4)
   {
      trade.SetExpertMagicNumber(InpBBMidMagic_S4);

      double entry = bbMid + InpBBMidEntryOffsetPts_S4 * _Point;
      if(stopsLvl > 0 && (entry - bid) < stopsLvl * _Point)
         entry = bid + stopsLvl * _Point;

      double sl   = HighestHighTF(InpBBMidSignalTF_S4, InpBBMidSwingLookback_S4) + InpBBMidSL_BufferPts_S4 * _Point;
      double tp   = entry - InpBBMidRR_S4 * (sl - entry);
      double lots = CalcLotsEx_ZZBB(entry, sl, InpBBMidRiskPercent_S4, InpBBMidMinSLPoints_S4, InpBBMidMaxSLPoints_S4);

      if(lots > 0 && entry > bid)
         trade.SellLimit(lots, entry, _Symbol, sl, tp, ORDER_TIME_GTC, 0, InpBBMidComment_S4);

      if(InpEnableBBMidInv_S5 && !IsRegimeTrending())
      {
         trade.SetExpertMagicNumber(InpBBMidInvMagic_S5);

         double rEntry = entry;
         if(stopsLvl > 0 && (rEntry - ask) < stopsLvl * _Point)
            rEntry = ask + stopsLvl * _Point;

         double rSL   = LowestLowTF(InpBBMidSignalTF_S4, InpBBMidInvSwingLookback_S5) - InpBBMidInvSL_BufferPts_S5 * _Point;
         double rTP   = rEntry + InpBBMidInvRR_S5 * (rEntry - rSL);
         double rLots = CalcLotsEx_ZZBB(rEntry, rSL, InpBBMidInvRiskPercent_S5, InpBBMidInvMinSLPoints_S5, InpBBMidInvMaxSLPoints_S5);

         if(rLots > 0 && rEntry > ask)
            trade.BuyStop(rLots, rEntry, _Symbol, rSL, rTP, ORDER_TIME_GTC, 0, InpBBMidInvComment_S5);
      }
   }
}

//====================================================================
// MT5 Events
//====================================================================
int OnInit()
{
   g_lastSignalBarTime_S1  = iTime(_Symbol, InpSignalTF_S1, 0);
   g_lastFractalBarTime_S3 = iTime(_Symbol, InpFractalSignalTF_S3, 0);

   // Strategy 3: fractals handle
   if(InpEnableFractalStops_S3)
   {
      g_fractalsHandle = iFractals(_Symbol, InpFractalSignalTF_S3);
      if(g_fractalsHandle == INVALID_HANDLE) return INIT_FAILED;
   }

   // Strategy 4/5: Bollinger handle (shared)
   if(InpEnableBBMid_S4 || InpEnableBBMidInv_S5)
   {
      g_bbandsHandle_S4 = iBands(_Symbol, InpBBMidSignalTF_S4, InpBB_Period_S4, 0, InpBB_Deviation_S4, InpBB_Price_S4);
      if(g_bbandsHandle_S4 == INVALID_HANDLE) return INIT_FAILED;
      g_lastBBMidBarTime_S4 = iTime(_Symbol, InpBBMidSignalTF_S4, 0);
   }

   // Regime filter BB handle
   if(InpEnableBBWidthFilter)
   {
      g_bbwidthHandle = iBands(_Symbol, InpBBWidth_TF, InpBBWidth_Period, 0, InpBBWidth_Deviation, PRICE_CLOSE);
      if(g_bbwidthHandle == INVALID_HANDLE) return INIT_FAILED;
   }

   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   if(g_fractalsHandle  != INVALID_HANDLE) IndicatorRelease(g_fractalsHandle);
   if(g_bbandsHandle_S4 != INVALID_HANDLE) IndicatorRelease(g_bbandsHandle_S4);
   if(g_bbwidthHandle   != INVALID_HANDLE) IndicatorRelease(g_bbwidthHandle);
}

void OnTick()
{
   // Strategy 1/2 entries on new bar
   if(IsNewBar(InpSignalTF_S1, g_lastSignalBarTime_S1))
   {
      if(InpEnable_S1)
         PlaceMainAndReverse_S1S2();
   }

   // Strategy 3 entries on new bar
   if(InpEnableFractalStops_S3 && IsNewBar(InpFractalSignalTF_S3, g_lastFractalBarTime_S3))
      PlaceFractalStops_S3();

   // Strategy 4/5 entries on new bar (shared TF = InpBBMidSignalTF_S4)
   if((InpEnableBBMid_S4 || InpEnableBBMidInv_S5) && IsNewBar(InpBBMidSignalTF_S4, g_lastBBMidBarTime_S4))
      PlaceBBMid_ZZBBExact();
}

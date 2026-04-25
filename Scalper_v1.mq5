//+------------------------------------------------------------------+
//| Scalper_v1.mq5                                                    |
//| Donchian breakout scalper with daily profit target                |
//+------------------------------------------------------------------+
#property copyright "ZGB Trading"
#property version   "1.00"

#include <Trade/Trade.mqh>

CTrade g_trade;

//====================================================================
// Inputs
//====================================================================
input int     _Magic              = 2000;   // Magic number
input ENUM_TIMEFRAMES _EntryTF    = PERIOD_M5;

//==================== Stream 1 (US session) ========================
input bool    _S1_Enabled         = true;
input string  _S1_Comment         = "DT818_S1";
input double  _S1_RiskPct         = 2.5;
input int     _S1_DonchianBars    = 20;
input int     _S1_TakeProfit      = 150;
input int     _S1_StopLoss        = 50;
input double  _S1_HalfTP_Ratio    = 0.3;
input int     _S1_PendingExpireBars = 2;
input int     _S1_TradeStartHour  = 14;     // 14 UTC = London+NY overlap
input int     _S1_TradeEndHour    = 22;
input double  _S1_DailyTargetPct  = 5.0;    // Stop at +X% daily (0=off)
input double  _S1_DailyLossPct    = 10.0;   // Stop at -X% daily (0=off)

//====================================================================
// Globals
//====================================================================
datetime g_lastBar_M1   = 0;
datetime g_sessionStart = 0;
double   g_balanceAtSessionStart = 0;  // balance at start of day (for % calcs)

//====================================================================
// Helpers
//====================================================================
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

double CalcLots(double slPoints, double riskPct)
{
   double riskMoney = AccountInfoDouble(ACCOUNT_BALANCE) * riskPct / 100.0;
   double tickVal   = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickSize  = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   if(tickVal <= 0 || tickSize <= 0 || slPoints <= 0) return 0;
   double slMoney = (slPoints * _Point / tickSize) * tickVal;
   if(slMoney <= 0) return 0;
   double lots = riskMoney / slMoney;

   double minL  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxL  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);
   double step  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   if(step <= 0) step = 0.01;

   lots = MathMax(minL, MathMin(maxL, lots));
   lots = MathRound(lots / step) * step;
   return lots;
}

bool HasPending(const string comment)
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0) continue;
      if(!OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != _Magic) continue;
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
      if((int)OrderGetInteger(ORDER_MAGIC) != _Magic) continue;
      if(OrderGetString(ORDER_COMMENT) != comment) continue;
      g_trade.OrderDelete(t);
   }
}

double HighestHigh(ENUM_TIMEFRAMES tf, int bars)
{
   double h = -DBL_MAX;
   for(int i = 1; i <= bars; i++) h = MathMax(h, iHigh(_Symbol, tf, i));
   return h;
}

double LowestLow(ENUM_TIMEFRAMES tf, int bars)
{
   double l = DBL_MAX;
   for(int i = 1; i <= bars; i++) l = MathMin(l, iLow(_Symbol, tf, i));
   return l;
}

//====================================================================
// Daily P&L tracking
//====================================================================
datetime DayStart(datetime t)
{
   MqlDateTime dt;
   TimeToStruct(t, dt);
   dt.hour = 0; dt.min = 0; dt.sec = 0;
   return StructToTime(dt);
}

void UpdateSession()
{
   datetime today = DayStart(TimeCurrent());
   if(today != g_sessionStart)
   {
      g_sessionStart = today;
      g_balanceAtSessionStart = AccountInfoDouble(ACCOUNT_BALANCE);
   }
}

// Realized P&L since day start (single-stream EA — magic+symbol is unique).
double RealizedToday()
{
   double total = 0;
   HistorySelect(g_sessionStart, TimeCurrent());
   int deals = HistoryDealsTotal();
   for(int i = 0; i < deals; i++)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket == 0) continue;
      if(HistoryDealGetString(ticket, DEAL_SYMBOL) != _Symbol) continue;
      if((int)HistoryDealGetInteger(ticket, DEAL_MAGIC) != _Magic) continue;
      total += HistoryDealGetDouble(ticket, DEAL_PROFIT)
             + HistoryDealGetDouble(ticket, DEAL_SWAP)
             + HistoryDealGetDouble(ticket, DEAL_COMMISSION);
   }
   return total;
}

// Unrealized P&L on open positions (filtered by comment for future multi-stream).
double UnrealizedProfit(const string comment)
{
   double total = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0) continue;
      if(!PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _Magic) continue;
      if(PositionGetString(POSITION_COMMENT) != comment) continue;
      total += PositionGetDouble(POSITION_PROFIT)
             + PositionGetDouble(POSITION_SWAP);
   }
   return total;
}

double TodayProfit(const string comment)
{
   return RealizedToday() + UnrealizedProfit(comment);
}

bool DailyTargetHit(const string comment, double targetPct)
{
   if(targetPct <= 0) return false;
   double target = g_balanceAtSessionStart * targetPct / 100.0;
   return TodayProfit(comment) >= target;
}

bool DailyLossHit(const string comment, double lossPct)
{
   if(lossPct <= 0) return false;
   double loss = g_balanceAtSessionStart * lossPct / 100.0;
   return TodayProfit(comment) <= -loss;
}

//====================================================================
// Time filter
//====================================================================
bool TradingHoursOK(int startHour, int endHour)
{
   MqlDateTime dt;
   TimeToStruct(TimeCurrent(), dt);
   // Block weekend
   if(dt.day_of_week == 0 || dt.day_of_week == 6) return false;
   // Within trading hours
   if(dt.hour < startHour || dt.hour >= endHour) return false;
   return true;
}

//====================================================================
// Close all positions + pending for a stream
//====================================================================
void CloseAllAndPending(const string comment)
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0) continue;
      if(!PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _Magic) continue;
      if(PositionGetString(POSITION_COMMENT) != comment) continue;
      g_trade.PositionClose(t);
   }
   DeletePending(comment);
}

//====================================================================
// Stream processor — one stream's logic
//====================================================================
void ProcessStream(const string comment, double riskPct,
                   int donchBars, int tpPts, int slPts, double htpRatio,
                   int penBars, int startHour, int endHour,
                   double dailyTargetPct, double dailyLossPct)
{
   // Daily limits (per stream)
   if(DailyTargetHit(comment, dailyTargetPct) ||
      DailyLossHit(comment, dailyLossPct))
   {
      CloseAllAndPending(comment);
      return;
   }

   if(!TradingHoursOK(startHour, endHour))
   {
      DeletePending(comment);
      return;
   }

   // Skip if pending exists for this stream
   if(HasPending(comment)) return;

   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);

   datetime expiry = iTime(_Symbol, _EntryTF, 0) + penBars * PeriodSeconds(_EntryTF);

   double totalLots = CalcLots((double)slPts, riskPct);
   if(totalLots <= 0) return;

   double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   double minLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   if(lotStep <= 0) lotStep = 0.01;
   double halfLots = MathRound(totalLots / 2.0 / lotStep) * lotStep;
   if(halfLots < minLot) halfLots = minLot;

   // Buy stop at Donchian high
   double highBrk = NormPrice(HighestHigh(_EntryTF, donchBars));
   double minBuy  = NormPrice(ask + stopsLvl * _Point);
   if(highBrk < minBuy) highBrk = minBuy;
   if(highBrk > ask)
   {
      double sl = NormPrice(highBrk - slPts * _Point);
      double tp = NormPrice(highBrk + tpPts * _Point);
      if(htpRatio > 0)
      {
         double tpHalf = NormPrice(highBrk + tpPts * htpRatio * _Point);
         g_trade.BuyStop(halfLots, highBrk, _Symbol, sl, tpHalf,
                         ORDER_TIME_SPECIFIED, expiry, comment);
         g_trade.BuyStop(halfLots, highBrk, _Symbol, sl, tp,
                         ORDER_TIME_SPECIFIED, expiry, comment);
      }
      else
      {
         g_trade.BuyStop(totalLots, highBrk, _Symbol, sl, tp,
                         ORDER_TIME_SPECIFIED, expiry, comment);
      }
   }

   // Sell stop at Donchian low
   double lowBrk  = NormPrice(LowestLow(_EntryTF, donchBars));
   double maxSell = NormPrice(bid - stopsLvl * _Point);
   if(lowBrk > maxSell) lowBrk = maxSell;
   if(lowBrk < bid)
   {
      double sl = NormPrice(lowBrk + slPts * _Point);
      double tp = NormPrice(lowBrk - tpPts * _Point);
      if(htpRatio > 0)
      {
         double tpHalf = NormPrice(lowBrk - tpPts * htpRatio * _Point);
         g_trade.SellStop(halfLots, lowBrk, _Symbol, sl, tpHalf,
                          ORDER_TIME_SPECIFIED, expiry, comment);
         g_trade.SellStop(halfLots, lowBrk, _Symbol, sl, tp,
                          ORDER_TIME_SPECIFIED, expiry, comment);
      }
      else
      {
         g_trade.SellStop(totalLots, lowBrk, _Symbol, sl, tp,
                          ORDER_TIME_SPECIFIED, expiry, comment);
      }
   }
}

//====================================================================
// Events
//====================================================================
int OnInit()
{
   g_trade.SetExpertMagicNumber(_Magic);
   g_trade.SetDeviationInPoints(30);

   g_lastBar_M1 = iTime(_Symbol, PERIOD_M1, 0);
   g_sessionStart = DayStart(TimeCurrent());
   g_balanceAtSessionStart = AccountInfoDouble(ACCOUNT_BALANCE);

   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason) {}

void OnTick()
{
   // Update session first (handles daily reset)
   UpdateSession();

   // Daily limit check on every tick (close immediately if hit)
   if(_S1_Enabled && (DailyTargetHit(_S1_Comment, _S1_DailyTargetPct) ||
                     DailyLossHit(_S1_Comment, _S1_DailyLossPct)))
      CloseAllAndPending(_S1_Comment);

   // Process on every M1 bar
   if(IsNewBar(PERIOD_M1, g_lastBar_M1))
   {
      if(_S1_Enabled)
         ProcessStream(_S1_Comment, _S1_RiskPct,
                       _S1_DonchianBars, _S1_TakeProfit, _S1_StopLoss, _S1_HalfTP_Ratio,
                       _S1_PendingExpireBars, _S1_TradeStartHour, _S1_TradeEndHour,
                       _S1_DailyTargetPct, _S1_DailyLossPct);
   }
}
//+------------------------------------------------------------------+

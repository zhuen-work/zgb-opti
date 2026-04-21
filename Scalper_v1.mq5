//+------------------------------------------------------------------+
//| Scalper_v1.mq5                                                    |
//| Donchian breakout scalper with daily profit target                |
//+------------------------------------------------------------------+
#property copyright "ZGB Trading"
#property version   "1.00"
#property strict

#include <Trade/Trade.mqh>

CTrade g_trade;

//====================================================================
// Inputs
//====================================================================
input int     _Magic              = 2000;   // Magic number
input double  _RiskPct            = 0.5;    // Risk % per trade
input int     _MaxPositions       = 2;      // Max concurrent positions

// Entry
input ENUM_TIMEFRAMES _EntryTF    = PERIOD_M5;
input int     _DonchianBars       = 10;     // Donchian lookback bars
input int     _TakeProfit         = 100;    // TP in points
input int     _StopLoss           = 100;    // SL in points
input double  _HalfTP_Ratio       = 0.0;    // Split TP ratio (0=off, e.g., 0.5 = half at 50% TP)
input int     _PendingExpireBars  = 1;      // Pending expiry (bars)


// Daily limits
input double  _DailyTargetPct     = 0.0;    // Stop trading at +X% daily (0=off)
input double  _DailyLossPct       = 0.0;    // Stop trading at -X% daily (0=off)
input int     _DailyMaxWins       = 3;      // Stop trading after N winning trades (0=off)
input int     _DailyMaxLosses     = 2;      // Stop trading after N losing trades (0=off)

// Time filter (broker server time)
input int     _TradeStartHour     = 13;     // Start hour (13 UTC = London+NY overlap start)
input int     _TradeEndHour       = 17;     // End hour
input bool    _BlockFriPM         = true;   // Block Friday after 17:00

input string  _OrderCommentTag    = "Scalp";

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

double CalcLots(double slPoints)
{
   double riskMoney = AccountInfoDouble(ACCOUNT_BALANCE) * _RiskPct / 100.0;
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

int CountPositions()
{
   int count = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0) continue;
      if(!PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _Magic) continue;
      count++;
   }
   return count;
}

bool HasPending()
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0) continue;
      if(!OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != _Magic) continue;
      return true;
   }
   return false;
}

void DeletePending()
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0) continue;
      if(!OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != _Magic) continue;
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

// Realized P&L today from this EA's magic only (closed deals since session start)
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

// Unrealized P&L on this EA's open positions only
double UnrealizedProfit()
{
   double total = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0) continue;
      if(!PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _Magic) continue;
      total += PositionGetDouble(POSITION_PROFIT)
             + PositionGetDouble(POSITION_SWAP);
   }
   return total;
}

// Today's total P&L = realized + unrealized (equity-based, scoped to this EA only)
double TodayProfit()
{
   return RealizedToday() + UnrealizedProfit();
}

// Count today's closed trades by outcome (scoped to this EA only)
// Returns pair: (wins, losses)
void CountTodayOutcomes(int &wins, int &losses)
{
   wins = 0;
   losses = 0;
   HistorySelect(g_sessionStart, TimeCurrent());
   int deals = HistoryDealsTotal();
   for(int i = 0; i < deals; i++)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket == 0) continue;
      if(HistoryDealGetString(ticket, DEAL_SYMBOL) != _Symbol) continue;
      if((int)HistoryDealGetInteger(ticket, DEAL_MAGIC) != _Magic) continue;
      // Only count exit deals (entry=IN, exit=OUT)
      long entry = HistoryDealGetInteger(ticket, DEAL_ENTRY);
      if(entry != DEAL_ENTRY_OUT) continue;
      double profit = HistoryDealGetDouble(ticket, DEAL_PROFIT)
                    + HistoryDealGetDouble(ticket, DEAL_SWAP)
                    + HistoryDealGetDouble(ticket, DEAL_COMMISSION);
      if(profit > 0) wins++;
      else if(profit < 0) losses++;
   }
}

bool DailyTargetHit()
{
   // % target
   if(_DailyTargetPct > 0)
   {
      double target = g_balanceAtSessionStart * _DailyTargetPct / 100.0;
      if(TodayProfit() >= target) return true;
   }
   // Win count target
   if(_DailyMaxWins > 0)
   {
      int w, l; CountTodayOutcomes(w, l);
      if(w >= _DailyMaxWins) return true;
   }
   return false;
}

bool DailyLossHit()
{
   // % loss
   if(_DailyLossPct > 0)
   {
      double loss = g_balanceAtSessionStart * _DailyLossPct / 100.0;
      if(TodayProfit() <= -loss) return true;
   }
   // Loss count cap
   if(_DailyMaxLosses > 0)
   {
      int w, l; CountTodayOutcomes(w, l);
      if(l >= _DailyMaxLosses) return true;
   }
   return false;
}

//====================================================================
// Time filter
//====================================================================
bool TradingHoursOK()
{
   MqlDateTime dt;
   TimeToStruct(TimeCurrent(), dt);

   // Block Friday PM if enabled
   if(_BlockFriPM && dt.day_of_week == 5 && dt.hour >= _TradeEndHour) return false;

   // Block weekend
   if(dt.day_of_week == 0 || dt.day_of_week == 6) return false;

   // Within trading hours
   if(dt.hour < _TradeStartHour || dt.hour >= _TradeEndHour) return false;

   return true;
}

//====================================================================
// Close all positions + pending
//====================================================================
void CloseAllAndPending()
{
   g_trade.SetExpertMagicNumber(_Magic);
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0) continue;
      if(!PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != _Magic) continue;
      g_trade.PositionClose(t);
   }
   DeletePending();
}

//====================================================================
// Main scalper logic
//====================================================================
void ProcessScalp()
{
   // Session + daily limits
   UpdateSession();

   if(DailyTargetHit() || DailyLossHit())
   {
      CloseAllAndPending();
      return;
   }

   if(!TradingHoursOK())
   {
      DeletePending();
      return;
   }

   // Max positions check
   if(CountPositions() >= _MaxPositions) return;
   if(HasPending()) return;


   g_trade.SetExpertMagicNumber(_Magic);

   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);

   datetime expiry = iTime(_Symbol, _EntryTF, 0) + _PendingExpireBars * PeriodSeconds(_EntryTF);

   double totalLots = CalcLots((double)_StopLoss);
   if(totalLots <= 0) return;

   double lotStep = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   double minLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   if(lotStep <= 0) lotStep = 0.01;
   double halfLots = MathRound(totalLots / 2.0 / lotStep) * lotStep;
   if(halfLots < minLot) halfLots = minLot;

   // Buy stop at Donchian high
   double highBrk = NormPrice(HighestHigh(_EntryTF, _DonchianBars));
   double minBuy  = NormPrice(ask + stopsLvl * _Point);
   if(highBrk < minBuy) highBrk = minBuy;
   if(highBrk > ask)
   {
      double sl = NormPrice(highBrk - _StopLoss * _Point);
      double tp = NormPrice(highBrk + _TakeProfit * _Point);
      if(_HalfTP_Ratio > 0)
      {
         double tpHalf = NormPrice(highBrk + _TakeProfit * _HalfTP_Ratio * _Point);
         g_trade.BuyStop(halfLots, highBrk, _Symbol, sl, tpHalf,
                         ORDER_TIME_SPECIFIED, expiry, _OrderCommentTag);
         g_trade.BuyStop(halfLots, highBrk, _Symbol, sl, tp,
                         ORDER_TIME_SPECIFIED, expiry, _OrderCommentTag);
      }
      else
      {
         g_trade.BuyStop(totalLots, highBrk, _Symbol, sl, tp,
                         ORDER_TIME_SPECIFIED, expiry, _OrderCommentTag);
      }
   }

   // Sell stop at Donchian low
   double lowBrk  = NormPrice(LowestLow(_EntryTF, _DonchianBars));
   double maxSell = NormPrice(bid - stopsLvl * _Point);
   if(lowBrk > maxSell) lowBrk = maxSell;
   if(lowBrk < bid)
   {
      double sl = NormPrice(lowBrk + _StopLoss * _Point);
      double tp = NormPrice(lowBrk - _TakeProfit * _Point);
      if(_HalfTP_Ratio > 0)
      {
         double tpHalf = NormPrice(lowBrk - _TakeProfit * _HalfTP_Ratio * _Point);
         g_trade.SellStop(halfLots, lowBrk, _Symbol, sl, tpHalf,
                          ORDER_TIME_SPECIFIED, expiry, _OrderCommentTag);
         g_trade.SellStop(halfLots, lowBrk, _Symbol, sl, tp,
                          ORDER_TIME_SPECIFIED, expiry, _OrderCommentTag);
      }
      else
      {
         g_trade.SellStop(totalLots, lowBrk, _Symbol, sl, tp,
                          ORDER_TIME_SPECIFIED, expiry, _OrderCommentTag);
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

   // Daily target/loss check on every tick
   if(DailyTargetHit() || DailyLossHit())
   {
      CloseAllAndPending();
      return;
   }

   // Process on every M1 bar
   if(IsNewBar(PERIOD_M1, g_lastBar_M1))
      ProcessScalp();
}
//+------------------------------------------------------------------+

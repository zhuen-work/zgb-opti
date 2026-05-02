//+------------------------------------------------------------------+
//| Scalper_v2.mq5                                                    |
//| 2-stream scalping EA on M5:                                       |
//|   S1: Opening Range Breakout (ORB) — London + NY sessions         |
//|   S2: Liquidity Sweep Reversal (SMC-style stop-hunt fade)         |
//|                                                                    |
//| Streams enable independently. Each has its own magic offset and   |
//| comment so positions don't conflict in MT5 history.                |
//+------------------------------------------------------------------+
#property copyright "ZGB Trading"
#property version   "2.00"
#property strict

#include <Trade/Trade.mqh>

CTrade g_trade;

//====================================================================
// Global Inputs
//====================================================================
input int     _BaseMagic       = 3000;       // Base magic; S1=+0, S2=+1
input double  _RiskPct         = 1.0;
input int     _LotMode         = 1;          // 0=fixed lot, 1=risk%
input double  LotStep          = 0.01;

//====================================================================
// Stream 1: Opening Range Breakout
//====================================================================
input bool             _S1_Enabled              = true;
input string           _S1_Comment              = "ORB";
input ENUM_TIMEFRAMES  _S1_EntryTF              = PERIOD_M5;

// Sessions to trade (UTC)
input bool             _S1_LDN_Enabled          = true;
input int              _S1_LDN_StartHour        = 7;     // London 07:00 UTC
input bool             _S1_NY_Enabled           = true;
input int              _S1_NY_StartHour         = 13;    // NY 13:00 UTC

// Range definition + entry
input int              _S1_RangeMinutes         = 30;    // First N minutes define range
input int              _S1_BufferPts            = 30;    // BuyStop = high+buffer; SellStop = low-buffer
input int              _S1_MinRangePts          = 200;   // Skip session if range < this
input int              _S1_MaxRangePts          = 5000;  // Skip session if range > this

// Exits
input int              _S1_FixedSL_Pts          = 0;     // 0 = SL = range_size; else fixed pts
input double           _S1_RR_Ratio             = 2.0;   // TP = RR × SL distance
input double           _S1_HalfTP_Ratio         = 0.0;   // Split TP at HTP×TP, 0=off

// Order management
input int              _S1_PendingExpireMinutes = 120;   // Cancel pending after N min from range close
// (removed _S1_OneTradePerSession Apr 28 — use account-level _DailyTargetPct/_DailyLossPct instead)

//====================================================================
// Stream 2: Liquidity Sweep Reversal
//====================================================================
input bool             _S2_Enabled              = true;
input string           _S2_Comment              = "SWEEP";
input ENUM_TIMEFRAMES  _S2_EntryTF              = PERIOD_M5;

// Swing + sweep detection
input int              _S2_SwingLookback        = 30;    // Bars to find swing high/low
input int              _S2_SweepMinPts          = 50;    // Spike must exceed swing by >= this
input int              _S2_ConfirmBars          = 1;     // Bars after sweep that close back inside

// Exits
input int              _S2_SL_BufferPts         = 30;    // SL = sweep extreme + buffer
input double           _S2_RR_Ratio             = 2.0;   // TP = RR × SL distance
input double           _S2_HalfTP_Ratio         = 0.0;

// Order management
input int              _S2_PendingExpireBars    = 5;     // M5 bars
input bool             _S2_OneTradePerSweep     = true;  // Only 1 trade per detected sweep

// Trading hours (both streams)
input int              _TradeStartHour          = 0;     // 0 = no filter
input int              _TradeEndHour            = 24;
input bool             _BlockWeekendCarryover   = true;  // Close all on Friday late

//====================================================================
// Daily target / loss caps (account-level — applies to BOTH streams)
//====================================================================
input double           _DailyTargetPct          = 6.0;   // Stop trading once today's realized PnL >= X% of day-start balance
input double           _DailyLossPct            = 8.0;   // Stop trading once today's realized PnL <= -X% of day-start balance

//====================================================================
// Globals
//====================================================================
datetime g_lastBar_M1 = 0;

// Daily PnL tracking (account-level)
datetime g_session_day = 0;
double   g_balance_day_start = 0.0;
bool     g_daily_lock = false;       // true once daily target/loss hit; reset on next day

// S1 (ORB) per-session state
struct ORBSession
{
   bool       active;            // In range-formation window?
   bool       range_complete;    // Range formed?
   bool       fired;             // Has placed orders?
   bool       traded;            // Has any trade filled?
   datetime   range_start;
   datetime   range_end;
   datetime   pending_expire;
   double     range_high;
   double     range_low;
};
ORBSession g_ldn = {0};
ORBSession g_ny  = {0};

// S2 (Sweep) state
struct SweepState
{
   bool       waiting_confirm;
   int        sweep_dir;         // +1 bullish sweep (high taken), -1 bearish (low taken)
   double     sweep_extreme;     // High of bullish sweep or low of bearish
   double     swing_level;       // The level that was swept
   datetime   sweep_time;
   int        confirm_bars_seen;
};
SweepState g_sweep = {false, 0, 0, 0, 0, 0};

//====================================================================
// Helpers
//====================================================================
bool IsNewBar(ENUM_TIMEFRAMES tf, datetime &lastTime)
{
   datetime t = iTime(_Symbol, tf, 0);
   if(t != lastTime) { lastTime = t; return true; }
   return false;
}

double NormPrice(double p)
{
   double ts = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   if(ts <= 0) return NormalizeDouble(p, _Digits);
   return NormalizeDouble(MathRound(p / ts) * ts, _Digits);
}

double CalcLots(double slPoints)
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

bool InTradingHours(datetime t)
{
   MqlDateTime mt;
   TimeToStruct(t, mt);
   if(mt.day_of_week == 0 || mt.day_of_week == 6) return false;  // weekend
   if(_TradeStartHour == 0 && _TradeEndHour >= 24) return true;
   return mt.hour >= _TradeStartHour && mt.hour < _TradeEndHour;
}

int CountPositionsByMagic(int magic, const string comment)
{
   int n = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0) continue;
      if(!PositionSelectByTicket(t)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      if((int)PositionGetInteger(POSITION_MAGIC) != magic) continue;
      if(comment != "" && PositionGetString(POSITION_COMMENT) != comment) continue;
      n++;
   }
   return n;
}

bool HasPendingByMagic(int magic, const string comment)
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0) continue;
      if(!OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != magic) continue;
      if(comment != "" && OrderGetString(ORDER_COMMENT) != comment) continue;
      return true;
   }
   return false;
}

void DeletePendingByMagic(int magic, const string comment)
{
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong t = OrderGetTicket(i);
      if(t == 0) continue;
      if(!OrderSelect(t)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      if((int)OrderGetInteger(ORDER_MAGIC) != magic) continue;
      if(comment != "" && OrderGetString(ORDER_COMMENT) != comment) continue;
      g_trade.OrderDelete(t);
   }
}

//====================================================================
// Stream 1: ORB
//====================================================================
void InitSession(ORBSession &s, int startHour, datetime now)
{
   MqlDateTime mt;
   TimeToStruct(now, mt);
   mt.hour = startHour; mt.min = 0; mt.sec = 0;
   s.range_start = StructToTime(mt);
   s.range_end   = s.range_start + _S1_RangeMinutes * 60;
   s.pending_expire = s.range_end + _S1_PendingExpireMinutes * 60;
   s.active = true;
   s.range_complete = false;
   s.fired = false;
   s.traded = false;
   s.range_high = 0;
   s.range_low  = 0;
}

void UpdateSession(ORBSession &s, int magic, datetime now)
{
   if(!s.active) return;

   // New trading day? Reset (we'll re-init on next session start)
   MqlDateTime mt_now, mt_range;
   TimeToStruct(now, mt_now);
   TimeToStruct(s.range_start, mt_range);
   if(mt_now.day != mt_range.day || mt_now.mon != mt_range.mon)
   {
      s.active = false;
      return;
   }

   // In range-formation window?
   if(now < s.range_end)
   {
      // Track running range from M1 highs/lows since range_start
      double rh = iHigh(_Symbol, _S1_EntryTF, 0);
      double rl = iLow(_Symbol, _S1_EntryTF, 0);
      // More accurate: scan all M5 bars from range_start to now
      for(int i = 0; i < 50; i++)
      {
         datetime bt = iTime(_Symbol, _S1_EntryTF, i);
         if(bt < s.range_start) break;
         if(bt >= s.range_end) continue;
         double h = iHigh(_Symbol, _S1_EntryTF, i);
         double l = iLow(_Symbol, _S1_EntryTF, i);
         if(s.range_high == 0 || h > s.range_high) s.range_high = h;
         if(s.range_low == 0 || l < s.range_low) s.range_low = l;
      }
      return;
   }

   // Range complete — fire orders once
   if(!s.fired && s.range_high > 0 && s.range_low > 0)
   {
      double range_pts = (s.range_high - s.range_low) / _Point;
      if(range_pts < _S1_MinRangePts || range_pts > _S1_MaxRangePts)
      {
         s.fired = true;   // skip this session
         return;
      }

      double sl_dist_pts = (_S1_FixedSL_Pts > 0) ? _S1_FixedSL_Pts : range_pts;
      double tp_dist_pts = sl_dist_pts * _S1_RR_Ratio;

      double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
      double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      int stopsLvl = (int)SymbolInfoInteger(_Symbol, SYMBOL_TRADE_STOPS_LEVEL);

      double totalLots = CalcLots(sl_dist_pts);
      if(totalLots <= 0) { s.fired = true; return; }

      double halfLots = totalLots;
      if(_S1_HalfTP_Ratio > 0)
      {
         double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
         double minL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
         if(step <= 0) step = 0.01;
         halfLots = MathRound(totalLots / 2.0 / step) * step;
         if(halfLots < minL) halfLots = minL;
      }

      g_trade.SetExpertMagicNumber(magic);

      // BuyStop above range_high
      double buyEntry = NormPrice(s.range_high + _S1_BufferPts * _Point);
      double minBuy = NormPrice(ask + stopsLvl * _Point);
      if(buyEntry < minBuy) buyEntry = minBuy;
      if(buyEntry > ask)
      {
         double sl = NormPrice(buyEntry - sl_dist_pts * _Point);
         double tp = NormPrice(buyEntry + tp_dist_pts * _Point);
         if(_S1_HalfTP_Ratio > 0)
         {
            double tpHalf = NormPrice(buyEntry + tp_dist_pts * _S1_HalfTP_Ratio * _Point);
            g_trade.BuyStop(halfLots, buyEntry, _Symbol, sl, tpHalf,
                            ORDER_TIME_SPECIFIED, s.pending_expire, _S1_Comment);
            g_trade.BuyStop(halfLots, buyEntry, _Symbol, sl, tp,
                            ORDER_TIME_SPECIFIED, s.pending_expire, _S1_Comment);
         }
         else
         {
            g_trade.BuyStop(totalLots, buyEntry, _Symbol, sl, tp,
                            ORDER_TIME_SPECIFIED, s.pending_expire, _S1_Comment);
         }
      }

      // SellStop below range_low
      double sellEntry = NormPrice(s.range_low - _S1_BufferPts * _Point);
      double maxSell = NormPrice(bid - stopsLvl * _Point);
      if(sellEntry > maxSell) sellEntry = maxSell;
      if(sellEntry < bid)
      {
         double sl = NormPrice(sellEntry + sl_dist_pts * _Point);
         double tp = NormPrice(sellEntry - tp_dist_pts * _Point);
         if(_S1_HalfTP_Ratio > 0)
         {
            double tpHalf = NormPrice(sellEntry - tp_dist_pts * _S1_HalfTP_Ratio * _Point);
            g_trade.SellStop(halfLots, sellEntry, _Symbol, sl, tpHalf,
                             ORDER_TIME_SPECIFIED, s.pending_expire, _S1_Comment);
            g_trade.SellStop(halfLots, sellEntry, _Symbol, sl, tp,
                             ORDER_TIME_SPECIFIED, s.pending_expire, _S1_Comment);
         }
         else
         {
            g_trade.SellStop(totalLots, sellEntry, _Symbol, sl, tp,
                             ORDER_TIME_SPECIFIED, s.pending_expire, _S1_Comment);
         }
      }

      s.fired = true;
      s.range_complete = true;
   }

   // (OCO removed — both BuyStop and SellStop can fire if session whipsaws.
   // Account-level _DailyTargetPct/_DailyLossPct caps manage exposure.)

   // Past pending expire: cleanup
   if(now >= s.pending_expire)
   {
      DeletePendingByMagic(magic, _S1_Comment);
      s.active = false;
   }
}

void ProcessORB()
{
   datetime now = TimeCurrent();
   MqlDateTime mt;
   TimeToStruct(now, mt);

   // London session
   if(_S1_LDN_Enabled)
   {
      if(!g_ldn.active && mt.hour == _S1_LDN_StartHour && mt.min < 5)
         InitSession(g_ldn, _S1_LDN_StartHour, now);
      UpdateSession(g_ldn, _BaseMagic + 0, now);
   }
   // NY session
   if(_S1_NY_Enabled)
   {
      if(!g_ny.active && mt.hour == _S1_NY_StartHour && mt.min < 5)
         InitSession(g_ny, _S1_NY_StartHour, now);
      UpdateSession(g_ny, _BaseMagic + 0, now);
   }
}

//====================================================================
// Stream 2: Liquidity Sweep Reversal
//====================================================================
// NOTE: scan bars PRIOR to the sweep-candidate bar (i=1). Old version i=1..lookback
// included bar 1 itself, making sweep_above = last_high - swing_hi always <= 0
// (bug — branch never fired). Now i=2..lookback+1.
double GetSwingHigh(int lookback)
{
   double hi = 0;
   for(int i = 2; i <= lookback + 1; i++)
   {
      double h = iHigh(_Symbol, _S2_EntryTF, i);
      if(h > hi) hi = h;
   }
   return hi;
}

double GetSwingLow(int lookback)
{
   double lo = DBL_MAX;
   for(int i = 2; i <= lookback + 1; i++)
   {
      double l = iLow(_Symbol, _S2_EntryTF, i);
      if(l < lo) lo = l;
   }
   return (lo == DBL_MAX) ? 0 : lo;
}

void ProcessSweep()
{
   int magic = _BaseMagic + 1;
   datetime now = TimeCurrent();
   if(!InTradingHours(now)) return;

   // If position already open or pending placed, skip
   if(CountPositionsByMagic(magic, _S2_Comment) > 0) return;
   if(HasPendingByMagic(magic, _S2_Comment)) return;

   // State machine: detect sweep, wait for confirmation, fire entry
   double swing_hi = GetSwingHigh(_S2_SwingLookback);
   double swing_lo = GetSwingLow(_S2_SwingLookback);
   if(swing_hi <= 0 || swing_lo <= 0) return;

   // Last completed bar
   double last_high = iHigh(_Symbol, _S2_EntryTF, 1);
   double last_low  = iLow(_Symbol, _S2_EntryTF, 1);
   double last_close = iClose(_Symbol, _S2_EntryTF, 1);

   if(!g_sweep.waiting_confirm)
   {
      // Detect a fresh sweep: bar 1 spiked past swing then closed back inside
      double sweep_above = last_high - swing_hi;
      double sweep_below = swing_lo - last_low;

      if(sweep_above >= _S2_SweepMinPts * _Point && last_close < swing_hi)
      {
         // Bullish sweep (high taken, closed back inside) → expect down move
         g_sweep.waiting_confirm = true;
         g_sweep.sweep_dir = -1;   // sell direction after sweep
         g_sweep.sweep_extreme = last_high;
         g_sweep.swing_level = swing_hi;
         g_sweep.sweep_time = iTime(_Symbol, _S2_EntryTF, 1);
         g_sweep.confirm_bars_seen = 0;
      }
      else if(sweep_below >= _S2_SweepMinPts * _Point && last_close > swing_lo)
      {
         // Bearish sweep (low taken, closed back inside) → expect up move
         g_sweep.waiting_confirm = true;
         g_sweep.sweep_dir = 1;
         g_sweep.sweep_extreme = last_low;
         g_sweep.swing_level = swing_lo;
         g_sweep.sweep_time = iTime(_Symbol, _S2_EntryTF, 1);
         g_sweep.confirm_bars_seen = 0;
      }
   }
   else
   {
      // Waiting for confirmation
      g_sweep.confirm_bars_seen++;

      // Confirm criteria: confirm_bars_seen reached AND price still on the right side
      bool confirmed = false;
      double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
      double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);

      if(g_sweep.confirm_bars_seen >= _S2_ConfirmBars)
      {
         if(g_sweep.sweep_dir == -1 && bid < g_sweep.swing_level)
            confirmed = true;   // sell setup, price below swept high
         else if(g_sweep.sweep_dir == 1 && ask > g_sweep.swing_level)
            confirmed = true;   // buy setup, price above swept low
      }

      // Invalidate if price re-takes the swept extreme
      if(g_sweep.sweep_dir == -1 && bid > g_sweep.sweep_extreme)
      {
         g_sweep.waiting_confirm = false;
         return;
      }
      if(g_sweep.sweep_dir == 1 && ask < g_sweep.sweep_extreme)
      {
         g_sweep.waiting_confirm = false;
         return;
      }

      // Stale: clear if too many bars passed without confirm
      if(g_sweep.confirm_bars_seen > _S2_ConfirmBars + 3)
      {
         g_sweep.waiting_confirm = false;
         return;
      }

      if(confirmed)
      {
         double sl_pts, sl_price, tp_price;
         double entry_price;

         if(g_sweep.sweep_dir == 1)
         {
            // Buy
            entry_price = ask;
            sl_price = NormPrice(g_sweep.sweep_extreme - _S2_SL_BufferPts * _Point);
            sl_pts = (entry_price - sl_price) / _Point;
            if(sl_pts <= 0) { g_sweep.waiting_confirm = false; return; }
            tp_price = NormPrice(entry_price + sl_pts * _S2_RR_Ratio * _Point);
         }
         else
         {
            // Sell
            entry_price = bid;
            sl_price = NormPrice(g_sweep.sweep_extreme + _S2_SL_BufferPts * _Point);
            sl_pts = (sl_price - entry_price) / _Point;
            if(sl_pts <= 0) { g_sweep.waiting_confirm = false; return; }
            tp_price = NormPrice(entry_price - sl_pts * _S2_RR_Ratio * _Point);
         }

         double totalLots = CalcLots(sl_pts);
         if(totalLots <= 0) { g_sweep.waiting_confirm = false; return; }

         g_trade.SetExpertMagicNumber(magic);

         if(_S2_HalfTP_Ratio > 0)
         {
            double step = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
            double minL = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
            if(step <= 0) step = 0.01;
            double halfLots = MathRound(totalLots / 2.0 / step) * step;
            if(halfLots < minL) halfLots = minL;

            double tp_half;
            if(g_sweep.sweep_dir == 1)
               tp_half = NormPrice(entry_price + sl_pts * _S2_RR_Ratio * _S2_HalfTP_Ratio * _Point);
            else
               tp_half = NormPrice(entry_price - sl_pts * _S2_RR_Ratio * _S2_HalfTP_Ratio * _Point);

            if(g_sweep.sweep_dir == 1)
            {
               g_trade.Buy(halfLots, _Symbol, 0, sl_price, tp_half, _S2_Comment);
               g_trade.Buy(halfLots, _Symbol, 0, sl_price, tp_price, _S2_Comment);
            }
            else
            {
               g_trade.Sell(halfLots, _Symbol, 0, sl_price, tp_half, _S2_Comment);
               g_trade.Sell(halfLots, _Symbol, 0, sl_price, tp_price, _S2_Comment);
            }
         }
         else
         {
            if(g_sweep.sweep_dir == 1)
               g_trade.Buy(totalLots, _Symbol, 0, sl_price, tp_price, _S2_Comment);
            else
               g_trade.Sell(totalLots, _Symbol, 0, sl_price, tp_price, _S2_Comment);
         }

         g_sweep.waiting_confirm = false;
      }
   }
}

//====================================================================
// OnInit / OnDeinit / OnTick
//====================================================================
int OnInit()
{
   g_trade.SetExpertMagicNumber(_BaseMagic);
   g_trade.SetDeviationInPoints(50);
   g_lastBar_M1 = iTime(_Symbol, PERIOD_M1, 0);
   g_session_day = 0;
   g_balance_day_start = AccountInfoDouble(ACCOUNT_BALANCE);
   g_daily_lock = false;
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason) {}

//====================================================================
// Daily target/loss enforcement (account-level across both streams)
//====================================================================
double SumRealizedToday(datetime dayStart)
{
   double total = 0.0;
   HistorySelect(dayStart, TimeCurrent());
   for(int i = HistoryDealsTotal() - 1; i >= 0; i--)
   {
      ulong tk = HistoryDealGetTicket(i);
      if(tk == 0) continue;
      if(HistoryDealGetString(tk, DEAL_SYMBOL) != _Symbol) continue;
      int magic = (int)HistoryDealGetInteger(tk, DEAL_MAGIC);
      if(magic != _BaseMagic && magic != _BaseMagic + 1) continue;
      // Only count exit deals (entry deals have 0 PnL)
      if(HistoryDealGetInteger(tk, DEAL_ENTRY) != DEAL_ENTRY_OUT) continue;
      total += HistoryDealGetDouble(tk, DEAL_PROFIT);
      total += HistoryDealGetDouble(tk, DEAL_SWAP);
      total += HistoryDealGetDouble(tk, DEAL_COMMISSION);
   }
   return total;
}

double SumUnrealized()
{
   double total = 0.0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong tk = PositionGetTicket(i);
      if(tk == 0) continue;
      if(!PositionSelectByTicket(tk)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      int magic = (int)PositionGetInteger(POSITION_MAGIC);
      if(magic != _BaseMagic && magic != _BaseMagic + 1) continue;
      total += PositionGetDouble(POSITION_PROFIT);
      total += PositionGetDouble(POSITION_SWAP);
   }
   return total;
}

void CloseAllAndCancelPending()
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong tk = PositionGetTicket(i);
      if(tk == 0) continue;
      if(!PositionSelectByTicket(tk)) continue;
      if(PositionGetString(POSITION_SYMBOL) != _Symbol) continue;
      int magic = (int)PositionGetInteger(POSITION_MAGIC);
      if(magic != _BaseMagic && magic != _BaseMagic + 1) continue;
      g_trade.PositionClose(tk);
   }
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong tk = OrderGetTicket(i);
      if(tk == 0) continue;
      if(!OrderSelect(tk)) continue;
      if(OrderGetString(ORDER_SYMBOL) != _Symbol) continue;
      int magic = (int)OrderGetInteger(ORDER_MAGIC);
      if(magic != _BaseMagic && magic != _BaseMagic + 1) continue;
      g_trade.OrderDelete(tk);
   }
}

bool DailyCapsCheck()
{
   // Returns true if trading should be locked for the day.
   datetime now = TimeCurrent();
   MqlDateTime mt;
   TimeToStruct(now, mt);
   mt.hour = 0; mt.min = 0; mt.sec = 0;
   datetime today = StructToTime(mt);

   // Day rollover: reset
   if(today != g_session_day)
   {
      g_session_day = today;
      g_balance_day_start = AccountInfoDouble(ACCOUNT_BALANCE);
      g_daily_lock = false;
   }

   if(g_daily_lock) return true;
   if(g_balance_day_start <= 0) return false;

   double realized = SumRealizedToday(today);
   double unrealized = SumUnrealized();
   double pnl_today = realized + unrealized;

   if(_DailyTargetPct > 0 && pnl_today >= g_balance_day_start * _DailyTargetPct / 100.0)
   {
      CloseAllAndCancelPending();
      g_daily_lock = true;
      return true;
   }
   if(_DailyLossPct > 0 && pnl_today <= -g_balance_day_start * _DailyLossPct / 100.0)
   {
      CloseAllAndCancelPending();
      g_daily_lock = true;
      return true;
   }
   return false;
}

void OnTick()
{
   // Daily cap check on every tick (cheap, mostly returns false)
   if(DailyCapsCheck()) return;

   // Stream processing only on new M1 bar
   if(!IsNewBar(PERIOD_M1, g_lastBar_M1)) return;

   if(_S1_Enabled) ProcessORB();
   if(_S2_Enabled) ProcessSweep();
}
//+------------------------------------------------------------------+

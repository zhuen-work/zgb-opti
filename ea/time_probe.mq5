//+------------------------------------------------------------------+
//| time_probe.mq5                                                   |
//|                                                                  |
//| Prints TimeGMT(), TimeCurrent(), iTime() so we can verify what   |
//| timezone each function returns on this MT5 server.               |
//|                                                                  |
//| Usage: attach to any chart (XAUUSD.sc preferred), check Experts  |
//| tab for the [TIME_PROBE] line, detach.                           |
//|                                                                  |
//| Expected output if Vantage broker = UTC+3 and TimeGMT returns    |
//| real UTC (Session B alignment), with real wall-clock UTC = X:    |
//|   TimeGMT     = X:00          (real UTC)                         |
//|   TimeCurrent = X+3:00        (broker local time)                |
//|   iTime(M5,0) = X+3:00 (rounded down to nearest 5min, broker)    |
//|                                                                  |
//| Expected output if TimeGMT also returns broker time (Session A   |
//| alignment, what we WANT):                                        |
//|   TimeGMT     = X+3:00        (same as broker)                   |
//|   TimeCurrent = X+3:00                                           |
//|   iTime(M5,0) = X+3:00                                           |
//+------------------------------------------------------------------+
#property copyright "ZGB"
#property version   "1.00"
#property strict

int OnInit()
{
   datetime t_gmt = TimeGMT();
   datetime t_cur = TimeCurrent();
   datetime t_bar = iTime(_Symbol, PERIOD_M5, 0);
   datetime t_local = TimeLocal();

   PrintFormat("[TIME_PROBE] TimeGMT=%s  TimeCurrent=%s  iTime(M5,0)=%s  TimeLocal=%s",
               TimeToString(t_gmt,     TIME_DATE | TIME_SECONDS),
               TimeToString(t_cur,     TIME_DATE | TIME_SECONDS),
               TimeToString(t_bar,     TIME_DATE | TIME_SECONDS),
               TimeToString(t_local,   TIME_DATE | TIME_SECONDS));

   PrintFormat("[TIME_PROBE] epoch_GMT=%d  epoch_Current=%d  epoch_iTime=%d  epoch_Local=%d",
               (int)t_gmt, (int)t_cur, (int)t_bar, (int)t_local);

   PrintFormat("[TIME_PROBE] TimeGMTOffset()=%d sec (= %.1f hours)",
               (int)TimeGMTOffset(), TimeGMTOffset() / 3600.0);

   // Compute current real UTC from system: today's date + (local - GMT_offset) — but
   // can't easily do this in MQL5. User cross-checks against their system clock.
   PrintFormat("[TIME_PROBE] >>> Compare TimeGMT above with system UTC clock. If equal, TimeGMT() returns REAL UTC. If TimeGMT is +3h ahead of system, TimeGMT returns BROKER time.");

   return INIT_SUCCEEDED;
}

void OnTick() {}

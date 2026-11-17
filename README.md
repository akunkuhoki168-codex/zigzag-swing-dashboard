# Zigzag Swing dashboard

Public live view of the **Zigzag Swing** alert-only bot (Binance USD-M Futures ATR zigzag BUY/SELL, plus a 1-minute ADX scanner).

Live page: https://dedkovic441-hub.github.io/zigzag-swing-dashboard/

`data.json` is a compact first-paint snapshot (the market list, paper accounts, and latest signals), regenerated and pushed about every 2 minutes when anything material has changed. Chart markers and the longer trade and alert history are in `data_detail.json`, which the page loads after the list appears. The page renders from `data.json` immediately and does not wait on the chart library or Binance. It reloads `data.json` every 30 seconds.

All times on the page are in WIB (Asia/Jakarta, UTC+7).

This is an alert dashboard only. No orders are placed. Not financial advice.

The live chart is at https://dedkovic441-hub.github.io/zigzag-swing/.

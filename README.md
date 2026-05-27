# Binance/MEXC funding alert bot

The bot compares Binance USD-M Futures and MEXC Futures funding rates for shared symbols. It sends a Telegram alert when the absolute spread is greater than `THRESHOLD_PERCENT`.

## Alert logic

- First alert: sent when `abs(Binance funding - MEXC funding) > THRESHOLD_PERCENT`.
- Follow-up alert for the same symbol and same direction: sent only when the spread grows by at least `ALERT_STEP_PERCENT` from the last alerted spread.
- If the spread falls below the threshold, the alert state resets. A later breakout above the threshold will alert again.

Example with defaults:

- `THRESHOLD_PERCENT=0.1`
- `ALERT_STEP_PERCENT=0.15`

If `BTCUSDT` first alerts at `0.12%`, the next alert for the same direction needs at least `0.27%`, then `0.42%`, and so on.

## Run

Requires Python 3.10+.

```powershell
Copy-Item .env.example .env
notepad .env
python .\funding_alert_bot.py
```

Telegram-only test:

```powershell
python .\funding_alert_bot.py --test-telegram
```

Dry run without Telegram sending:

```env
DRY_RUN=true
```

## Telegram

`TELEGRAM_CHAT_ID` is optional for running the monitor, but Telegram cannot send a real message without a target chat. If it is empty, alerts are logged as skipped.

For a channel, add the bot as an admin and set `TELEGRAM_CHAT_ID` to the channel username, for example `@my_channel`, or to its numeric id.

For a separate topic/thread in a forum supergroup, set:

```env
TELEGRAM_CHAT_ID=-1001234567890
TELEGRAM_MESSAGE_THREAD_ID=123
```

Telegram channels themselves do not use `message_thread_id` the same way forum supergroups do. If you mean a discussion topic, use the linked discussion supergroup topic id.

## Settings

- `THRESHOLD_PERCENT=0.1` - spread threshold in percent.
- `ALERT_STEP_PERCENT=0.15` - additional spread growth required for repeat alert.
- `POLL_INTERVAL_SECONDS=60` - check interval.
- `QUOTE_ASSETS=USDT` - quote assets to compare.
- `DRY_RUN=true` - log alerts instead of sending Telegram messages.
- `TELEGRAM_MESSAGE_THREAD_ID` - optional Telegram forum topic id.

## API endpoints

- Binance `GET /fapi/v1/premiumIndex`
- Binance `GET /fapi/v1/fundingInfo`
- MEXC `GET /api/v1/contract/ticker`
- MEXC `GET /api/v1/contract/funding_rate/{symbol}`

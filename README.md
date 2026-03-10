# Meds Bot (Telegram)

Tracks medicines with time-based reminders + buttons. Buttons only appear within a configured time window or period.

## 1) Create Telegram bot token
1. Open Telegram → chat with **@BotFather**.
2. Send `/newbot` and follow the prompts.
3. Copy the token.

## 2) Configure
```bash
cp config.example.json config.json
```
Edit `config.json`:
- `botToken`: your BotFather token
- `targetChatId`: the group chat id (after adding the bot)
- `admins`: Telegram user ids who can run admin commands

## 3) Install + run
```bash
npm install
npm run dev
```

## 4) Add bot to the group
- Add the bot to your group.
- Send a message in the group.
- The bot will log the chat id in console if `targetChatId` is empty.

## Commands
- `/status` → shows today’s medicine status
- `/due` → shows currently pending medicines with buttons
- `/setchat` → sets current chat as target (admin only)
- `/ping` → health check

## Notes
- Reminders repeat twice by default (15 minutes apart).
- Timed doses use `windowMinutes` and `windowStartOffsetMinutes`.
- Period doses can use `morning`, `afternoon`, or `day` with optional `reminderTime`.

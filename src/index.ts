import fs from "node:fs";
import path from "node:path";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
import timezone from "dayjs/plugin/timezone.js";
import { Telegraf, Markup } from "telegraf";
import { open } from "sqlite";
import sqlite3 from "sqlite3";

dayjs.extend(utc);
dayjs.extend(timezone);

const CONFIG_PATH = path.resolve(process.cwd(), "config.json");
if (!fs.existsSync(CONFIG_PATH)) {
  console.error("Missing config.json. Copy config.example.json and edit it.");
  process.exit(1);
}

const config = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf-8"));
const {
  botToken,
  timezone: tz,
  targetChatId,
  admins = [],
  reminders = { repeatCount: 2, repeatEveryMinutes: 15 },
  defaults = { windowMinutes: 120, windowStartOffsetMinutes: 0 },
  meds = []
} = config;

if (!botToken) {
  console.error("botToken missing in config.json");
  process.exit(1);
}

const bot = new Telegraf(botToken);
let telegramBackoffUntil = 0;
const deferredSlotUntil = new Map<string, number>();

function noteTelegramBackoff(err: any) {
  const retryAfter = err?.response?.parameters?.retry_after;
  if (!retryAfter) return false;
  const until = Date.now() + (Number(retryAfter) * 1000);
  telegramBackoffUntil = Math.max(telegramBackoffUntil, until);
  console.warn(`Telegram rate limited; backing off for ${retryAfter}s`);
  return true;
}

function makeSlotKey(chatId: string, dateStr: string, doses: any[]) {
  return `${chatId}|${dateStr}|${getDoseDisplayTime(doses[0])}`;
}

function noteDeferredSlot(slotKey: string, err: any) {
  const retryAfter = err?.response?.parameters?.retry_after;
  if (!retryAfter) return false;
  const until = Date.now() + (Number(retryAfter) * 1000);
  deferredSlotUntil.set(slotKey, until);
  return true;
}

function isDeferredSlot(slotKey: string) {
  const until = deferredSlotUntil.get(slotKey);
  if (!until) return false;
  if (Date.now() >= until) {
    deferredSlotUntil.delete(slotKey);
    return false;
  }
  return true;
}

function isTelegramBackedOff() {
  return Date.now() < telegramBackoffUntil;
}

bot.catch((err, ctx) => {
  noteTelegramBackoff(err);
  console.error(`Bot error for update ${ctx.updateType}`, err);
});

async function safeReply(ctx: any, text: string, extra?: any) {
  if (isTelegramBackedOff()) return null;
  try {
    return await ctx.reply(text, extra);
  } catch (err) {
    noteTelegramBackoff(err);
    console.error("Reply failed", err);
    try {
      console.error(JSON.stringify(err));
    } catch {}
    return null;
  }
}

const MAIN_KEYBOARD = Markup.keyboard([
  ["Pendientes", "Status"]
]).resize();

const dbPath = path.resolve(process.cwd(), "data", "meds.db");
fs.mkdirSync(path.dirname(dbPath), { recursive: true });

const db = await open({ filename: dbPath, driver: sqlite3.Database });
await db.exec(`
  CREATE TABLE IF NOT EXISTS doses_given (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurrence_id TEXT NOT NULL,
    dose_id TEXT NOT NULL,
    given_at TEXT NOT NULL,
    user_id TEXT,
    user_name TEXT,
    chat_id TEXT
  );
  CREATE TABLE IF NOT EXISTS reminders (
    occurrence_id TEXT PRIMARY KEY,
    dose_id TEXT NOT NULL,
    date TEXT NOT NULL,
    sent_count INTEGER NOT NULL DEFAULT 0,
    last_sent_at TEXT,
    message_id TEXT,
    chat_id TEXT
  );
`);

function isAdmin(userId?: number) {
  return userId && admins.includes(userId);
}

function getDefaultPeriodReminderTime(period: string) {
  if (period === "morning") return "08:00";
  if (period === "afternoon") return "17:00";
  if (period === "evening") return "21:00";
  return "09:00";
}

function getDoseWindow(dose: any, now: dayjs.Dayjs) {
  const day = now.tz(tz).startOf("day");

  if (dose.period) {
    const reminderTime = dose.reminderTime ?? getDefaultPeriodReminderTime(dose.period);
    const [baseH, baseM] = reminderTime.split(":").map(Number);
    const base = day.hour(baseH).minute(baseM).second(0).millisecond(0);

    if (dose.period === "morning") {
      const start = day.hour(6).minute(0).second(0).millisecond(0);
      const end = day.hour(14).minute(0).second(0).millisecond(0);
      return { start, end, base };
    }

    if (dose.period === "afternoon") {
      const start = day.hour(14).minute(1).second(0).millisecond(0);
      const end = day.hour(23).minute(59).second(59).millisecond(999);
      return { start, end, base };
    }

    if (dose.period === "evening") {
      const start = day.hour(18).minute(0).second(0).millisecond(0);
      const end = day.hour(23).minute(59).second(59).millisecond(999);
      return { start, end, base };
    }

    const start = day.hour(6).minute(0).second(0).millisecond(0);
    const end = day.hour(23).minute(59).second(59).millisecond(999);
    return { start, end, base };
  }

  const windowMinutes = dose.windowMinutes ?? defaults.windowMinutes;
  const windowStartOffsetMinutes =
    dose.windowStartOffsetMinutes ?? defaults.windowStartOffsetMinutes;

  const [h, m] = dose.time.split(":").map(Number);
  const base = day.hour(h).minute(m).second(0).millisecond(0);
  const start = base.add(windowStartOffsetMinutes, "minute");
  const end = start.add(windowMinutes, "minute");
  return { start, end, base };
}

function getDoseDisplayTime(dose: any) {
  if (dose.period === "morning") return "Mañana";
  if (dose.period === "afternoon") return "Tarde";
  if (dose.period === "evening") return "Noche";
  if (dose.period === "day") return "Día";
  return dose.time;
}

function getDoseSortKey(dose: any) {
  if (dose.period === "morning") return "06:00";
  if (dose.period === "day") return "12:00";
  if (dose.period === "afternoon") return "14:01";
  if (dose.period === "evening") return "18:00";
  return dose.time;
}

function isDoseActiveToday(dose: any, now: dayjs.Dayjs) {
  if (dose.schedule === "daily") return true;
  if (dose.schedule === "everyOtherDay") {
    const startDate = dayjs.tz(dose.startDate, tz).startOf("day");
    const today = now.tz(tz).startOf("day");
    const diff = today.diff(startDate, "day");
    return diff >= 0 && diff % 2 === 0;
  }
  return false;
}

function occurrenceId(dose: any, dateStr: string) {
  return `${dose.id}:${dateStr}`;
}

async function doseGiven(occId: string) {
  const row = await db.get(
    "SELECT 1 FROM doses_given WHERE occurrence_id = ? LIMIT 1",
    occId
  );
  return !!row;
}

async function markDoseGiven(opts: {
  occId: string;
  doseId: string;
  userId?: number;
  userName?: string;
  chatId?: string;
}) {
  await db.run(
    `INSERT INTO doses_given (occurrence_id, dose_id, given_at, user_id, user_name, chat_id)
     VALUES (?, ?, ?, ?, ?, ?)` ,
    opts.occId,
    opts.doseId,
    dayjs().toISOString(),
    opts.userId?.toString() ?? null,
    opts.userName ?? null,
    opts.chatId ?? null
  );
}

async function updateReminder(occId: string, patch: any) {
  const existing = await db.get("SELECT * FROM reminders WHERE occurrence_id = ?", occId);
  if (!existing) {
    await db.run(
      `INSERT INTO reminders (occurrence_id, dose_id, date, sent_count, last_sent_at, message_id, chat_id)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      occId,
      patch.dose_id,
      patch.date,
      patch.sent_count ?? 0,
      patch.last_sent_at ?? null,
      patch.message_id ?? null,
      patch.chat_id ?? null
    );
    return;
  }
  await db.run(
    `UPDATE reminders
     SET sent_count = COALESCE(?, sent_count),
         last_sent_at = COALESCE(?, last_sent_at),
         message_id = COALESCE(?, message_id),
         chat_id = COALESCE(?, chat_id)
     WHERE occurrence_id = ?`,
    patch.sent_count ?? null,
    patch.last_sent_at ?? null,
    patch.message_id ?? null,
    patch.chat_id ?? null,
    occId
  );
}

async function maybeClearButtons(now: dayjs.Dayjs) {
  const rows = await db.all("SELECT * FROM reminders WHERE message_id IS NOT NULL");
  for (const row of rows) {
    const dose = meds.find((m: any) => m.id === row.dose_id);
    if (!dose) continue;
    const date = dayjs.tz(row.date, tz);
    const { start, end } = getDoseWindow(dose, date);
    if (now.isAfter(end)) {
      try {
        await bot.telegram.editMessageReplyMarkup(
          row.chat_id,
          Number(row.message_id),
          undefined,
          undefined
        );
      } catch {
        // ignore
      }
      await updateReminder(row.occurrence_id, { message_id: null });
    }
  }
}

function buildButtons(doses: any[], dateStr: string) {
  const buttons = doses.map((dose) =>
    Markup.button.callback(
      `✅ ${dose.label}`,
      `give:${dose.id}:${dateStr}`
    )
  );
  return Markup.inlineKeyboard(buttons, { columns: 1 });
}

async function sendDoseMessage(chatId: string, doses: any[], dateStr: string) {
  const time = getDoseDisplayTime(doses[0]);
  const labels = doses.map((d: any) => `• ${d.label}`).join("\n");
  const text = `🕒 Medicación (${time})\n${labels}\n\nPulsa para marcar como dada.`;
  try {
    const msg = await bot.telegram.sendMessage(
      chatId,
      text,
      buildButtons(doses, dateStr)
    );
    return msg.message_id;
  } catch (err) {
    console.error("Send reminder failed", err);
    return null;
  }
}

async function sendReminder(doses: any[], dateStr: string) {
  if (!targetChatId) return;
  return sendDoseMessage(targetChatId, doses, dateStr);
}

async function getDueDoses(now: dayjs.Dayjs) {
  const dateStr = now.format("YYYY-MM-DD");
  const pending = meds.filter((dose: any) => isDoseActiveToday(dose, now));

  const dueDoses: any[] = [];
  for (const dose of pending) {
    const { start, end } = getDoseWindow(dose, now);
    if (now.isBefore(start) || now.isAfter(end)) continue;
    const occId = occurrenceId(dose, dateStr);
    if (await doseGiven(occId)) continue;
    dueDoses.push(dose);
  }

  return dueDoses
    .slice()
    .sort((a: any, b: any) => getDoseSortKey(a).localeCompare(getDoseSortKey(b)) || a.label.localeCompare(b.label));
}

async function sendDueButtonsToChat(chatId: string, now: dayjs.Dayjs) {
  const dateStr = now.format("YYYY-MM-DD");
  const dueDoses = await getDueDoses(now);

  if (dueDoses.length === 0) {
    return bot.telegram.sendMessage(chatId, "No hay medicinas pendientes en este momento.", MAIN_KEYBOARD);
  }

  const bySlot: Record<string, any[]> = {};
  for (const dose of dueDoses) {
    const slot = getDoseDisplayTime(dose);
    bySlot[slot] = bySlot[slot] || [];
    bySlot[slot].push(dose);
  }

  for (const group of Object.values(bySlot)) {
    const slotKey = makeSlotKey(chatId, dateStr, group);
    if (isDeferredSlot(slotKey)) continue;

    const existingRows = [];
    for (const dose of group) {
      const occId = occurrenceId(dose, dateStr);
      const reminder = await db.get("SELECT * FROM reminders WHERE occurrence_id = ?", occId);
      if (reminder) existingRows.push(reminder);
    }

    const hasActiveMessage = existingRows.some((row: any) => row?.message_id && row?.chat_id === chatId);
    if (hasActiveMessage) continue;

    const messageId = await sendDoseMessage(chatId, group, dateStr);
    if (!messageId) continue;

    for (const dose of group) {
      const occId = occurrenceId(dose, dateStr);
      const reminder = await db.get("SELECT * FROM reminders WHERE occurrence_id = ?", occId);
      await updateReminder(occId, {
        dose_id: dose.id,
        date: dateStr,
        sent_count: reminder?.sent_count ?? 0,
        last_sent_at: reminder?.last_sent_at ?? null,
        message_id: messageId.toString(),
        chat_id: chatId
      });
    }
  }

  return null;
}

async function tick() {
  if (isTelegramBackedOff()) return;
  const now = dayjs().tz(tz);
  const dateStr = now.format("YYYY-MM-DD");

  await maybeClearButtons(now);

  const activeDoses = meds.filter((dose: any) => isDoseActiveToday(dose, now));
  const dueDoses: any[] = [];

  for (const dose of activeDoses) {
    const { start, end, base } = getDoseWindow(dose, now);
    if (now.isBefore(start) || now.isAfter(end)) continue;

    const occId = occurrenceId(dose, dateStr);
    if (await doseGiven(occId)) continue;

    const reminder = await db.get("SELECT * FROM reminders WHERE occurrence_id = ?", occId);
    const lastSentAt = reminder?.last_sent_at ? dayjs(reminder.last_sent_at) : null;
    const sentCount = reminder?.sent_count ?? 0;
    const allowRepeats = !dose.period;
    const maxSends = allowRepeats ? 1 + reminders.repeatCount : 1;

    const shouldSendInitial = sentCount === 0 && now.isAfter(base);
    const shouldSendRepeat =
      allowRepeats &&
      sentCount > 0 &&
      sentCount < maxSends &&
      lastSentAt &&
      now.diff(lastSentAt, "minute") >= reminders.repeatEveryMinutes;

    if (shouldSendInitial || shouldSendRepeat) {
      dueDoses.push(dose);
    }
  }

  if (dueDoses.length === 0) return;

  // Group by display slot so a single message is sent per time slot / period
  const byTime: Record<string, any[]> = {};
  for (const dose of dueDoses) {
    const slot = getDoseDisplayTime(dose);
    byTime[slot] = byTime[slot] || [];
    byTime[slot].push(dose);
  }

  for (const [, group] of Object.entries(byTime)) {
    let hasActiveMessage = false;
    for (const dose of group) {
      const occId = occurrenceId(dose, dateStr);
      const reminder = await db.get("SELECT * FROM reminders WHERE occurrence_id = ?", occId);
      if (reminder?.message_id && reminder?.chat_id === targetChatId) {
        hasActiveMessage = true;
        break;
      }
    }
    if (hasActiveMessage) continue;

    const messageId = await sendReminder(group, dateStr);
    if (!messageId) continue;

    for (const dose of group) {
      const occId = occurrenceId(dose, dateStr);
      const reminder = await db.get("SELECT * FROM reminders WHERE occurrence_id = ?", occId);
      const sentCount = (reminder?.sent_count ?? 0) + 1;
      await updateReminder(occId, {
        dose_id: dose.id,
        date: dateStr,
        sent_count: sentCount,
        last_sent_at: now.toISOString(),
        message_id: messageId.toString(),
        chat_id: targetChatId
      });
    }
  }
}

bot.start((ctx) => safeReply(ctx, "Meds bot online. Use /status", MAIN_KEYBOARD));

bot.telegram.getMe().then((me) => {
  console.log(`Bot started: @${me.username}`);
}).catch((err) => {
  console.error("Bot start error", err);
});

bot.command("ping", (ctx) => safeReply(ctx, "pong", MAIN_KEYBOARD));

bot.on("message", async (ctx, next) => {
  const chatId = ctx.chat?.id?.toString();
  const text = (ctx.message as any)?.text ?? "";
  console.log(`Message from ${chatId}: ${text}`);
  if (!config.targetChatId && chatId) {
    console.log(`Detected chat id: ${chatId}`);
  }
  return next();
});

bot.hears(/^\/setchat(?:@\w+)?$/i, (ctx) => {
  if (!isAdmin(ctx.from?.id)) return safeReply(ctx, "Not allowed");
  const chatId = ctx.chat?.id?.toString();
  if (!chatId) return;
  try {
    config.targetChatId = chatId;
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2));
  } catch (err) {
    console.error("Failed to write config", err);
    return safeReply(ctx, "Failed to save chat id");
  }
  return safeReply(ctx, `Chat set to ${chatId}`, MAIN_KEYBOARD);
});

async function buildStatusText(now: dayjs.Dayjs) {
  const dateStr = now.format("YYYY-MM-DD");
  const activeDoses = meds
    .filter((dose: any) => isDoseActiveToday(dose, now))
    .slice()
    .sort((a: any, b: any) => getDoseSortKey(a).localeCompare(getDoseSortKey(b)) || a.label.localeCompare(b.label));

  const lines: string[] = [];

  for (const dose of activeDoses) {
    const { end } = getDoseWindow(dose, now);
    const occId = occurrenceId(dose, dateStr);
    const given = await doseGiven(occId);
    const state = given ? "✅" : now.isAfter(end) ? "⏱️" : "⏳";
    lines.push(`${state} ${getDoseDisplayTime(dose)} — ${dose.label}`);
  }

  return lines.length ? lines.join("\n") : "No doses scheduled today.";
}

async function handleStatus(ctx: any) {
  const now = dayjs().tz(tz);
  const text = await buildStatusText(now);
  await safeReply(ctx, text, MAIN_KEYBOARD);
}

async function handleDue(ctx: any) {
  const now = dayjs().tz(tz);
  const chatId = ctx.chat?.id?.toString();
  if (!chatId) return safeReply(ctx, "No pude detectar este chat.", MAIN_KEYBOARD);
  return sendDueButtonsToChat(chatId, now);
}

bot.hears(/^\/status(?:@\w+)?$/i, handleStatus);
bot.hears(/^\/due(?:@\w+)?$/i, handleDue);
bot.hears(/^status$/i, handleStatus);
bot.hears(/^pendientes$/i, handleDue);

bot.action(/give:(.+):(.+)/, async (ctx) => {
  const doseId = ctx.match[1];
  const dateStr = ctx.match[2];
  const dose = meds.find((m: any) => m.id === doseId);
  if (!dose) return ctx.answerCbQuery("Unknown dose");

  const occId = occurrenceId(dose, dateStr);
  if (await doseGiven(occId)) {
    return ctx.answerCbQuery("Already marked");
  }

  await markDoseGiven({
    occId,
    doseId,
    userId: ctx.from?.id,
    userName: ctx.from?.username ?? ctx.from?.first_name,
    chatId: ctx.chat?.id?.toString()
  });

  try {
    const inline = (ctx.callbackQuery as any)?.message?.reply_markup?.inline_keyboard ?? [];
    const filtered = inline
      .map((row: any[]) => row.filter((button: any) => button.callback_data !== `give:${doseId}:${dateStr}`))
      .filter((row: any[]) => row.length > 0);

    if (!isTelegramBackedOff()) {
      await ctx.editMessageReplyMarkup(filtered.length > 0 ? { inline_keyboard: filtered } : undefined);
    }
  } catch (err) {
    noteTelegramBackoff(err);
    console.error("Failed to update reminder buttons", err);
  }

  try {
    if (!isTelegramBackedOff()) {
      await ctx.answerCbQuery("Marked ✅");
    }
  } catch (err) {
    noteTelegramBackoff(err);
    console.error("Failed to answer callback query", err);
  }
});

const BOT_COMMANDS = [
  { command: "status", description: "Ver estado de medicación" },
  { command: "due", description: "Mostrar medicinas pendientes con botones" },
  { command: "setchat", description: "Fijar este chat como destino (admin)" },
  { command: "ping", description: "Comprobar estado del bot" }
];

bot.launch().then(async () => {
  try {
    await bot.telegram.setMyCommands(BOT_COMMANDS);
    console.log("Bot commands registered");
  } catch (err) {
    console.error("Failed to register bot commands", err);
  }
});

setInterval(() => {
  tick().catch((err) => console.error("tick error", err));
}, 60_000);

// Run immediately on startup
void tick();

process.once("SIGINT", () => bot.stop("SIGINT"));
process.once("SIGTERM", () => bot.stop("SIGTERM"));

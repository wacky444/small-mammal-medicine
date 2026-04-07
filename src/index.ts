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
const chatQueues = new Map<string, Promise<any>>();

const TELEGRAM_GROUP_MESSAGES_PER_MIN = 20;
const TELEGRAM_SEND_INTERVAL_MS = Math.ceil(60000 / TELEGRAM_GROUP_MESSAGES_PER_MIN);

type ReminderJob = {
  slotKey: string;
  chatId: string;
  dateStr: string;
  doses: any[];
  source: "scheduled" | "manual";
};

type SlotStatus = "queued" | "deferred";
type SlotState = {
  status: SlotStatus;
  job: ReminderJob;
  deferredUntil?: number;
  nextAttemptAt?: number;
  deferredAt?: number;
};

const reminderQueue: ReminderJob[] = [];
/** Single source of truth for per-slot ownership. A slot absent from this map is "ready". */
const slotStates = new Map<string, SlotState>();
let processingReminderQueue = false;
let nextReminderSendAt = 0;
let queueDay = dayjs().tz(tz).format("YYYY-MM-DD");

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

function isTelegramBackedOff() {
  return Date.now() < telegramBackoffUntil;
}

async function enqueueChatOp<T>(chatKey: string, op: () => Promise<T>, retryOn429 = true): Promise<T | null> {
  const prev = chatQueues.get(chatKey) ?? Promise.resolve();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  chatQueues.set(chatKey, prev.finally(() => gate));

  await prev.catch(() => {});

  try {
    const waitMs = telegramBackoffUntil - Date.now();
    if (waitMs > 0) {
      await sleep(waitMs);
    }

    try {
      return await op();
    } catch (err: any) {
      const hadBackoff = noteTelegramBackoff(err);
      if (retryOn429 && hadBackoff) {
        const retryWaitMs = telegramBackoffUntil - Date.now();
        if (retryWaitMs > 0) {
          await sleep(retryWaitMs);
        }
        return await op();
      }
      throw err;
    }
  } catch (err) {
    throw err;
  } finally {
    release();
    if (chatQueues.get(chatKey) === gate) {
      chatQueues.delete(chatKey);
    }
  }
}

bot.catch((err, ctx) => {
  noteTelegramBackoff(err);
  console.error(`Bot error for update ${ctx.updateType}`, err);
});

async function safeReply(ctx: any, text: string, extra?: any) {
  const chatKey = ctx.chat?.id?.toString?.() ?? "global";
  try {
    return await enqueueChatOp(chatKey, () => ctx.reply(text, extra));
  } catch (err) {
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
  CREATE TABLE IF NOT EXISTS reminder_debug (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    stage TEXT NOT NULL,
    occurrence_id TEXT,
    dose_id TEXT,
    slot TEXT,
    details TEXT
  );
  CREATE TABLE IF NOT EXISTS food_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    pienso_grams INTEGER NOT NULL,
    lata_halves INTEGER NOT NULL,
    colin_sobre_halves INTEGER NOT NULL DEFAULT 0,
    colin_churu_quarters INTEGER NOT NULL DEFAULT 0,
    done_at TEXT NOT NULL,
    user_id TEXT,
    user_name TEXT,
    UNIQUE(date, chat_id)
  );
  CREATE TABLE IF NOT EXISTS tracker_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    event_date TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    user_id TEXT,
    user_name TEXT
  );
`);

async function ensureColumn(table: string, column: string, ddl: string) {
  const cols = await db.all(`PRAGMA table_info(${table})`);
  if (!cols.some((c: any) => c.name === column)) {
    await db.exec(`ALTER TABLE ${table} ADD COLUMN ${ddl}`);
  }
}

await ensureColumn("food_log", "colin_sobre_halves", "colin_sobre_halves INTEGER NOT NULL DEFAULT 0");
await ensureColumn("food_log", "colin_churu_quarters", "colin_churu_quarters INTEGER NOT NULL DEFAULT 0");

function isAdmin(userId?: number) {
  return userId && admins.includes(userId);
}

function getDefaultPeriodReminderTime(period: string) {
  if (period === "earlyMorning") return "05:00";
  if (period === "morning") return "08:00";
  if (period === "afternoon") return "17:00";
  if (period === "evening") return "21:00";
  return "09:00";
}

function atLocalTime(now: dayjs.Dayjs, hhmm: string) {
  const dateStr = now.tz(tz).format("YYYY-MM-DD");
  return dayjs.tz(`${dateStr} ${hhmm}`, "YYYY-MM-DD HH:mm", tz).second(0).millisecond(0);
}

function getDoseWindow(dose: any, now: dayjs.Dayjs) {
  if (dose.period) {
    const reminderTime = dose.reminderTime ?? getDefaultPeriodReminderTime(dose.period);
    const base = atLocalTime(now, reminderTime);

    if (dose.period === "earlyMorning") {
      const start = atLocalTime(now, "00:00");
      const end = atLocalTime(now, "05:59").second(59).millisecond(999);
      return { start, end, base };
    }

    if (dose.period === "morning") {
      const start = atLocalTime(now, "06:00");
      const end = atLocalTime(now, "14:00");
      return { start, end, base };
    }

    if (dose.period === "afternoon") {
      const start = atLocalTime(now, "14:01");
      const end = atLocalTime(now, "23:59").second(59).millisecond(999);
      return { start, end, base };
    }

    if (dose.period === "evening") {
      const start = atLocalTime(now, "18:00");
      const end = atLocalTime(now, "23:59").second(59).millisecond(999);
      return { start, end, base };
    }

    const start = atLocalTime(now, "06:00");
    const end = atLocalTime(now, "23:59").second(59).millisecond(999);
    return { start, end, base };
  }

  const windowMinutes = dose.windowMinutes ?? defaults.windowMinutes;
  const windowStartOffsetMinutes =
    dose.windowStartOffsetMinutes ?? defaults.windowStartOffsetMinutes;

  const base = atLocalTime(now, dose.time);
  const start = base.add(windowStartOffsetMinutes, "minute");
  const end = start.add(windowMinutes, "minute");
  return { start, end, base };
}

function getDoseDisplayTime(dose: any) {
  if (dose.period === "earlyMorning") return "Madrugada";
  if (dose.period === "morning") return "Mañana";
  if (dose.period === "afternoon") return "Tarde";
  if (dose.period === "evening") return "Noche";
  if (dose.period === "day") return "Día";
  return dose.time;
}

function getDoseSortKey(dose: any) {
  if (dose.period === "earlyMorning") return "00:00";
  if (dose.period === "morning") return "06:00";
  if (dose.period === "day") return "12:00";
  if (dose.period === "afternoon") return "14:01";
  if (dose.period === "evening") return "18:00";
  return dose.time;
}

function isDoseActiveToday(dose: any, now: dayjs.Dayjs) {
  const today = now.tz(tz).startOf("day");

  if (dose.endDate) {
    const endDate = dayjs.tz(dose.endDate, tz).startOf("day");
    if (today.isAfter(endDate)) return false;
  }

  if (dose.schedule === "daily") return true;
  if (dose.schedule === "everyOtherDay") {
    const startDate = dayjs.tz(dose.startDate, tz).startOf("day");
    const diff = today.diff(startDate, "day");
    return diff >= 0 && diff % 2 === 0;
  }
  return false;
}

function occurrenceId(dose: any, dateStr: string) {
  return `${dose.id}:${dateStr}`;
}

async function debugLog(stage: string, opts: { occurrence_id?: string; dose_id?: string; slot?: string; details?: any } = {}) {
  try {
    await db.run(
      `INSERT INTO reminder_debug (ts, stage, occurrence_id, dose_id, slot, details)
       VALUES (?, ?, ?, ?, ?, ?)`,
      dayjs().toISOString(),
      stage,
      opts.occurrence_id ?? null,
      opts.dose_id ?? null,
      opts.slot ?? null,
      opts.details ? JSON.stringify(opts.details) : null
    );
  } catch {
    // ignore debug logging failures
  }
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
    await debugLog("updateReminder.inserted", { occurrence_id: occId, dose_id: patch?.dose_id, details: { message_id: patch?.message_id, sent_count: patch?.sent_count, source: patch?.source } });
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

type FoodState = {
  piensoGrams: number;
  lataHalves: number;
  colinSobreHalves: number;
  colinChuruQuarters: number;
  history: Array<{ piensoDelta: number; lataDelta: number; colinSobreDelta: number; colinChuruDelta: number }>;
};

const foodStates = new Map<string, FoodState>();

function foodStateKey(chatId: string, dateStr: string) {
  return `${chatId}:${dateStr}`;
}

function getOrCreateFoodState(chatId: string, dateStr: string) {
  const key = foodStateKey(chatId, dateStr);
  const existing = foodStates.get(key);
  if (existing) return existing;
  const created: FoodState = { piensoGrams: 0, lataHalves: 0, colinSobreHalves: 0, colinChuruQuarters: 0, history: [] };
  foodStates.set(key, created);
  return created;
}

function foodText(dateStr: string, s: FoodState) {
  const lataText = `${(s.lataHalves / 2).toFixed(1)}/1`;
  const piensoTarget = 45;
  const piensoProgress = `${s.piensoGrams}/${piensoTarget}g`;
  const colinSobreText = `${(s.colinSobreHalves / 2).toFixed(1)}/1`;
  const colinChuruText = `${(s.colinChuruQuarters / 4).toFixed(2)}/1`;
  return [
    `🍽️ Comida (${dateStr})`,
    `• Mosti pienso: ${piensoProgress}`,
    `• Mosti lata: ${lataText}`,
    `• Colin sobre: ${colinSobreText}`,
    `• Colin churu: ${colinChuruText}`
  ].join("\n");
}

function foodKeyboard(chatId: string, dateStr: string, state: FoodState) {
  const key = foodStateKey(chatId, dateStr);
  return Markup.inlineKeyboard([
    [
      Markup.button.callback("+5g pienso", `food:add:${key}:5:0:0:0`),
      Markup.button.callback("+10g pienso", `food:add:${key}:10:0:0:0`),
      Markup.button.callback("+20g pienso", `food:add:${key}:20:0:0:0`)
    ],
    [
      Markup.button.callback("+media lata mosti", `food:add:${key}:0:1:0:0`)
    ],
    [
      Markup.button.callback("+1/2 sobre colin", `food:add:${key}:0:0:1:0`),
      Markup.button.callback("+1 sobre colin", `food:add:${key}:0:0:2:0`)
    ],
    [
      Markup.button.callback("+1/4 churu", `food:add:${key}:0:0:0:1`),
      Markup.button.callback("+1/2 churu", `food:add:${key}:0:0:0:2`),
      Markup.button.callback("+1 churu", `food:add:${key}:0:0:0:4`)
    ],
    [
      Markup.button.callback("↩️ Undo", `food:undo:${key}`),
      Markup.button.callback("🧹 Reset", `food:reset:${key}`),
      Markup.button.callback("✅ Done", `food:done:${key}`)
    ]
  ]);
}

async function upsertFoodLog(opts: {
  dateStr: string;
  chatId: string;
  piensoGrams: number;
  lataHalves: number;
  colinSobreHalves: number;
  colinChuruQuarters: number;
  userId?: number;
  userName?: string;
}) {
  await db.run(
    `INSERT INTO food_log (date, chat_id, pienso_grams, lata_halves, colin_sobre_halves, colin_churu_quarters, done_at, user_id, user_name)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(date, chat_id) DO UPDATE SET
       pienso_grams = excluded.pienso_grams,
       lata_halves = excluded.lata_halves,
       colin_sobre_halves = excluded.colin_sobre_halves,
       colin_churu_quarters = excluded.colin_churu_quarters,
       done_at = excluded.done_at,
       user_id = excluded.user_id,
       user_name = excluded.user_name`,
    opts.dateStr,
    opts.chatId,
    opts.piensoGrams,
    opts.lataHalves,
    opts.colinSobreHalves,
    opts.colinChuruQuarters,
    dayjs().toISOString(),
    opts.userId?.toString() ?? null,
    opts.userName ?? null
  );
}

async function sendDoseMessageNow(chatId: string, doses: any[], dateStr: string) {
  const time = getDoseDisplayTime(doses[0]);
  const labels = doses.map((d: any) => `• ${d.label}`).join("\n");
  const text = `🕒 Medicación (${time})\n${labels}`;
  const msg = await bot.telegram.sendMessage(
    chatId,
    text,
    buildButtons(doses, dateStr)
  );
  return msg.message_id;
}

function enqueueReminderJob(chatId: string, doses: any[], dateStr: string, source: "scheduled" | "manual") {
  const slotKey = makeSlotKey(chatId, dateStr, doses);
  const existing = slotStates.get(slotKey);
  if (existing) {
    if (existing.status === "deferred") {
      void debugLog("slot.skip_deferred", {
        slot: getDoseDisplayTime(doses[0]),
        details: { slotKey, deferredUntil: existing.deferredUntil, nextAttemptAt: existing.nextAttemptAt, source }
      });
    }
    return false;
  }
  const job: ReminderJob = { slotKey, chatId, dateStr, doses, source };
  reminderQueue.push(job);
  slotStates.set(slotKey, { status: "queued", job });
  return true;
}

/**
 * Promotes any deferred slot whose nextAttemptAt has passed back into the
 * send queue.  Called at the top of every processReminderQueue() tick so
 * retries happen exactly once, without waiting for the minute-level tick().
 */
function promoteExpiredDeferrals() {
  const now = Date.now();
  for (const [slotKey, state] of slotStates) {
    if (state.status === "deferred" && now >= (state.nextAttemptAt ?? 0)) {
      reminderQueue.push(state.job);
      slotStates.set(slotKey, { status: "queued", job: state.job });
      void debugLog("slot.retry_released", {
        slot: getDoseDisplayTime(state.job.doses[0]),
        details: { slotKey, wasNextAttemptAt: state.nextAttemptAt }
      });
    }
  }
}

async function processReminderQueue() {
  if (processingReminderQueue) return;
  if (reminderQueue.length === 0 && [...slotStates.values()].every(s => s.status !== "deferred")) return;
  processingReminderQueue = true;
  try {
    promoteExpiredDeferrals();

    if (reminderQueue.length === 0) return;

    const nowMs = Date.now();
    if (nowMs < telegramBackoffUntil || nowMs < nextReminderSendAt) {
      return;
    }

    const job = reminderQueue[0];
    const { slotKey, chatId, dateStr, doses, source } = job;

    await debugLog("send.attempt", {
      slot: getDoseDisplayTime(doses[0]),
      details: { slotKey, source, doseIds: doses.map((d: any) => d.id) }
    });

    try {
      const messageId = await sendDoseMessageNow(chatId, doses, dateStr);

      await debugLog("send.ok", {
        slot: getDoseDisplayTime(doses[0]),
        details: { slotKey, message_id: messageId }
      });

      for (const dose of doses) {
        const occId = occurrenceId(dose, dateStr);
        const reminder = await db.get("SELECT * FROM reminders WHERE occurrence_id = ?", occId);
        const sentCount = source === "scheduled" ? (reminder?.sent_count ?? 0) + 1 : (reminder?.sent_count ?? 0);
        const lastSentAt = source === "scheduled" ? dayjs().toISOString() : reminder?.last_sent_at ?? null;

        await updateReminder(occId, {
          dose_id: dose.id,
          date: dateStr,
          sent_count: sentCount,
          last_sent_at: lastSentAt,
          message_id: messageId.toString(),
          chat_id: chatId
        });
      }

      reminderQueue.shift();
      slotStates.delete(slotKey);
      nextReminderSendAt = Date.now() + TELEGRAM_SEND_INTERVAL_MS;
    } catch (err: any) {
      noteTelegramBackoff(err);
      const retryAfter = err?.response?.parameters?.retry_after;
      reminderQueue.shift();

      if (retryAfter) {
        const deferredAt = Date.now();
        const deferredUntil = deferredAt + Number(retryAfter) * 1000;
        slotStates.set(slotKey, {
          status: "deferred",
          job,
          deferredAt,
          deferredUntil,
          nextAttemptAt: deferredUntil
        });
        await debugLog("send.429", {
          slot: getDoseDisplayTime(doses[0]),
          details: { slotKey, retry_after: retryAfter }
        });
        await debugLog("slot.deferred_until", {
          slot: getDoseDisplayTime(doses[0]),
          details: { slotKey, deferredUntil: new Date(deferredUntil).toISOString() }
        });
        await debugLog("slot.retry_scheduled", {
          slot: getDoseDisplayTime(doses[0]),
          details: { slotKey, nextAttemptAt: new Date(deferredUntil).toISOString() }
        });
      } else {
        // Non-429 error: release the slot so tick() can re-evaluate normally
        slotStates.delete(slotKey);
      }

      console.error("Send reminder failed", err);
    }
  } finally {
    processingReminderQueue = false;
  }
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
    return enqueueChatOp(chatId, () => bot.telegram.sendMessage(chatId, "No hay medicinas pendientes en este momento.", MAIN_KEYBOARD));
  }

  const bySlot: Record<string, any[]> = {};
  for (const dose of dueDoses) {
    const slot = getDoseDisplayTime(dose);
    bySlot[slot] = bySlot[slot] || [];
    bySlot[slot].push(dose);
  }

  for (const group of Object.values(bySlot)) {
    const slotKey = makeSlotKey(chatId, dateStr, group);
    const slotState = slotStates.get(slotKey);
    if (slotState) {
      if (slotState.status === "deferred") {
        void debugLog("slot.skip_deferred", {
          slot: getDoseDisplayTime(group[0]),
          details: { slotKey, deferredUntil: slotState.deferredUntil, source: "manual" }
        });
      }
      continue;
    }

    const existingRows = [];
    for (const dose of group) {
      const occId = occurrenceId(dose, dateStr);
      const reminder = await db.get("SELECT * FROM reminders WHERE occurrence_id = ?", occId);
      if (reminder) existingRows.push(reminder);
    }

    const hasActiveMessage = existingRows.some((row: any) => row?.message_id && row?.chat_id === chatId);
    if (hasActiveMessage) continue;

    enqueueReminderJob(chatId, group, dateStr, "manual");
  }

  return null;
}

async function tick() {
  if (isTelegramBackedOff()) return;
  const now = dayjs().tz(tz);
  const dateStr = now.format("YYYY-MM-DD");

  if (dateStr !== queueDay) {
    reminderQueue.length = 0;
    slotStates.clear();
    nextReminderSendAt = 0;
    queueDay = dateStr;
    await debugLog("queue.cleared.day_change", { details: { newDay: dateStr } });
  }

  await maybeClearButtons(now);

  const activeDoses = meds.filter((dose: any) => isDoseActiveToday(dose, now));
  const dueDoses: any[] = [];

  for (const dose of activeDoses) {
    const { start, end, base } = getDoseWindow(dose, now);
    if (now.isBefore(start) || now.isAfter(end)) continue;

    const occId = occurrenceId(dose, dateStr);
    const slot = getDoseDisplayTime(dose);
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

    await debugLog("dose.evaluated", {
      occurrence_id: occId,
      dose_id: dose.id,
      slot,
      details: { sentCount, shouldSendInitial, shouldSendRepeat, base: base.toISOString(), now: now.toISOString() }
    });

    if (shouldSendInitial || shouldSendRepeat) {
      dueDoses.push(dose);
      await debugLog("dose.marked_due", { occurrence_id: occId, dose_id: dose.id, slot });
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

  for (const [slot, group] of Object.entries(byTime)) {
    let hasActiveMessage = false;
    for (const dose of group) {
      const occId = occurrenceId(dose, dateStr);
      const reminder = await db.get("SELECT * FROM reminders WHERE occurrence_id = ?", occId);
      if (
        reminder?.message_id &&
        reminder?.chat_id === targetChatId &&
        ((reminder?.sent_count ?? 0) > 0 || !!reminder?.last_sent_at)
      ) {
        hasActiveMessage = true;
        break;
      }
    }
    if (hasActiveMessage) continue;

    const enqueued = enqueueReminderJob(String(targetChatId), group, dateStr, "scheduled");
    await debugLog("slot.enqueued", { slot, details: { enqueued, targetChatId } });
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

async function getLastEventDate(chatId: string, eventType: "poop" | "bath") {
  const row = await db.get(
    "SELECT event_date FROM tracker_events WHERE chat_id = ? AND event_type = ? ORDER BY id DESC LIMIT 1",
    chatId,
    eventType
  );
  return row?.event_date ?? null;
}

async function addTrackerEvent(opts: { chatId: string; eventType: "poop" | "bath"; userId?: number; userName?: string }) {
  const eventDate = dayjs().tz(tz).format("YYYY-MM-DD");
  await db.run(
    `INSERT INTO tracker_events (event_type, event_date, chat_id, created_at, user_id, user_name)
     VALUES (?, ?, ?, ?, ?, ?)`,
    opts.eventType,
    eventDate,
    opts.chatId,
    dayjs().toISOString(),
    opts.userId?.toString() ?? null,
    opts.userName ?? null
  );
}

async function undoTrackerEvent(chatId: string, eventType: "poop" | "bath") {
  const row = await db.get(
    "SELECT id FROM tracker_events WHERE chat_id = ? AND event_type = ? ORDER BY id DESC LIMIT 1",
    chatId,
    eventType
  );
  if (!row?.id) return false;
  await db.run("DELETE FROM tracker_events WHERE id = ?", row.id);
  return true;
}

async function buildStatusText(now: dayjs.Dayjs, chatId?: string) {
  const dateStr = now.format("YYYY-MM-DD");
  const activeDoses = meds
    .filter((dose: any) => isDoseActiveToday(dose, now))
    .slice()
    .sort((a: any, b: any) => getDoseSortKey(a).localeCompare(getDoseSortKey(b)) || a.label.localeCompare(b.label));

  const lines: string[] = [];

  if (chatId) {
    const [lastPoop, lastBath] = await Promise.all([
      getLastEventDate(chatId, "poop"),
      getLastEventDate(chatId, "bath")
    ]);
    lines.push(`💩 Última caca Mario: ${lastPoop ?? "—"}`);
    lines.push(`🛁 Último baño Mario: ${lastBath ?? "—"}`);
    lines.push("");
  }

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
  const chatId = ctx.chat?.id?.toString();
  const text = await buildStatusText(now, chatId);
  await safeReply(ctx, text, MAIN_KEYBOARD);
}

async function handleDue(ctx: any) {
  const now = dayjs().tz(tz);
  const chatId = ctx.chat?.id?.toString();
  if (!chatId) return safeReply(ctx, "No pude detectar este chat.", MAIN_KEYBOARD);
  return sendDueButtonsToChat(chatId, now);
}

async function handleQueueStatus(ctx: any) {
  if (!isAdmin(ctx.from?.id)) return safeReply(ctx, "Not allowed", MAIN_KEYBOARD);

  const now = Date.now();
  const nextMs = Math.max(0, nextReminderSendAt - now);
  const backoffMs = Math.max(0, telegramBackoffUntil - now);

  const queuedSlots = [...slotStates.values()].filter(s => s.status === "queued");
  const deferredSlots = [...slotStates.values()].filter(s => s.status === "deferred");

  const deferredLines = deferredSlots.map(s => {
    const retryInMs = Math.max(0, (s.nextAttemptAt ?? 0) - now);
    const displaySlot = getDoseDisplayTime(s.job.doses[0]);
    return `  • ${displaySlot} (${s.job.dateStr}) → retry in ${Math.ceil(retryInMs / 1000)}s`;
  });

  const oldestDeferredAgeMs = deferredSlots.reduce<number>((acc, s) => {
    if (s.deferredAt === undefined) return acc;
    return Math.max(acc, now - s.deferredAt);
  }, 0);

  const lines = [
    `Queue size: ${reminderQueue.length}`,
    `Queued: ${queuedSlots.length}`,
    `Deferred: ${deferredSlots.length}`,
    ...(deferredLines.length ? deferredLines : []),
    ...(deferredSlots.length ? [`Oldest deferred: ${Math.ceil(oldestDeferredAgeMs / 1000)}s ago`] : []),
    `Next send in: ${Math.ceil(nextMs / 1000)}s`,
    `Global backoff: ${Math.ceil(backoffMs / 1000)}s`,
    `Queue day: ${queueDay}`
  ].join("\n");

  return safeReply(ctx, lines, MAIN_KEYBOARD);
}

async function handlePoop(ctx: any) {
  const chatId = ctx.chat?.id?.toString();
  if (!chatId) return safeReply(ctx, "No pude detectar este chat.", MAIN_KEYBOARD);
  await addTrackerEvent({
    chatId,
    eventType: "poop",
    userId: ctx.from?.id,
    userName: ctx.from?.username ?? ctx.from?.first_name
  });
  return safeReply(ctx, "💩 Caca de Mario registrada.", MAIN_KEYBOARD);
}

async function handleUndoPoop(ctx: any) {
  const chatId = ctx.chat?.id?.toString();
  if (!chatId) return safeReply(ctx, "No pude detectar este chat.", MAIN_KEYBOARD);
  const ok = await undoTrackerEvent(chatId, "poop");
  return safeReply(ctx, ok ? "↩️ Última caca de Mario eliminada." : "No hay cacas para deshacer.", MAIN_KEYBOARD);
}

async function handleBath(ctx: any) {
  const chatId = ctx.chat?.id?.toString();
  if (!chatId) return safeReply(ctx, "No pude detectar este chat.", MAIN_KEYBOARD);
  await addTrackerEvent({
    chatId,
    eventType: "bath",
    userId: ctx.from?.id,
    userName: ctx.from?.username ?? ctx.from?.first_name
  });
  return safeReply(ctx, "🛁 Baño de Mario registrado.", MAIN_KEYBOARD);
}

async function handleUndoBath(ctx: any) {
  const chatId = ctx.chat?.id?.toString();
  if (!chatId) return safeReply(ctx, "No pude detectar este chat.", MAIN_KEYBOARD);
  const ok = await undoTrackerEvent(chatId, "bath");
  return safeReply(ctx, ok ? "↩️ Último baño de Mario eliminado." : "No hay baños para deshacer.", MAIN_KEYBOARD);
}

async function handleFood(ctx: any) {
  const now = dayjs().tz(tz);
  const dateStr = now.format("YYYY-MM-DD");
  const chatId = ctx.chat?.id?.toString();
  if (!chatId) return safeReply(ctx, "No pude detectar este chat.", MAIN_KEYBOARD);

  const existing = await db.get(
    "SELECT pienso_grams, lata_halves, colin_sobre_halves, colin_churu_quarters FROM food_log WHERE date = ? AND chat_id = ?",
    dateStr,
    chatId
  );

  const state = getOrCreateFoodState(chatId, dateStr);
  if (existing) {
    state.piensoGrams = existing.pienso_grams ?? 0;
    state.lataHalves = existing.lata_halves ?? 0;
    state.colinSobreHalves = existing.colin_sobre_halves ?? 0;
    state.colinChuruQuarters = existing.colin_churu_quarters ?? 0;
    state.history = [];
  }

  return enqueueChatOp(chatId, () => bot.telegram.sendMessage(
    chatId,
    foodText(dateStr, state),
    foodKeyboard(chatId, dateStr, state)
  ));
}

bot.hears(/^\/status(?:@\w+)?$/i, handleStatus);
bot.hears(/^\/due(?:@\w+)?$/i, handleDue);
bot.hears(/^\/queue(?:@\w+)?$/i, handleQueueStatus);
bot.hears(/^\/food(?:@\w+)?$/i, handleFood);
bot.hears(/^\/poop(?:@\w+)?$/i, handlePoop);
bot.hears(/^\/undopoop(?:@\w+)?$/i, handleUndoPoop);
bot.hears(/^\/bath(?:@\w+)?$/i, handleBath);
bot.hears(/^\/undobath(?:@\w+)?$/i, handleUndoBath);
bot.hears(/^status$/i, handleStatus);
bot.hears(/^pendientes$/i, handleDue);
bot.hears(/^queue$/i, handleQueueStatus);
bot.hears(/^food$/i, handleFood);

async function answerCbQuick(ctx: any, text?: string) {
  try {
    await ctx.answerCbQuery(text);
  } catch {
    // ignore expired callback errors
  }
}

bot.action(/give:(.+):(.+)/, async (ctx) => {
  const doseId = ctx.match[1];
  const dateStr = ctx.match[2];
  const dose = meds.find((m: any) => m.id === doseId);
  if (!dose) return answerCbQuick(ctx, "Unknown dose");

  const occId = occurrenceId(dose, dateStr);
  if (await doseGiven(occId)) {
    return answerCbQuick(ctx, "Already marked");
  }

  await answerCbQuick(ctx, "Marked ✅");

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

    const chatKey = ctx.chat?.id?.toString?.() ?? "callback";
    await enqueueChatOp(chatKey, () => ctx.editMessageReplyMarkup(filtered.length > 0 ? { inline_keyboard: filtered } : undefined));
  } catch (err) {
    console.error("Failed to update reminder buttons", err);
  }
});

bot.action(/food:add:([^:]+:[^:]+):(-?\d+):(\d+):(\d+):(\d+)/, async (ctx) => {
  const key = ctx.match[1];
  const piensoDelta = Number(ctx.match[2]);
  const lataDelta = Number(ctx.match[3]);
  const colinSobreDelta = Number(ctx.match[4]);
  const colinChuruDelta = Number(ctx.match[5]);
  const [chatId, dateStr] = key.split(":");
  const state = getOrCreateFoodState(chatId, dateStr);

  state.history.push({ piensoDelta, lataDelta, colinSobreDelta, colinChuruDelta });
  state.piensoGrams = Math.max(0, state.piensoGrams + piensoDelta);
  state.lataHalves = Math.max(0, Math.min(2, state.lataHalves + lataDelta));
  state.colinSobreHalves = Math.max(0, Math.min(2, state.colinSobreHalves + colinSobreDelta));
  state.colinChuruQuarters = Math.max(0, Math.min(4, state.colinChuruQuarters + colinChuruDelta));

  try {
    const chatKey = ctx.chat?.id?.toString?.() ?? "food";
    await enqueueChatOp(chatKey, () => ctx.editMessageText(
      foodText(dateStr, state),
      foodKeyboard(chatId, dateStr, state)
    ));
    await answerCbQuick(ctx, "Updated");
  } catch (err) {
    console.error("Failed to update food message", err);
  }
});

bot.action(/food:undo:([^:]+:[^:]+)/, async (ctx) => {
  const key = ctx.match[1];
  const [chatId, dateStr] = key.split(":");
  const state = getOrCreateFoodState(chatId, dateStr);
  const last = state.history.pop();
  if (last) {
    state.piensoGrams = Math.max(0, state.piensoGrams - last.piensoDelta);
    state.lataHalves = Math.max(0, Math.min(2, state.lataHalves - last.lataDelta));
    state.colinSobreHalves = Math.max(0, Math.min(2, state.colinSobreHalves - last.colinSobreDelta));
    state.colinChuruQuarters = Math.max(0, Math.min(4, state.colinChuruQuarters - last.colinChuruDelta));
  }

  try {
    const chatKey = ctx.chat?.id?.toString?.() ?? "food";
    await enqueueChatOp(chatKey, () => ctx.editMessageText(
      foodText(dateStr, state),
      foodKeyboard(chatId, dateStr, state)
    ));
    await answerCbQuick(ctx, last ? "Undone" : "Nothing to undo");
  } catch (err) {
    console.error("Failed to undo food update", err);
  }
});

bot.action(/food:reset:([^:]+:[^:]+)/, async (ctx) => {
  const key = ctx.match[1];
  const [chatId, dateStr] = key.split(":");
  const state = getOrCreateFoodState(chatId, dateStr);
  state.piensoGrams = 0;
  state.lataHalves = 0;
  state.colinSobreHalves = 0;
  state.colinChuruQuarters = 0;
  state.history = [];

  try {
    const chatKey = ctx.chat?.id?.toString?.() ?? "food";
    await enqueueChatOp(chatKey, () => ctx.editMessageText(
      foodText(dateStr, state),
      foodKeyboard(chatId, dateStr, state)
    ));
    await answerCbQuick(ctx, "Reset");
  } catch (err) {
    console.error("Failed to reset food", err);
  }
});

bot.action(/food:done:([^:]+:[^:]+)/, async (ctx) => {
  const key = ctx.match[1];
  const [chatId, dateStr] = key.split(":");
  const state = getOrCreateFoodState(chatId, dateStr);

  try {
    await upsertFoodLog({
      dateStr,
      chatId,
      piensoGrams: state.piensoGrams,
      lataHalves: state.lataHalves,
      colinSobreHalves: state.colinSobreHalves,
      colinChuruQuarters: state.colinChuruQuarters,
      userId: ctx.from?.id,
      userName: ctx.from?.username ?? ctx.from?.first_name
    });

    const chatKey = ctx.chat?.id?.toString?.() ?? "food";
    await enqueueChatOp(chatKey, () => ctx.editMessageText(
      `${foodText(dateStr, state)}\n✅ Guardado`,
      Markup.inlineKeyboard([])
    ));
    await answerCbQuick(ctx, "Saved ✅");
  } catch (err) {
    console.error("Failed to save food", err);
    try {
      await answerCbQuick(ctx, "Save failed");
    } catch {}
  }
});

const BOT_COMMANDS = [
  { command: "status", description: "Ver estado de medicación" },
  { command: "due", description: "Mostrar medicinas pendientes con botones" },
  { command: "food", description: "Registrar comida" },
  { command: "poop", description: "Registrar caca de Mario" },
  { command: "undopoop", description: "Deshacer última caca de Mario" },
  { command: "bath", description: "Registrar baño de Mario" },
  { command: "undobath", description: "Deshacer último baño de Mario" },
  { command: "queue", description: "Ver cola y rate limit (admin)" },
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

setInterval(() => {
  processReminderQueue().catch((err) => console.error("queue error", err));
}, 1000);

// Run immediately on startup
void tick();
void processReminderQueue();

process.once("SIGINT", () => bot.stop("SIGINT"));
process.once("SIGTERM", () => bot.stop("SIGTERM"));

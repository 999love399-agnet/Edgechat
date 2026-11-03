import {
  createTextSessionReport,
  getTextSessionReport,
  listRetryableReports,
  listStalePendingDmSessions,
  markTextSessionRetry,
  readMemoryToken,
  updateTextSessionReportResult
} from "./accounts.js";
import { reportSessionEnded } from "./pi-client.js";
import { postSpeakIngest } from "./ingest-client.js";
import { decryptMessageContent } from "../../encryption.js";

/**
 * TASK-133：文本会话结算（本单最大坑的落点）。
 *
 * 判据（变更单 §三）：
 * - 主判据：客户端切后台时上报 —— POST /api/v1/memory/text-session/ended → 本函数转调 /pi/api/session/ended
 * - 兜底：cron 扫"未结算且最后消息 > 30 分钟前"的 DM 会话补报
 * - 状态机：pending → reported（pi 2xx）/ failed（非 2xx 或网络失败）；failed 由 cron 限次重试，
 *   防止"pi 一次失败导致该段摘要永久静默丢失"
 * - session_id 语义（定死）：`chat_dm_<roomId>_<lastMessageId>`，与变更单 §3.1 保持一致
 */

const MAX_ATTEMPTS = 3;
const RETRY_AGE_MINUTES = 5;
/** 单段结算最多转发多少条说话消息（防御极端长会话拖垮请求；超出部分下段兜底）。 */
const MAX_FORWARD_MESSAGES = 50;

/** session_id 语义唯一实现：roomId + 该段最后一条消息 id。 */
export function sessionIdFor(roomId, lastMessageId) {
  return `chat_dm_${Number(roomId)}_${Number(lastMessageId)}`;
}

/** D1 的 CURRENT_TIMESTAMP 是 UTC 的 "YYYY-MM-DD HH:MM:SS"，转毫秒；解析不出返回 0。 */
function dbTimestampMs(value) {
  const text = String(value || "").trim();
  if (!text) return 0;
  const normalized = text.includes("T") ? text : `${text.replace(" ", "T")}Z`;
  const ms = Date.parse(normalized);
  return Number.isFinite(ms) ? ms : 0;
}

/**
 * 对话说话直写 SAG（帮帮 reportSpeak 的云端移植，02:11 授权"移植帮帮有而 chat 版没有的能力"）。
 * 在结算摘要之前，把本段新消息（上一结算点之后 → lastMessageId）逐条转发给 SAG /ingest：
 * - 帮帮版由 vFlow 壳端上直报（[vFlow] 用户说话/AI说话）；chat 版消息本体就在云端 D1 里，
 *   worker 直读解密后代报，端上零改动（不新装上报器、不耗手机流量）。
 * - 格式与帮帮同构：{"类型":"语音对话","说话人":"用户/AI","内容":"…"}，metadata.type=user_speak/ai_speak，
 *   extract 不带 false → SAG 走完整抽取，进 V2 五维事实管线（说话才是记忆的主料）。
 * - 幂等：client_msg_id=chat_msg_<messageId>（SAG 幂等闸按它去重，重结算/重试不重复落库）。
 * - 失败只记日志，不抛异常、不阻塞摘要主链路（pi 摘要兜底仍然按时跑；失败段下轮 cron 重试时
 *   上一次已成功的消息靠幂等键跳过）。
 */
async function forwardSessionMessages(env, { roomId, userId, lastMessageId, fetchImpl = fetch }) {
  try {
    const prev = await env.DB.prepare(
      `SELECT MAX(r.last_message_id) AS prev
       FROM text_session_reports r
       WHERE r.room_id = ? AND r.last_message_id < ? AND r.user_id = ? AND r.status = 'reported'`
    )
      .bind(Number(roomId), Number(lastMessageId), Number(userId))
      .first();
    const afterId = Number(prev?.prev || 0);
    const rows = await env.DB.prepare(
      `SELECT m.id, m.content, m.sender_id, m.sender_kind, m.source, m.external_sender_id,
              m.created_at, u.username AS sender_username
       FROM messages m LEFT JOIN users u ON u.id = m.sender_id
       WHERE m.channel_id = ? AND m.id > ? AND m.id <= ? AND m.deleted_at IS NULL
         AND m.sender_kind = 'local' AND m.content IS NOT NULL AND m.content != ''
       ORDER BY m.id ASC LIMIT ?`
    )
      .bind(Number(roomId), afterId, Number(lastMessageId), MAX_FORWARD_MESSAGES)
      .all();
    const list = rows.results || [];
    const bot = await env.DB.prepare(
      `SELECT id FROM users WHERE username = 'benben' AND deleted_at IS NULL LIMIT 1`
    ).first();
    const botId = Number(bot?.id || 0);
    if (!botId) return { forwarded: 0, skipped: "no_bot" };

    const account = await readMemoryToken(env, Number(userId));
    if (!account?.token) return { forwarded: 0, skipped: "no_account" };

    let forwarded = 0;
    let failed = 0;
    for (const row of list) {
      const senderId = Number(row.sender_id || 0);
      // 只转发「用户 ↔ 笨笨」两个声道；桥接/系统消息不进记忆
      if (senderId !== botId && senderId !== Number(userId)) continue;
      let text = "";
      try {
        text = await decryptMessageContent(env, row.content, {
          channelId: Number(roomId),
          senderId,
          senderContext: ""
        });
      } catch {
        continue; // 解密失败的消息（含音频附件占位）跳过，不阻塞其余转发
      }
      if (!String(text || "").trim()) continue;
      const role = senderId === botId ? "AI" : "用户";
      const createdMs = dbTimestampMs(row.created_at) || Date.now();
      const r = await postSpeakIngest(env, account.token, {
        role,
        text,
        speakTimeMs: createdMs,
        clientMsgId: `chat_msg_${Number(row.id)}`,
        ownerUserId: account.customerId || "",
        fetchImpl
      });
      if (r.ok) forwarded += 1;
      else failed += 1;
      if (!r.ok && r.status) {
        console.log(
          JSON.stringify({
            message: "text_session speak forward item failed",
            roomId: Number(roomId),
            msgId: Number(row.id),
            status: r.status
          })
        );
      }
    }
    if (list.length) {
      console.log(
        JSON.stringify({
          message: "text_session speak forwarded",
          roomId: Number(roomId),
          afterId,
          lastMsg: Number(lastMessageId),
          forwarded,
          failed
        })
      );
    }
    return { forwarded, failed };
  } catch (error) {
    console.log(
      JSON.stringify({
        message: "text_session speak forward error",
        roomId: Number(roomId),
        lastMsg: Number(lastMessageId),
        error: String(error?.message || error || "").slice(0, 120)
      })
    );
    return { forwarded: 0, failed: 0, error: true };
  }
}

async function attemptReport(
  env,
  { roomId, lastMessageId, userId, sessionId, startedAt, turnCount = 0, lastUserText = "", fetchImpl = fetch }
) {
  const account = await readMemoryToken(env, userId);
  if (!account) {
    console.log(
      JSON.stringify({ message: "text_session skipped no_account", roomId, lastMsg: lastMessageId, userId })
    );
    return { status: "no_account", piStatus: null };
  }
  const endedAt = Date.now();
  // 先把本段新消息直写 SAG（帮帮口径：说话内容进 V2 抽取管线），再报摘要
  await forwardSessionMessages(env, { roomId, userId, lastMessageId, fetchImpl });
  const piStatus = await reportSessionEnded(
    env,
    account.token,
    { sessionId, startedAt: Number(startedAt) || endedAt, endedAt, turnCount, lastUserText },
    { fetchImpl }
  );
  const ok = piStatus >= 200 && piStatus < 300;
  await updateTextSessionReportResult(env.DB, { roomId, lastMessageId, piStatus, ok });
  console.log(
    JSON.stringify({
      message: ok ? "text_session ended" : "text_session ended failed",
      roomId,
      lastMsg: lastMessageId,
      piStatus
    })
  );
  return { status: ok ? "reported" : "pi_failed", piStatus };
}

/**
 * 结算一个文本会话段。
 * 返回 { status: 'reported'|'duplicate'|'no_account'|'pi_failed'|'retry_exhausted'|'invalid', piStatus }
 */
export async function settleTextSession(
  env,
  { roomId, userId, lastMessageId, startedAt, turnCount = 0, lastUserText = "", fetchImpl = fetch } = {}
) {
  const room = Number(roomId);
  const messageId = Number(lastMessageId);
  const uid = Number(userId);
  if (!Number.isInteger(room) || room <= 0 || !Number.isInteger(messageId) || messageId <= 0 || !Number.isInteger(uid) || uid <= 0) {
    return { status: "invalid", piStatus: null };
  }

  const existing = await getTextSessionReport(env.DB, room, messageId);
  if (existing) {
    // 已成功 → 幂等返回；失败/进行中 → 限次重试（cron 另有兜底拾取）
    if (existing.status === "reported") {
      console.log(
        JSON.stringify({ message: "text_session duplicate skipped", roomId: room, lastMsg: messageId })
      );
      return { status: "duplicate", piStatus: existing.pi_status ?? null };
    }
    if (Number(existing.attempts) >= MAX_ATTEMPTS) {
      console.log(
        JSON.stringify({ message: "text_session retry exhausted", roomId: room, lastMsg: messageId })
      );
      return { status: "retry_exhausted", piStatus: existing.pi_status ?? null };
    }
    await markTextSessionRetry(env.DB, { roomId: room, lastMessageId: messageId });
    return attemptReport(env, {
      roomId: room,
      lastMessageId: messageId,
      userId: uid,
      sessionId: existing.session_id || sessionIdFor(room, messageId),
      startedAt,
      turnCount,
      lastUserText,
      fetchImpl
    });
  }

  const account = await readMemoryToken(env, uid);
  if (!account) {
    console.log(JSON.stringify({ message: "text_session skipped no_account", roomId: room, userId: uid }));
    return { status: "no_account", piStatus: null };
  }

  const sessionId = sessionIdFor(room, messageId);
  const created = await createTextSessionReport(env.DB, {
    roomId: room,
    lastMessageId: messageId,
    userId: uid,
    sessionId
  });
  if (!created) {
    console.log(JSON.stringify({ message: "text_session duplicate skipped", roomId: room, lastMsg: messageId }));
    return { status: "duplicate", piStatus: null };
  }
  return attemptReport(env, {
    roomId: room,
    lastMessageId: messageId,
    userId: uid,
    sessionId,
    startedAt,
    turnCount,
    lastUserText,
    fetchImpl
  });
}

/**
 * cron 拾取失败记录重试（防"pi 非 2xx 导致该段摘要永久静默丢失"）。
 * 返回 { retried, recovered }。
 */
export async function retryFailedReports(env, { fetchImpl = fetch, limit = 20 } = {}) {
  const rows = await listRetryableReports(env.DB, {
    maxAttempts: MAX_ATTEMPTS,
    minAgeMinutes: RETRY_AGE_MINUTES,
    limit
  });
  let retried = 0;
  let recovered = 0;
  for (const row of rows) {
    const roomId = Number(row.room_id);
    const lastMessageId = Number(row.last_message_id);
    const userId = Number(row.user_id);
    await markTextSessionRetry(env.DB, { roomId, lastMessageId });
    const result = await attemptReport(env, {
      roomId,
      lastMessageId,
      userId,
      sessionId: row.session_id || sessionIdFor(roomId, lastMessageId),
      startedAt: Date.now(),
      turnCount: 0,
      lastUserText: "",
      fetchImpl
    });
    retried += 1;
    if (result.status === "reported") recovered += 1;
  }
  return { retried, recovered };
}

/**
 * cron 兜底：①扫描并补报"静默超过阈值的未结算 DM 会话"；②重试失败记录。
 */
export async function backfillStaleSessions(
  env,
  { botId, minSilentMinutes = 30, limit = 20, fetchImpl = fetch } = {}
) {
  const bot = Number(botId);
  if (!Number.isInteger(bot) || bot <= 0) return { scanned: 0, settled: 0, skipped: 0, retried: 0, recovered: 0 };

  const rows = await listStalePendingDmSessions(env.DB, { botId: bot, minSilentMinutes, limit });
  let settled = 0;
  let skipped = 0;
  for (const row of rows) {
    const roomId = Number(row.room_id);
    const lastMessageId = Number(row.last_message_id);
    const { results } = await env.DB
      .prepare(`SELECT user_id FROM channel_members WHERE channel_id = ? AND user_id != ? LIMIT 1`)
      .bind(roomId, bot)
      .all();
    const userId = Number(results?.[0]?.user_id || 0);
    if (!userId) {
      skipped += 1;
      continue;
    }
    const result = await settleTextSession(env, {
      roomId,
      userId,
      lastMessageId,
      startedAt: Date.parse(row.last_message_at || "") || Date.now(),
      turnCount: Number(row.message_count || 0),
      lastUserText: "",
      fetchImpl
    });
    if (result.status === "reported") settled += 1;
    else skipped += 1;
  }

  const retry = await retryFailedReports(env, { fetchImpl, limit });
  return {
    scanned: rows.length,
    settled,
    skipped,
    retried: retry.retried,
    recovered: retry.recovered
  };
}

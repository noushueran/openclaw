import { DatabaseSync } from "node:sqlite";
import fs from "node:fs";
import path from "node:path";
import type { proto, WAMessage } from "@whiskeysockets/baileys";
import { resolveStateDir } from "../config/paths.js";
import { resolveUserPath } from "../utils.js";
import { createSubsystemLogger } from "../logging/subsystem.js";

const log = createSubsystemLogger("whatsapp/history");

export type StoredMessage = {
  message_id: string;
  conversation_jid: string;
  sender_jid: string | null;
  sender_e164: string | null;
  sender_name: string | null;
  body: string;
  timestamp: number;
  is_from_me: boolean;
  reply_to_id: string | null;
  media_path: string | null;
  media_type: string | null;
  location_lat: number | null;
  location_lon: number | null;
  raw_message: string;
  account_id: string;
  created_at: number;
};

export type ConversationMetadata = {
  jid: string;
  chat_type: "direct" | "group";
  display_name: string | null;
  last_message_at: number;
  created_at: number;
};

export type ExportFilter = {
  from?: string; // ISO 8601 date
  to?: string; // ISO 8601 date
  conversationJid?: string;
  accountId?: string;
  limit?: number;
};

export type ExportedConversation = {
  jid: string;
  chatType: "direct" | "group";
  displayName: string | null;
  messages: Array<{
    messageId: string;
    senderJid: string | null;
    senderE164: string | null;
    senderName: string | null;
    body: string;
    timestamp: number;
    isFromMe: boolean;
    replyToId: string | null;
    mediaPath: string | null;
    mediaType: string | null;
    location: { lat: number; lon: number } | null;
  }>;
};

export class WhatsAppHistoryStore {
  private db: DatabaseSync | null = null;
  private dbPath: string;

  constructor(dbPath?: string) {
    const defaultPath = path.join(resolveStateDir(), "whatsapp-history.sqlite");
    this.dbPath = resolveUserPath(dbPath ?? defaultPath);
  }

  /**
   * Initialize the database and create tables if they don't exist.
   */
  initialize(): void {
    if (this.db) {
      return;
    }

    const dir = path.dirname(this.dbPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    this.db = new DatabaseSync(this.dbPath);

    // Create schema
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS conversations (
        jid TEXT PRIMARY KEY,
        chat_type TEXT NOT NULL,
        display_name TEXT,
        last_message_at INTEGER NOT NULL,
        created_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT NOT NULL,
        conversation_jid TEXT NOT NULL,
        sender_jid TEXT,
        sender_e164 TEXT,
        sender_name TEXT,
        body TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        is_from_me INTEGER NOT NULL,
        reply_to_id TEXT,
        media_path TEXT,
        media_type TEXT,
        location_lat REAL,
        location_lon REAL,
        raw_message TEXT NOT NULL,
        account_id TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (conversation_jid, message_id)
      );

      CREATE TABLE IF NOT EXISTS participants (
        conversation_jid TEXT NOT NULL,
        participant_jid TEXT NOT NULL,
        participant_e164 TEXT,
        joined_at INTEGER NOT NULL,
        PRIMARY KEY (conversation_jid, participant_jid)
      );

      CREATE INDEX IF NOT EXISTS idx_messages_conversation_timestamp 
        ON messages(conversation_jid, timestamp);
      CREATE INDEX IF NOT EXISTS idx_messages_timestamp 
        ON messages(timestamp);
      CREATE INDEX IF NOT EXISTS idx_messages_sender_e164 
        ON messages(sender_e164);
      CREATE INDEX IF NOT EXISTS idx_messages_account_id 
        ON messages(account_id);
    `);

    log.info(`WhatsApp history database initialized at ${this.dbPath}`);
  }

  /**
   * Close the database connection.
   */
  close(): void {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }

  /**
   * Store a message in the database.
   */
  storeMessage(params: {
    messageId: string;
    conversationJid: string;
    chatType: "direct" | "group";
    senderJid: string | null;
    senderE164: string | null;
    senderName: string | null;
    body: string;
    timestamp: number;
    isFromMe: boolean;
    replyToId?: string;
    mediaPath?: string;
    mediaType?: string;
    location?: { lat: number; lon: number };
    rawMessage: WAMessage;
    accountId: string;
    displayName?: string;
    groupParticipants?: string[];
  }): void {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    const now = Date.now();

    try {
      // Upsert conversation metadata
      const upsertConv = this.db.prepare(`
        INSERT INTO conversations (jid, chat_type, display_name, last_message_at, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(jid) DO UPDATE SET
          display_name = COALESCE(excluded.display_name, display_name),
          last_message_at = excluded.last_message_at
      `);
      upsertConv.run(
        params.conversationJid,
        params.chatType,
        params.displayName ?? null,
        params.timestamp,
        now,
      );

      // Insert message
      const insertMsg = this.db.prepare(`
        INSERT OR REPLACE INTO messages (
          message_id, conversation_jid, sender_jid, sender_e164, sender_name,
          body, timestamp, is_from_me, reply_to_id, media_path, media_type,
          location_lat, location_lon, raw_message, account_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      insertMsg.run(
        params.messageId,
        params.conversationJid,
        params.senderJid,
        params.senderE164,
        params.senderName,
        params.body,
        params.timestamp,
        params.isFromMe ? 1 : 0,
        params.replyToId ?? null,
        params.mediaPath ?? null,
        params.mediaType ?? null,
        params.location?.lat ?? null,
        params.location?.lon ?? null,
        JSON.stringify(params.rawMessage),
        params.accountId,
        now,
      );

      // Store group participants if provided
      if (params.chatType === "group" && params.groupParticipants) {
        const insertParticipant = this.db.prepare(`
          INSERT OR IGNORE INTO participants (conversation_jid, participant_jid, participant_e164, joined_at)
          VALUES (?, ?, ?, ?)
        `);
        for (const participantJid of params.groupParticipants) {
          insertParticipant.run(params.conversationJid, participantJid, null, now);
        }
      }
    } catch (err) {
      log.error(`Failed to store message: ${err instanceof Error ? err.message : String(err)}`);
      throw err;
    }
  }

  /**
   * Query messages with optional filters.
   */
  queryMessages(filter: ExportFilter = {}): StoredMessage[] {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    const conditions: string[] = [];
    const params: unknown[] = [];

    if (filter.from) {
      const fromMs = new Date(filter.from).getTime();
      conditions.push("timestamp >= ?");
      params.push(fromMs);
    }

    if (filter.to) {
      const toMs = new Date(filter.to).getTime();
      conditions.push("timestamp <= ?");
      params.push(toMs);
    }

    if (filter.conversationJid) {
      conditions.push("conversation_jid = ?");
      params.push(filter.conversationJid);
    }

    if (filter.accountId) {
      conditions.push("account_id = ?");
      params.push(filter.accountId);
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const limitClause = filter.limit ? `LIMIT ${filter.limit}` : "";

    const query = `
      SELECT * FROM messages
      ${whereClause}
      ORDER BY timestamp ASC
      ${limitClause}
    `;

    const stmt = this.db.prepare(query);
    return stmt.all(...params) as StoredMessage[];
  }

  /**
   * Export messages grouped by conversation.
   */
  exportMessages(filter: ExportFilter = {}): ExportedConversation[] {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    const messages = this.queryMessages(filter);
    const conversationMap = new Map<string, ExportedConversation>();

    for (const msg of messages) {
      let conv = conversationMap.get(msg.conversation_jid);
      if (!conv) {
        // Fetch conversation metadata
        const convStmt = this.db.prepare(
          "SELECT chat_type, display_name FROM conversations WHERE jid = ?",
        );
        const convData = convStmt.get(msg.conversation_jid) as
          | { chat_type: string; display_name: string | null }
          | undefined;

        conv = {
          jid: msg.conversation_jid,
          chatType: (convData?.chat_type as "direct" | "group") ?? "direct",
          displayName: convData?.display_name ?? null,
          messages: [],
        };
        conversationMap.set(msg.conversation_jid, conv);
      }

      conv.messages.push({
        messageId: msg.message_id,
        senderJid: msg.sender_jid,
        senderE164: msg.sender_e164,
        senderName: msg.sender_name,
        body: msg.body,
        timestamp: msg.timestamp,
        isFromMe: Boolean(msg.is_from_me),
        replyToId: msg.reply_to_id,
        mediaPath: msg.media_path,
        mediaType: msg.media_type,
        location:
          msg.location_lat !== null && msg.location_lon !== null
            ? { lat: msg.location_lat, lon: msg.location_lon }
            : null,
      });
    }

    return Array.from(conversationMap.values());
  }

  /**
   * Get database statistics.
   */
  getStats(): {
    totalMessages: number;
    totalConversations: number;
    oldestMessage: number | null;
    newestMessage: number | null;
  } {
    if (!this.db) {
      throw new Error("Database not initialized");
    }

    const msgCount = this.db.prepare("SELECT COUNT(*) as count FROM messages").get() as {
      count: number;
    };
    const convCount = this.db.prepare("SELECT COUNT(*) as count FROM conversations").get() as {
      count: number;
    };
    const oldest = this.db.prepare("SELECT MIN(timestamp) as ts FROM messages").get() as {
      ts: number | null;
    };
    const newest = this.db.prepare("SELECT MAX(timestamp) as ts FROM messages").get() as {
      ts: number | null;
    };

    return {
      totalMessages: msgCount.count,
      totalConversations: convCount.count,
      oldestMessage: oldest.ts,
      newestMessage: newest.ts,
    };
  }
}

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WhatsAppHistoryStore } from "./history-store.js";

describe("WhatsAppHistoryStore", () => {
  let tempDir: string;
  let dbPath: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "openclaw-history-test-"));
    dbPath = join(tempDir, "test-history.sqlite");
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("initializes database and creates tables", () => {
    const store = new WhatsAppHistoryStore(dbPath);
    store.initialize();

    const stats = store.getStats();
    expect(stats.totalMessages).toBe(0);
    expect(stats.totalConversations).toBe(0);
    expect(stats.oldestMessage).toBeNull();
    expect(stats.newestMessage).toBeNull();

    store.close();
  });

  it("stores and retrieves messages", () => {
    const store = new WhatsAppHistoryStore(dbPath);
    store.initialize();

    const testMessage = {
      messageId: "msg-123",
      conversationJid: "1234567890@s.whatsapp.net",
      chatType: "direct" as const,
      senderJid: "1234567890@s.whatsapp.net",
      senderE164: "+1234567890",
      senderName: "Test User",
      body: "Hello, world!",
      timestamp: Date.now(),
      isFromMe: false,
      rawMessage: { key: { id: "msg-123" } } as any,
      accountId: "default",
    };

    store.storeMessage(testMessage);

    const messages = store.queryMessages();
    expect(messages).toHaveLength(1);
    expect(messages[0].message_id).toBe("msg-123");
    expect(messages[0].body).toBe("Hello, world!");
    expect(messages[0].sender_e164).toBe("+1234567890");

    const stats = store.getStats();
    expect(stats.totalMessages).toBe(1);
    expect(stats.totalConversations).toBe(1);

    store.close();
  });

  it("filters messages by date range", () => {
    const store = new WhatsAppHistoryStore(dbPath);
    store.initialize();

    const now = Date.now();
    const yesterday = now - 24 * 60 * 60 * 1000;
    const tomorrow = now + 24 * 60 * 60 * 1000;

    store.storeMessage({
      messageId: "msg-1",
      conversationJid: "1234567890@s.whatsapp.net",
      chatType: "direct" as const,
      senderJid: "1234567890@s.whatsapp.net",
      senderE164: "+1234567890",
      senderName: "Test User",
      body: "Yesterday",
      timestamp: yesterday,
      isFromMe: false,
      rawMessage: { key: { id: "msg-1" } } as any,
      accountId: "default",
    });

    store.storeMessage({
      messageId: "msg-2",
      conversationJid: "1234567890@s.whatsapp.net",
      chatType: "direct" as const,
      senderJid: "1234567890@s.whatsapp.net",
      senderE164: "+1234567890",
      senderName: "Test User",
      body: "Today",
      timestamp: now,
      isFromMe: false,
      rawMessage: { key: { id: "msg-2" } } as any,
      accountId: "default",
    });

    const filtered = store.queryMessages({
      from: new Date(now - 1000).toISOString(),
    });

    expect(filtered).toHaveLength(1);
    expect(filtered[0].body).toBe("Today");

    store.close();
  });

  it("exports messages grouped by conversation", () => {
    const store = new WhatsAppHistoryStore(dbPath);
    store.initialize();

    store.storeMessage({
      messageId: "msg-1",
      conversationJid: "1234567890@s.whatsapp.net",
      chatType: "direct" as const,
      senderJid: "1234567890@s.whatsapp.net",
      senderE164: "+1234567890",
      senderName: "User 1",
      body: "Message 1",
      timestamp: Date.now(),
      isFromMe: false,
      rawMessage: { key: { id: "msg-1" } } as any,
      accountId: "default",
      displayName: "User 1",
    });

    store.storeMessage({
      messageId: "msg-2",
      conversationJid: "1234567890@s.whatsapp.net",
      chatType: "direct" as const,
      senderJid: "1234567890@s.whatsapp.net",
      senderE164: "+1234567890",
      senderName: "User 1",
      body: "Message 2",
      timestamp: Date.now() + 1000,
      isFromMe: false,
      rawMessage: { key: { id: "msg-2" } } as any,
      accountId: "default",
      displayName: "User 1",
    });

    const exported = store.exportMessages();
    expect(exported).toHaveLength(1);
    expect(exported[0].jid).toBe("1234567890@s.whatsapp.net");
    expect(exported[0].messages).toHaveLength(2);
    expect(exported[0].messages[0].body).toBe("Message 1");
    expect(exported[0].messages[1].body).toBe("Message 2");

    store.close();
  });
});


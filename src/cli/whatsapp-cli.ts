import type { Command } from "commander";
import { writeFile } from "node:fs/promises";
import { WhatsAppHistoryStore } from "../web/history-store.js";
import { defaultRuntime } from "../runtime.js";
import { formatDocsLink } from "../terminal/links.js";
import { theme } from "../terminal/theme.js";
import { resolveUserPath } from "../utils.js";

type ExportFormat = "json" | "csv" | "jsonl";

function validateIsoDate(dateStr: string): boolean {
  const date = new Date(dateStr);
  return !Number.isNaN(date.getTime());
}

function csvEscape(value: string): string {
  if (value.includes('"') || value.includes(",") || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function toCsvRow(values: Array<string | number | null | undefined>): string {
  return values
    .map((val) => {
      if (val === undefined || val === null) {
        return "";
      }
      return csvEscape(String(val));
    })
    .join(",");
}

export function registerWhatsAppCli(program: Command) {
  const whatsapp = program
    .command("whatsapp")
    .description("WhatsApp utilities")
    .addHelpText(
      "after",
      () =>
        `\n${theme.muted("Docs:")} ${formatDocsLink("/cli/whatsapp", "docs.openclaw.ai/cli/whatsapp")}\n`,
    );

  whatsapp
    .command("export-history")
    .description("Export WhatsApp message history from SQLite database")
    .option("--format <format>", "Output format (json, csv, or jsonl)", "json")
    .option("--output <path>", "Output file path", "./whatsapp-history-export.json")
    .option("--from <date>", "Start date in ISO 8601 format (e.g., 2024-01-01)")
    .option("--to <date>", "End date in ISO 8601 format (e.g., 2024-12-31)")
    .option("--conversation <jid>", "Filter by specific conversation JID")
    .option("--account <accountId>", "Filter by WhatsApp account ID")
    .option("--limit <number>", "Maximum number of messages to export", Number.parseInt)
    .option("--db-path <path>", "Path to SQLite database (default: ~/.openclaw/whatsapp-history.sqlite)")
    .option("--verbose", "Show progress during export", false)
    .action(async (opts) => {
      try {
        // Validate format
        const format = opts.format?.toLowerCase() as ExportFormat;
        if (!["json", "csv", "jsonl"].includes(format)) {
          throw new Error(`Invalid format: ${opts.format}. Must be one of: json, csv, jsonl`);
        }

        // Validate dates
        if (opts.from && !validateIsoDate(opts.from)) {
          throw new Error(`Invalid --from date: ${opts.from}. Use ISO 8601 format (e.g., 2024-01-01)`);
        }
        if (opts.to && !validateIsoDate(opts.to)) {
          throw new Error(`Invalid --to date: ${opts.to}. Use ISO 8601 format (e.g., 2024-12-31)`);
        }

        // Validate limit
        if (opts.limit !== undefined && (Number.isNaN(opts.limit) || opts.limit <= 0)) {
          throw new Error(`Invalid --limit: ${opts.limit}. Must be a positive number`);
        }

        const verbose = Boolean(opts.verbose);

        if (verbose) {
          defaultRuntime.log(theme.info("Initializing WhatsApp history store..."));
        }

        // Initialize history store
        const store = new WhatsAppHistoryStore(opts.dbPath);
        store.initialize();

        if (verbose) {
          const stats = store.getStats();
          defaultRuntime.log(
            theme.info(
              `Database contains ${stats.totalMessages} messages across ${stats.totalConversations} conversations`,
            ),
          );
        }

        // Build filter
        const filter = {
          from: opts.from,
          to: opts.to,
          conversationJid: opts.conversation,
          accountId: opts.account,
          limit: opts.limit,
        };

        if (verbose) {
          defaultRuntime.log(theme.info("Exporting messages..."));
        }

        // Export based on format
        let outputContent: string;
        if (format === "json") {
          const conversations = store.exportMessages(filter);
          outputContent = JSON.stringify(conversations, null, 2);
        } else if (format === "csv") {
          const messages = store.queryMessages(filter);
          const headers = [
            "message_id",
            "conversation_jid",
            "sender_jid",
            "sender_e164",
            "sender_name",
            "body",
            "timestamp",
            "is_from_me",
            "reply_to_id",
            "media_path",
            "media_type",
            "location_lat",
            "location_lon",
            "account_id",
          ];
          const rows = messages.map((msg) =>
            toCsvRow([
              msg.message_id,
              msg.conversation_jid,
              msg.sender_jid,
              msg.sender_e164,
              msg.sender_name,
              msg.body,
              msg.timestamp,
              msg.is_from_me,
              msg.reply_to_id,
              msg.media_path,
              msg.media_type,
              msg.location_lat,
              msg.location_lon,
              msg.account_id,
            ]),
          );
          outputContent = [toCsvRow(headers), ...rows].join("\n");
        } else {
          // jsonl
          const messages = store.queryMessages(filter);
          outputContent = messages.map((msg) => JSON.stringify(msg)).join("\n");
        }

        // Write to file
        const outputPath = resolveUserPath(opts.output);
        await writeFile(outputPath, outputContent, "utf-8");

        store.close();

        if (verbose) {
          defaultRuntime.log(theme.success(`✓ Exported to ${outputPath}`));
        } else {
          defaultRuntime.log(outputPath);
        }
      } catch (err) {
        defaultRuntime.error(theme.danger(`Export failed: ${err instanceof Error ? err.message : String(err)}`));
        defaultRuntime.exit(1);
      }
    });
}

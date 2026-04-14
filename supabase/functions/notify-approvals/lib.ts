export interface ApprovalRow {
  id?: string | null;
  created_at?: string | null;
  requester?: string | null;
  action?: string | null;
  campus?: string | null;
  day?: string | null;
  start_time?: string | null;
  end_time?: string | null;
  details?: string | null;
  status?: string | null;
}

export interface CalloutRow {
  approval_id?: string | null;
  submitted_at?: string | null;
  campus?: string | null;
  caller_name?: string | null;
  reason?: string | null;
  event_date?: string | null;
  shift_start_at?: string | null;
  shift_end_at?: string | null;
  notice_hours?: number | string | null;
}

export interface DbWebhookPayload {
  type?: string | null;
  table?: string | null;
  schema?: string | null;
  record?: ApprovalRow | CalloutRow | null;
  old_record?: ApprovalRow | CalloutRow | null;
}

function asText(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
}

function cleanDetails(details: unknown): string {
  const raw = asText(details);
  return raw.replace(/META=\{.*?\}\s*\|\s*/i, "").trim();
}

function niceAction(action: unknown): string {
  switch (asText(action).toLowerCase()) {
    case "pickup":
      return "Pickup request";
    case "add":
      return "Add-shift request";
    case "remove":
      return "Remove-shift request";
    default:
      return asText(action) || "Approval request";
  }
}

function prettyReason(reason: unknown): string {
  const raw = asText(reason);
  if (!raw) {
    return "Unknown";
  }
  const normalized = raw.replace(/_/g, " ");
  if (normalized.includes(":")) {
    const [head, ...rest] = normalized.split(":");
    const tail = rest.join(":").trim();
    const label = head.trim().replace(/\b\w/g, (ch) => ch.toUpperCase());
    return tail ? `${label}: ${tail}` : label;
  }
  return normalized.replace(/\b\w/g, (ch) => ch.toUpperCase());
}

function parseIsoDateTime(value: unknown): Date | null {
  const raw = asText(value);
  if (!raw) {
    return null;
  }
  const ms = Date.parse(raw);
  if (Number.isNaN(ms)) {
    return null;
  }
  return new Date(ms);
}

function formatLaTime(value: unknown): string {
  const dt = parseIsoDateTime(value);
  if (!dt) {
    return asText(value) || "Unknown";
  }
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    hour: "numeric",
    minute: "2-digit",
  }).format(dt);
}

function formatNoticeHours(value: number): string {
  return `${value.toFixed(2)} hours`;
}

function coerceNoticeHours(row: CalloutRow): number | null {
  const rawNotice = row.notice_hours;
  if (rawNotice !== null && rawNotice !== undefined && `${rawNotice}`.trim() !== "") {
    const num = Number(rawNotice);
    if (!Number.isNaN(num)) {
      return num;
    }
  }

  const submittedAt = parseIsoDateTime(row.submitted_at);
  const shiftStartAt = parseIsoDateTime(row.shift_start_at);
  if (!(submittedAt && shiftStartAt)) {
    return null;
  }

  return (shiftStartAt.getTime() - submittedAt.getTime()) / 3600000;
}

function buildAdaptiveCardPayload(args: {
  title: string;
  subtitle: string;
  facts: Array<{ title: string; value: string }>;
  detailsText: string;
  hiddenText: string;
}) {
  return {
    type: "message",
    attachments: [
      {
        contentType: "application/vnd.microsoft.card.adaptive",
        contentUrl: null,
        content: {
          $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
          type: "AdaptiveCard",
          version: "1.4",
          msteams: {
            width: "Full",
          },
          body: [
            {
              type: "TextBlock",
              text: args.title,
              weight: "Bolder",
              size: "Medium",
              wrap: true,
            },
            {
              type: "TextBlock",
              text: args.subtitle,
              spacing: "Small",
              wrap: true,
            },
            {
              type: "FactSet",
              facts: args.facts,
            },
            {
              type: "TextBlock",
              text: args.detailsText,
              wrap: true,
            },
            {
              type: "TextBlock",
              text: args.hiddenText,
              wrap: true,
              isVisible: false,
            },
          ],
        },
      },
    ],
  };
}

function buildApprovalText(row: ApprovalRow): string {
  const details = cleanDetails(row.details) || "requested";
  const requester = asText(row.requester) || "Unknown";
  const campus = asText(row.campus) || "Unknown";
  const day = asText(row.day) || "Unknown";
  const startTime = asText(row.start_time) || "Unknown";
  const endTime = asText(row.end_time) || "Unknown";
  const requestId = asText(row.id) || "Unknown";

  return [
    "New OA approval request",
    "",
    `Type: ${niceAction(row.action)}`,
    `Requester: ${requester}`,
    `Campus: ${campus}`,
    `Day: ${day}`,
    `Time: ${startTime} - ${endTime}`,
    `Details: ${details}`,
    `Request ID: ${requestId}`,
  ].join("\n");
}

function buildApprovalPayload(row: ApprovalRow) {
  const details = cleanDetails(row.details) || "requested";
  const requestType = niceAction(row.action);
  const requester = asText(row.requester) || "Unknown";
  const campus = asText(row.campus) || "Unknown";
  const day = asText(row.day) || "Unknown";
  const startTime = asText(row.start_time) || "Unknown";
  const endTime = asText(row.end_time) || "Unknown";
  const requestId = asText(row.id) || "Unknown";
  const createdAt = asText(row.created_at);

  return buildAdaptiveCardPayload({
    title: "New OA approval request",
    subtitle: requestType,
    facts: [
      { title: "Requester", value: requester },
      { title: "Campus", value: campus },
      { title: "Day", value: day },
      { title: "Time", value: `${startTime} - ${endTime}` },
      { title: "Request ID", value: requestId },
      ...(createdAt ? [{ title: "Created", value: createdAt }] : []),
    ],
    detailsText: `Details: ${details}`,
    hiddenText: buildApprovalText(row),
  });
}

function buildLateCalloutText(row: CalloutRow, noticeHours: number): string {
  const caller = asText(row.caller_name) || "Unknown";
  const campus = asText(row.campus) || "Unknown";
  const eventDate = asText(row.event_date) || "Unknown";
  const reason = prettyReason(row.reason);
  const shift = `${formatLaTime(row.shift_start_at)} - ${formatLaTime(row.shift_end_at)}`;
  const calloutId = asText(row.approval_id) || "Unknown";

  return [
    "Late non-sick OA callout",
    "",
    "Rule: Non-sick callout under 48 hours",
    `Caller: ${caller}`,
    `Campus: ${campus}`,
    `Date: ${eventDate}`,
    `Shift: ${shift}`,
    `Notice: ${formatNoticeHours(noticeHours)}`,
    `Reason: ${reason}`,
    `Callout ID: ${calloutId}`,
  ].join("\n");
}

function buildLateCalloutPayload(row: CalloutRow, noticeHours: number) {
  const caller = asText(row.caller_name) || "Unknown";
  const campus = asText(row.campus) || "Unknown";
  const eventDate = asText(row.event_date) || "Unknown";
  const submittedAt = asText(row.submitted_at) || "Unknown";
  const calloutId = asText(row.approval_id) || "Unknown";
  const shift = `${formatLaTime(row.shift_start_at)} - ${formatLaTime(row.shift_end_at)}`;
  const reason = prettyReason(row.reason);

  return buildAdaptiveCardPayload({
    title: "Late non-sick OA callout",
    subtitle: "Non-sick callout under 48 hours",
    facts: [
      { title: "Caller", value: caller },
      { title: "Campus", value: campus },
      { title: "Date", value: eventDate },
      { title: "Shift", value: shift },
      { title: "Notice", value: formatNoticeHours(noticeHours) },
      { title: "Reason", value: reason },
      { title: "Callout ID", value: calloutId },
      { title: "Submitted", value: submittedAt },
    ],
    detailsText: "Alert: this non-sick callout was submitted with less than 48 hours of notice.",
    hiddenText: buildLateCalloutText(row, noticeHours),
  });
}

export function buildTeamsPayload(payload: DbWebhookPayload): Record<string, unknown> | null {
  const type = asText(payload.type).toUpperCase();
  const table = asText(payload.table);
  const schema = asText(payload.schema);

  if (type !== "INSERT" || (schema && schema !== "public")) {
    return null;
  }

  if (table === "approvals") {
    const row = payload.record as ApprovalRow | null;
    if (!row) {
      return null;
    }
    const status = asText(row.status).toUpperCase();
    if (status !== "PENDING") {
      return null;
    }
    return buildApprovalPayload(row);
  }

  if (table === "callouts") {
    const row = payload.record as CalloutRow | null;
    if (!row) {
      return null;
    }
    const reason = asText(row.reason).toLowerCase();
    const noticeHours = coerceNoticeHours(row);
    if (noticeHours === null || reason.startsWith("sick") || noticeHours >= 48.0) {
      return null;
    }
    return buildLateCalloutPayload(row, noticeHours);
  }

  return null;
}

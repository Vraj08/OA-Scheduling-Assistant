import { buildTeamsPayload } from "./lib.ts";

Deno.test("buildTeamsPayload keeps pending approvals on the existing webhook format", () => {
  const payload = buildTeamsPayload({
    type: "INSERT",
    schema: "public",
    table: "approvals",
    record: {
      id: "a1b2c3",
      created_at: "2026-04-14T10:00:00-07:00",
      requester: "Vraj Patel",
      action: "pickup",
      campus: "UNH",
      day: "Tuesday",
      start_time: "6:00 PM",
      end_time: "8:00 PM",
      details: 'META={"sheet_title":"UNH (OA and GOAs)"} | target=Mary Tekele',
      status: "PENDING",
    },
  });

  if (!payload) {
    throw new Error("Expected approval payload");
  }

  const firstAttachment = (payload.attachments as Array<Record<string, unknown>>)[0];
  const content = firstAttachment.content as Record<string, unknown>;
  const body = content.body as Array<Record<string, unknown>>;
  if (body[0].text !== "New OA approval request") {
    throw new Error(`Unexpected approval title: ${body[0].text}`);
  }
});

Deno.test("buildTeamsPayload emits late non-sick callout alerts to the same Teams payload shape", () => {
  const payload = buildTeamsPayload({
    type: "INSERT",
    schema: "public",
    table: "callouts",
    record: {
      approval_id: "direct_callout|MC|2026-04-14|alexsmith|07:00|09:00",
      submitted_at: "2026-04-13T08:00:00-07:00",
      campus: "MC",
      caller_name: "Alex Smith",
      reason: "personal",
      event_date: "2026-04-14",
      shift_start_at: "2026-04-14T07:00:00-07:00",
      shift_end_at: "2026-04-14T09:00:00-07:00",
    },
  });

  if (!payload) {
    throw new Error("Expected late callout payload");
  }

  const firstAttachment = (payload.attachments as Array<Record<string, unknown>>)[0];
  const content = firstAttachment.content as Record<string, unknown>;
  const body = content.body as Array<Record<string, unknown>>;
  if (body[0].text !== "Late non-sick OA callout") {
    throw new Error(`Unexpected late-callout title: ${body[0].text}`);
  }

  const facts = (body[2].facts as Array<Record<string, string>>);
  const noticeFact = facts.find((fact) => fact.title === "Notice");
  if (!noticeFact || noticeFact.value !== "23.00 hours") {
    throw new Error(`Unexpected notice fact: ${JSON.stringify(noticeFact)}`);
  }
});

Deno.test("buildTeamsPayload ignores sick callouts even when they are under two hours", () => {
  const payload = buildTeamsPayload({
    type: "INSERT",
    schema: "public",
    table: "callouts",
    record: {
      approval_id: "direct_callout|ONCALL|2026-04-14|alexsmith|07:00|09:00",
      submitted_at: "2026-04-14T06:00:00-07:00",
      campus: "ONCALL",
      caller_name: "Alex Smith",
      reason: "sick",
      event_date: "2026-04-14",
      shift_start_at: "2026-04-14T07:00:00-07:00",
      shift_end_at: "2026-04-14T09:00:00-07:00",
    },
  });

  if (payload !== null) {
    throw new Error("Expected sick late callout to be ignored");
  }
});

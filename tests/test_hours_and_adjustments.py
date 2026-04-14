import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from oa_app.core import utils
from oa_app.services import approvals, callouts_db, pickups_db
from oa_app.jobs import sync_swaps_to_sheets
from oa_app.ui import page, schedule_query, ui_peek


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def select(self, *_args, **_kwargs):
        return self

    def gte(self, *_args, **_kwargs):
        return self

    def lte(self, *_args, **_kwargs):
        return self

    def execute(self):
        return SimpleNamespace(data=self._rows)


class _FakeSupabase:
    def __init__(self, table_rows):
        self._table_rows = table_rows

    def table(self, name):
        return _FakeQuery(self._table_rows.get(name, []))


class _FakeApprovalSheet:
    def __init__(self, *, fail_append: bool = False, fail_update: bool = False):
        self.fail_append = fail_append
        self.fail_update = fail_update
        self.appended_rows = []
        self.updated_ranges = []

    def append_row(self, row, value_input_option="RAW"):
        if self.fail_append:
            raise RuntimeError("append failed")
        self.appended_rows.append((list(row), value_input_option))

    def update(self, range_name, values):
        if self.fail_update:
            raise RuntimeError("update failed")
        self.updated_ranges.append((range_name, values))


class _FakeGridSpreadsheet:
    def __init__(self, grid, bg_grid):
        self.grid = grid
        self.bg_grid = bg_grid
        self.batch_requests = []

    def fetch_sheet_metadata(self, params=None):
        row_data = []
        max_cols = max((len(r) for r in self.grid), default=0)
        for r, row in enumerate(self.grid):
            values = []
            for c in range(max_cols):
                cell = {}
                txt = row[c] if c < len(row) else ""
                if txt != "":
                    cell["formattedValue"] = txt
                bg = None
                if r < len(self.bg_grid) and c < len(self.bg_grid[r]):
                    bg = self.bg_grid[r][c]
                if bg:
                    cell["effectiveFormat"] = {"backgroundColor": dict(bg)}
                values.append(cell)
            row_data.append({"values": values})
        return {"sheets": [{"data": [{"rowData": row_data}]}]}

    def batch_update(self, body):
        self.batch_requests.append(body)


class _FakeGridWorksheet:
    def __init__(self, *, title, grid, bg_grid, sheet_id=1, hidden=False):
        self.title = title
        self.id = sheet_id
        self._properties = {"hidden": hidden}
        self.spreadsheet = _FakeGridSpreadsheet(grid, bg_grid)


class _FakeWorkbook:
    def __init__(self, worksheets):
        self._worksheets = worksheets

    def worksheets(self):
        return list(self._worksheets)


class ScheduleQueryHoursTests(unittest.TestCase):
    def test_unh_mc_counts_half_hours_when_headers_are_below_top_row(self):
        grid = [
            ["OA schedule", "", ""],
            ["Time", "Monday", "Tuesday"],
            ["7:00 AM", "", ""],
            ["", "OA: Alex Smith", ""],
            ["7:30 AM", "", ""],
            ["", "Alex Smith", ""],
            ["8:00 AM", "", ""],
        ]
        ws = SimpleNamespace(title="UNH (OA and GOAs)")
        start = schedule_query._fmt(schedule_query._parse_time_cell("7:00 AM"))
        end = schedule_query._fmt(schedule_query._parse_time_cell("8:00 AM"))

        with patch.object(schedule_query, "_read_grid", return_value=grid):
            ranges = schedule_query._unh_mc_ranges(ws, schedule_query._norm_name("Alex Smith"))

        self.assertEqual(ranges, {"monday": [(start, end)]})

    def test_oncall_uses_date_headers_and_explicit_block_lengths(self):
        grid = [
            ["", "4/6", "4/11"],
            ["", "7:00 PM - 12:00 AM", "8:00 PM - 12:00 AM"],
            ["", "OA: Alex Smith", "GOA: Alex Smith"],
        ]
        ws = SimpleNamespace(title="On Call 4/5/2026 - 4/11/2026")

        with patch.object(schedule_query, "_read_grid", return_value=grid):
            blocks = schedule_query._oncall_blocks(ws, schedule_query._norm_name("Alex Smith"))

        mon = blocks.get("monday", [])
        sat = blocks.get("saturday", [])
        self.assertEqual(len(mon), 1)
        self.assertEqual(len(sat), 1)
        self.assertEqual(schedule_query._mins_between(*mon[0]), 300)
        self.assertEqual(schedule_query._mins_between(*sat[0]), 240)

    def test_oncall_supports_shared_time_labels_in_column_a(self):
        grid = [
            ["", "Monday"],
            ["7:00 PM - 12:00 AM", ""],
            ["", "Alex Smith"],
        ]
        ws = SimpleNamespace(title="On Call 4/5/2026 - 4/11/2026")

        with patch.object(schedule_query, "_read_grid", return_value=grid):
            blocks = schedule_query._oncall_blocks(ws, schedule_query._norm_name("Alex Smith"))

        self.assertEqual(len(blocks.get("monday", [])), 1)
        self.assertEqual(schedule_query._mins_between(*blocks["monday"][0]), 300)

    def test_build_schedule_dataframe_anchors_week_sunday_through_saturday(self):
        user_sched = {
            "sunday": {"UNH": [("8:00 AM", "10:00 AM")], "MC": [], "On-Call": []},
            "saturday": {"UNH": [], "MC": [("1:00 PM", "3:00 PM")], "On-Call": []},
        }

        with patch.object(schedule_query.week_range_mod, "la_today", return_value=date(2026, 4, 14)):
            df = schedule_query.build_schedule_dataframe(user_sched)

        self.assertEqual(df["Day"].tolist(), ["Sunday", "Saturday"])
        self.assertEqual(df["Date"].tolist(), [date(2026, 4, 12), date(2026, 4, 18)])

    def test_render_user_schedule_markdown_lists_sunday_before_monday(self):
        rendered = schedule_query.render_user_schedule_markdown(
            {
                "monday": {"UNH": [("8:00 AM", "10:00 AM")], "MC": [], "On-Call": []},
                "sunday": {"UNH": [], "MC": [("1:00 PM", "3:00 PM")], "On-Call": []},
            },
            include_weekly_summary=False,
        )

        self.assertLess(rendered.index("### Sunday"), rendered.index("### Monday"))


class AdjustmentDurationTests(unittest.TestCase):
    def test_callouts_compute_duration_hours_from_timestamps_when_missing(self):
        supabase = _FakeSupabase(
            {
                "callouts": [
                    {
                        "event_date": "2026-04-06",
                        "duration_hours": None,
                        "shift_start_at": "2026-04-06T19:00:00-07:00",
                        "shift_end_at": "2026-04-07T00:00:00-07:00",
                        "caller_name": "Alex Smith",
                    }
                ]
            }
        )

        with patch.object(callouts_db, "supabase_callouts_enabled", return_value=True), patch.object(
            callouts_db, "get_supabase", return_value=supabase
        ), patch.object(callouts_db, "with_retry", side_effect=lambda fn: fn()):
            rows = callouts_db.list_callouts_for_week(
                caller_name="Alex Smith",
                week_start=date(2026, 4, 5),
                week_end=date(2026, 4, 11),
            )
            total = callouts_db.sum_callout_hours_for_week(
                caller_name="Alex Smith",
                week_start=date(2026, 4, 5),
                week_end=date(2026, 4, 11),
            )

        self.assertAlmostEqual(rows[0]["duration_hours"], 5.0)
        self.assertAlmostEqual(total, 5.0)

    def test_pickups_compute_duration_hours_from_timestamps_when_missing(self):
        supabase = _FakeSupabase(
            {
                "pickups": [
                    {
                        "event_date": "2026-04-11",
                        "duration_hours": None,
                        "shift_start_at": "2026-04-11T20:00:00-07:00",
                        "shift_end_at": "2026-04-12T00:00:00-07:00",
                        "picker_name": "Alex Smith",
                    }
                ]
            }
        )

        with patch.object(pickups_db, "supabase_pickups_enabled", return_value=True), patch.object(
            pickups_db, "get_supabase", return_value=supabase
        ), patch.object(pickups_db, "with_retry", side_effect=lambda fn: fn()):
            rows = pickups_db.list_pickups_for_week(
                picker_name="Alex Smith",
                week_start=date(2026, 4, 5),
                week_end=date(2026, 4, 11),
            )
            total = pickups_db.sum_pickup_hours_for_week(
                picker_name="Alex Smith",
                week_start=date(2026, 4, 5),
                week_end=date(2026, 4, 11),
            )

        self.assertAlmostEqual(rows[0]["duration_hours"], 4.0)
        self.assertAlmostEqual(total, 4.0)


class CalloutNoticeTests(unittest.TestCase):
    def test_late_notice_callouts_use_notice_hours_policy(self):
        supabase = _FakeSupabase(
            {
                "callouts": [
                    {
                        "event_date": "2026-04-13",
                        "caller_name": "Alex Smith",
                        "campus": "MC",
                        "reason": "sick",
                        "notice_hours": 1.5,
                        "submitted_at": "2026-04-13T06:30:00-07:00",
                        "shift_start_at": "2026-04-13T08:00:00-07:00",
                        "shift_end_at": "2026-04-13T10:00:00-07:00",
                        "duration_hours": 2.0,
                    },
                    {
                        "event_date": "2026-04-14",
                        "caller_name": "Taylor Jones",
                        "campus": "UNH",
                        "reason": "personal",
                        "notice_hours": 20.0,
                        "submitted_at": "2026-04-13T09:00:00-07:00",
                        "shift_start_at": "2026-04-14T05:00:00-07:00",
                        "shift_end_at": "2026-04-14T09:00:00-07:00",
                        "duration_hours": 4.0,
                    },
                    {
                        "event_date": "2026-04-15",
                        "caller_name": "Jordan Lee",
                        "campus": "ONCALL",
                        "reason": "sick",
                        "notice_hours": 6.0,
                        "submitted_at": "2026-04-15T01:00:00-07:00",
                        "shift_start_at": "2026-04-15T07:00:00-07:00",
                        "shift_end_at": "2026-04-15T11:00:00-07:00",
                        "duration_hours": 4.0,
                    },
                ]
            }
        )

        with patch.object(callouts_db, "supabase_callouts_enabled", return_value=True), patch.object(
            callouts_db, "get_supabase", return_value=supabase
        ), patch.object(callouts_db, "with_retry", side_effect=lambda fn: fn()):
            rows = callouts_db.list_late_notice_callouts(
                week_start=date(2026, 4, 12),
                week_end=date(2026, 4, 18),
            )

        self.assertEqual([r["caller_name"] for r in rows], ["Taylor Jones", "Alex Smith"])
        self.assertEqual(rows[0]["late_notice_rule"], "Non-sick callout under 48 hours")
        self.assertEqual(rows[1]["late_notice_rule"], "Sick callout under 2 hours")

    def test_late_notice_callouts_fall_back_to_timestamp_difference(self):
        supabase = _FakeSupabase(
            {
                "callouts": [
                    {
                        "event_date": "2026-04-13",
                        "caller_name": "Alex Smith",
                        "campus": "MC",
                        "reason": "other:travel",
                        "notice_hours": None,
                        "submitted_at": "2026-04-13T08:00:00-07:00",
                        "shift_start_at": "2026-04-14T07:00:00-07:00",
                        "shift_end_at": "2026-04-14T09:00:00-07:00",
                        "duration_hours": 2.0,
                    }
                ]
            }
        )

        with patch.object(callouts_db, "supabase_callouts_enabled", return_value=True), patch.object(
            callouts_db, "get_supabase", return_value=supabase
        ), patch.object(callouts_db, "with_retry", side_effect=lambda fn: fn()):
            rows = callouts_db.list_late_notice_callouts(
                week_start=date(2026, 4, 12),
                week_end=date(2026, 4, 18),
            )

        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["notice_hours"], 23.0)
        self.assertEqual(rows[0]["late_notice_rule"], "Non-sick callout under 48 hours")


class ApprovalWorkflowTests(unittest.TestCase):
    def test_submit_request_reuses_matching_pending_request_before_insert(self):
        existing = {
            "ID": "abc123",
            "Requester": "Alex Smith",
            "Action": "pickup",
            "Campus": "ONCALL",
            "Day": "Sunday",
            "Start": "7:00 PM",
            "End": "12:00 AM",
            "Details": "META={\"sheet_title\":\"On Call 4/12 - 4/18\"} | target=Taylor",
            "Status": "PENDING",
        }

        with patch.object(approvals, "_use_db", return_value=False), patch.object(
            approvals, "read_requests", return_value=[existing]
        ), patch.object(approvals, "ensure_approval_sheet") as ensure_sheet:
            rid = approvals.submit_request(
                SimpleNamespace(),
                requester="Alex Smith",
                action="pickup",
                campus="ONCALL",
                day="Sunday",
                start="7:00 PM",
                end="12:00 AM",
                details="META={\"sheet_title\":\"On Call 4/12 - 4/18\"} | target=Taylor",
            )

        self.assertEqual(rid, "abc123")
        ensure_sheet.assert_not_called()

    def test_submit_request_recovers_existing_request_after_append_error(self):
        existing = {
            "ID": "xyz999",
            "Requester": "Alex Smith",
            "Action": "add",
            "Campus": "MC",
            "Day": "Monday",
            "Start": "8:00 AM",
            "End": "10:00 AM",
            "Details": "META={\"sheet_title\":\"MC (OA and GOAs)\"} | requested",
            "Status": "PENDING",
        }
        ws = _FakeApprovalSheet(fail_append=True)

        with patch.object(approvals, "_use_db", return_value=False), patch.object(
            approvals, "read_requests", side_effect=[[], [existing]]
        ), patch.object(approvals, "ensure_approval_sheet", return_value=ws), patch.object(
            approvals, "bump_ws_version"
        ):
            rid = approvals.submit_request(
                SimpleNamespace(),
                requester="Alex Smith",
                action="add",
                campus="MC",
                day="Monday",
                start="8:00 AM",
                end="10:00 AM",
                details="META={\"sheet_title\":\"MC (OA and GOAs)\"} | requested",
            )

        self.assertEqual(rid, "xyz999")

    def test_set_status_treats_already_updated_row_as_success(self):
        ws = _FakeApprovalSheet(fail_update=True)
        approved = {
            "ID": "done42",
            "Status": "APPROVED",
            "_row": 7,
        }

        with patch.object(approvals, "_use_db", return_value=False), patch.object(
            approvals, "ensure_approval_sheet", return_value=ws
        ), patch.object(approvals, "get_request", return_value=approved):
            approvals.set_status(
                SimpleNamespace(),
                row=7,
                req_id="done42",
                status="APPROVED",
                reviewed_by="Vraj",
                note="ok",
            )


class ApprovalDateResolutionTests(unittest.TestCase):
    def test_approval_row_event_date_uses_created_week_for_rolling_mc_titles_without_explicit_date(self):
        req = {
            "Campus": "MC",
            "Day": "Thursday",
            "Created": "2026-04-03T10:00:00-07:00",
            "Details": 'META={"campus_key":"MC","sheet_title":"MC (OA and GOAs)"} | target=Bajaj Aryaman',
        }

        with patch.object(page, "_date_for_weekday_in_sheet", side_effect=AssertionError("rolling tabs should not use the current sheet week")):
            event_d = page._approval_row_event_date(object(), req)

        self.assertEqual(event_d, date(2026, 4, 2))

    def test_append_my_pickups_skips_old_approved_pickups_from_prior_rolling_week(self):
        base_sched = {"thursday": {"UNH": [], "MC": [], "On-Call": []}}
        old_pickup = {
            "Action": "pickup",
            "Status": "APPROVED",
            "Requester": "Shepard, Tessa",
            "Campus": "MC",
            "Day": "Thursday",
            "Start": "10:00 AM",
            "End": "1:00 PM",
            "Created": "2026-04-03T10:00:00-07:00",
            "Details": 'META={"campus_key":"MC","sheet_title":"MC (OA and GOAs)"} | target=Bajaj Aryaman',
        }

        with patch.object(page, "_date_for_weekday_in_sheet", side_effect=AssertionError("rolling tabs should not use the current sheet week")):
            merged = page._append_my_pickups_into_sched(
                base_sched,
                [old_pickup],
                requester="Shepard, Tessa",
                week_titles={"MC (OA and GOAs)"},
                include_statuses={"APPROVED"},
                ss=object(),
                week_bounds=(date(2026, 4, 12), date(2026, 4, 18)),
            )

        self.assertEqual(merged["thursday"]["MC"], [])

    def test_append_my_pickups_keeps_current_week_pickups_with_explicit_date(self):
        base_sched = {"thursday": {"UNH": [], "MC": [], "On-Call": []}}
        current_pickup = {
            "Action": "pickup",
            "Status": "APPROVED",
            "Requester": "Shepard, Tessa",
            "Campus": "MC",
            "Day": "Thursday",
            "Start": "10:00 AM",
            "End": "1:00 PM",
            "Created": "2026-04-14T10:00:00-07:00",
            "Details": 'META={"campus_key":"MC","sheet_title":"MC (OA and GOAs)"} | target=Bajaj Aryaman | date=2026-04-16',
        }

        merged = page._append_my_pickups_into_sched(
            base_sched,
            [current_pickup],
            requester="Shepard, Tessa",
            week_titles={"MC (OA and GOAs)"},
            include_statuses={"APPROVED"},
            ss=object(),
            week_bounds=(date(2026, 4, 12), date(2026, 4, 18)),
        )

        self.assertEqual(merged["thursday"]["MC"], [("10:00 AM", "1:00 PM")])


class CalloutDateSelectionTests(unittest.TestCase):
    def test_callout_event_date_for_sheet_prefers_inferred_sheet_week_date(self):
        with patch.object(page, "_date_for_weekday_in_sheet", return_value=date(2026, 4, 16)):
            event_d = page._callout_event_date_for_sheet(object(), "MC (OA and GOAs)", "thursday")

        self.assertEqual(event_d, date(2026, 4, 16))

    def test_callout_event_date_for_sheet_falls_back_to_matching_day_not_week_start(self):
        with patch.object(page, "_date_for_weekday_in_sheet", return_value=None), patch.object(
            page, "_week_bounds_la", return_value=(date(2026, 4, 12), date(2026, 4, 18))
        ):
            event_d = page._callout_event_date_for_sheet(object(), "MC (OA and GOAs)", "tuesday")

        self.assertEqual(event_d, date(2026, 4, 14))


class OvertimeBaselineTests(unittest.TestCase):
    def test_overtime_baseline_uses_adjusted_hours_from_approved_changes(self):
        base_sched = {
            "monday": {
                "UNH": [("8:00 AM", "12:00 PM")],
                "MC": [("1:00 PM", "3:00 PM")],
            }
        }

        with patch.object(page.callouts_db, "supabase_callouts_enabled", return_value=True), patch.object(
            page.pickups_db, "supabase_pickups_enabled", return_value=True
        ), patch.object(
            page,
            "_approved_adjustment_minutes_for_week",
            return_value=(60, {"monday": 60}, 120, {"monday": 120}),
        ):
            week_mins, per_day = page._overtime_baseline_minutes(
                None,
                requester="Alex Smith",
                base_sched=base_sched,
                approvals_rows=[],
                week_bounds=(date(2026, 4, 12), date(2026, 4, 18)),
            )

        self.assertEqual(week_mins, 300)
        self.assertEqual(per_day["monday"], 300)

    def test_overtime_baseline_ignores_other_pending_pickups(self):
        base_sched = {"monday": {"UNH": [("8:00 AM", "12:00 PM")]}}
        pending_pickup = {
            "Action": "pickup",
            "Status": "PENDING",
            "Requester": "Alex Smith",
            "Day": "Monday",
            "Start": "4:00 PM",
            "End": "6:00 PM",
            "Details": "target=Taylor | date=2026-04-13",
            "Created": "2026-04-13T10:00:00-07:00",
        }

        with patch.object(page.callouts_db, "supabase_callouts_enabled", return_value=True), patch.object(
            page.pickups_db, "supabase_pickups_enabled", return_value=True
        ), patch.object(page, "_approved_adjustment_minutes_for_week", return_value=(0, {}, 0, {})):
            week_mins, per_day = page._overtime_baseline_minutes(
                None,
                requester="Alex Smith",
                base_sched=base_sched,
                approvals_rows=[pending_pickup],
                week_bounds=(date(2026, 4, 12), date(2026, 4, 18)),
            )

        self.assertEqual(week_mins, 240)
        self.assertEqual(per_day["monday"], 240)


class CampusNormalizationTests(unittest.TestCase):
    def test_oncall_titles_normalize_to_oncall_for_future_approvals(self):
        campus_key = utils.normalize_campus("On Call 4/19 - 4/25", "MC")

        self.assertEqual(campus_key, "ONCALL")
        self.assertTrue(
            page._should_color_schedule_now(
                campus_key=campus_key,
                event_d=date(2026, 4, 19),
            )
        )

    def test_missing_campus_uses_oncall_default_title(self):
        self.assertEqual(
            utils.normalize_campus("", "On Call 4/19 - 4/25"),
            "ONCALL",
        )


class RequestDetailsDisplayTests(unittest.TestCase):
    def test_overtime_details_are_humanized_for_display(self):
        details = (
            'META={"sheet_title":"MC (OA and GOAs)"}'
            " | target=Taylor Jones"
            " | overtime: yes"
            " | week_after=21.50"
            " | day_after=8.50"
        )

        rendered = page._format_request_details_for_display(details)

        self.assertIn("Target: Taylor Jones", rendered)
        self.assertIn("Overtime requested: Yes", rendered)
        self.assertIn("Total hours for the day: 8.50", rendered)
        self.assertIn("Total hours for the week: 21.50", rendered)
        self.assertNotIn("day_after=", rendered)
        self.assertNotIn("week_after=", rendered)


class SyncRoutingTests(unittest.TestCase):
    def test_oncall_previous_week_does_not_receive_next_week_events(self):
        today = date(2026, 4, 7)
        bucket = sync_swaps_to_sheets._bucket_label_for_sheet_event(
            "On Call 4/5 - 4/11",
            date(2026, 4, 12),
            today=today,
        )
        self.assertIsNone(bucket)

    def test_oncall_week_sheet_uses_its_own_week_for_weekly_bucket(self):
        today = date(2026, 4, 7)
        self.assertEqual(
            sync_swaps_to_sheets._bucket_label_for_sheet_event(
                "On Call 4/12 - 4/18",
                date(2026, 4, 12),
                today=today,
            ),
            "weekly",
        )
        self.assertIsNone(
            sync_swaps_to_sheets._bucket_label_for_sheet_event(
                "On Call 4/12 - 4/18",
                date(2026, 4, 11),
                today=today,
            )
        )

    def test_mc_still_uses_future_column_for_later_weeks(self):
        today = date(2026, 4, 7)
        self.assertEqual(
            sync_swaps_to_sheets._bucket_label_for_sheet_event(
                "MC (OA and GOAs)",
                date(2026, 4, 12),
                today=today,
            ),
            "future",
        )

    def test_oncall_future_week_sheet_still_applies_grid_colors_for_its_own_events(self):
        today = date(2026, 4, 11)
        self.assertTrue(
            sync_swaps_to_sheets._should_apply_grid_color_for_sheet(
                "On Call 4/12 - 4/18",
                date(2026, 4, 12),
                today=today,
            )
        )
        self.assertFalse(
            sync_swaps_to_sheets._should_apply_grid_color_for_sheet(
                "On Call 4/5 - 4/11",
                date(2026, 4, 12),
                today=today,
            )
        )

    def test_mc_future_week_sheet_does_not_apply_grid_colors_early(self):
        today = date(2026, 4, 11)
        self.assertFalse(
            sync_swaps_to_sheets._should_apply_grid_color_for_sheet(
                "MC (OA and GOAs)",
                date(2026, 4, 12),
                today=today,
            )
        )

    def test_manual_sync_skips_hidden_oncall_tabs(self):
        visible_oncall = SimpleNamespace(title="On Call 4/12 - 4/18", _properties={"hidden": False})
        hidden_oncall = SimpleNamespace(title="On Call 4/19 - 4/25", _properties={"hidden": True})
        visible_mc = SimpleNamespace(title="MC (OA and GOAs)", _properties={"hidden": False})
        hidden_policy = SimpleNamespace(title="EO Schedule Policies", _properties={"hidden": False})

        self.assertTrue(sync_swaps_to_sheets._should_auto_sync_worksheet(visible_oncall))
        self.assertFalse(sync_swaps_to_sheets._should_auto_sync_worksheet(hidden_oncall))
        self.assertTrue(sync_swaps_to_sheets._should_auto_sync_worksheet(visible_mc))
        self.assertFalse(sync_swaps_to_sheets._should_auto_sync_worksheet(hidden_policy))


class SyncGridReconcileTests(unittest.TestCase):
    def test_apply_grid_colors_clears_stale_past_cells_and_repaints_future_weekly_callout(self):
        grid = [
            ["Time", "Sunday", "Monday", "Wednesday"],
            ["7:00 AM", "", "", ""],
            ["", "OA: Pat", "OA: Alex Smith", "OA: Alex Smith"],
            ["7:30 AM", "", "", ""],
            ["", "OA: Pat", "OA: Alex Smith", "OA: Alex Smith"],
        ]
        bg_grid = [
            [None, None, None, None],
            [None, None, None, None],
            [None, None, sync_swaps_to_sheets.RED, None],
            [None, None, None, None],
            [None, None, sync_swaps_to_sheets.ORANGE, None],
        ]
        ws = _FakeGridWorksheet(title="MC (OA and GOAs)", grid=grid, bg_grid=bg_grid)
        callouts = [
            {
                "event_date": "2026-04-15",
                "shift_start_at": "2026-04-15T07:00:00-07:00",
                "shift_end_at": "2026-04-15T08:00:00-07:00",
                "caller_name": "Alex Smith",
                "campus": "MC",
            }
        ]

        with patch.object(sync_swaps_to_sheets, "bump_ws_version"):
            errs = sync_swaps_to_sheets._apply_grid_colors(
                ws,
                callouts=callouts,
                pickups=[],
                today=date(2026, 4, 14),
            )

        self.assertEqual(errs, [])
        painted = []
        for body in ws.spreadsheet.batch_requests:
            for req in body.get("requests", []):
                rep = req.get("repeatCell", {})
                rng = rep.get("range", {})
                color = (((rep.get("cell") or {}).get("userEnteredFormat") or {}).get("backgroundColor") or {})
                painted.append((rng.get("startRowIndex"), rng.get("startColumnIndex"), color))

        self.assertIn((2, 2, sync_swaps_to_sheets.WHITE), painted)
        self.assertIn((4, 2, sync_swaps_to_sheets.WHITE), painted)
        self.assertIn((2, 3, sync_swaps_to_sheets.RED), painted)
        self.assertIn((4, 3, sync_swaps_to_sheets.RED), painted)

    def test_apply_grid_colors_clears_stale_past_blank_colored_cells(self):
        grid = [
            ["Time", "Monday"],
            ["7:00 AM", ""],
            ["", ""],
        ]
        bg_grid = [
            [None, None],
            [None, None],
            [None, sync_swaps_to_sheets.RED],
        ]
        ws = _FakeGridWorksheet(title="MC (OA and GOAs)", grid=grid, bg_grid=bg_grid)

        with patch.object(sync_swaps_to_sheets, "bump_ws_version"):
            errs = sync_swaps_to_sheets._apply_grid_colors(
                ws,
                callouts=[],
                pickups=[],
                today=date(2026, 4, 14),
            )

        self.assertEqual(errs, [])
        painted = []
        for body in ws.spreadsheet.batch_requests:
            for req in body.get("requests", []):
                rep = req.get("repeatCell", {})
                rng = rep.get("range", {})
                color = (((rep.get("cell") or {}).get("userEnteredFormat") or {}).get("backgroundColor") or {})
                painted.append((rng.get("startRowIndex"), rng.get("startColumnIndex"), color))

        self.assertIn((2, 1, sync_swaps_to_sheets.WHITE), painted)

    def test_sync_swaps_manual_multi_sheet_sync_runs_grid_reconcile_for_each_target(self):
        ws_mc = _FakeGridWorksheet(title="MC (OA and GOAs)", grid=[[""]], bg_grid=[[None]], sheet_id=1)
        ws_oncall = _FakeGridWorksheet(title="On Call 4/12 - 4/18", grid=[[""]], bg_grid=[[None]], sheet_id=2)
        ss = _FakeWorkbook([ws_mc, ws_oncall])

        with patch.object(sync_swaps_to_sheets, "la_today", return_value=date(2026, 4, 14)), patch.object(
            sync_swaps_to_sheets, "_ensure_fixed_headers", return_value=(SimpleNamespace(), SimpleNamespace())
        ), patch.object(sync_swaps_to_sheets, "_query_all_from", return_value=[]), patch.object(
            sync_swaps_to_sheets, "_upsert_section", return_value=0
        ), patch.object(sync_swaps_to_sheets, "_apply_grid_colors", return_value=[]) as apply_grid:
            res = sync_swaps_to_sheets.sync_swaps_to_sheets(
                ss,
                supabase=object(),
                apply_grid_colors=True,
            )

        self.assertEqual(apply_grid.call_count, 2)
        self.assertEqual(sorted(res["sheets_updated"]), ["MC (OA and GOAs)", "On Call 4/12 - 4/18"])
        self.assertEqual(res["grid_errors"], [])


class PeekUiTests(unittest.TestCase):
    def test_df_from_grid_deduplicates_blank_and_repeated_headers(self):
        df = ui_peek._df_from_grid(
            [
                ["", "Friday", "Friday", ""],
                ["7:00 PM", "Alex Smith", "Taylor Jones", "Open"],
            ]
        )

        self.assertEqual(list(df.columns), ["Column 1", "Friday", "Friday (2)", "Column 4"])
        self.assertEqual(df.iloc[0].tolist(), ["7:00 PM", "Alex Smith", "Taylor Jones", "Open"])

    def test_unique_display_headers_avoids_suffix_collisions(self):
        headers = ["Friday", "Friday", "Friday (2)", "Friday"]

        self.assertEqual(
            ui_peek._unique_display_headers(headers),
            ["Friday", "Friday (2)", "Friday (2) (2)", "Friday (3)"],
        )

    def test_df_from_grid_uses_fallback_column_names_when_no_header_row_exists(self):
        df = ui_peek._df_from_grid(
            [
                ["Alex Smith", "Open"],
                ["Taylor Jones", ""],
            ]
        )

        self.assertEqual(list(df.columns), ["Column 1", "Column 2"])
        self.assertEqual(df.iloc[0].tolist(), ["Alex Smith", "Open"])
        self.assertEqual(df.iloc[1].tolist(), ["Taylor Jones", ""])


if __name__ == "__main__":
    unittest.main()

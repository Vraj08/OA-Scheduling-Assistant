import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from oa_app.services import callouts_db, pickups_db
from oa_app.ui import schedule_query


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


if __name__ == "__main__":
    unittest.main()

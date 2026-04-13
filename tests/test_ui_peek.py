import unittest

from oa_app.ui import ui_peek


class UiPeekDataframeTests(unittest.TestCase):
    def test_duplicate_and_blank_headers_are_made_unique(self):
        grid = [
            ["", "Monday", "Monday", ""],
            ["7:00 PM - 12:00 AM", "Alex", "Jamie", "Notes"],
        ]

        df = ui_peek._df_from_grid(grid)

        self.assertEqual(
            list(df.columns),
            ["Column 1", "Monday", "Monday (2)", "Column 4"],
        )

    def test_no_header_grid_gets_fallback_column_names(self):
        grid = [
            ["", ""],
            ["Alex", "UNH"],
            ["Jamie"],
        ]

        df = ui_peek._df_from_grid(grid)

        self.assertEqual(list(df.columns), ["Column 1", "Column 2"])
        self.assertEqual(df.iloc[2, 1], "")

"""Unit tests for PMC / spend helpers (no Streamlit runtime required beyond import)."""

from __future__ import annotations

import pandas as pd
import pytest

import oracle_app as oa


class TestPmcAlignChannelLabelForXrayPivot:
    def test_google_ads_maps_to_google_search(self) -> None:
        assert oa._pmc_align_channel_label_for_xray_pivot("Google Ads") == "Google Search"

    def test_google_search_unchanged(self) -> None:
        assert oa._pmc_align_channel_label_for_xray_pivot("Google Search") == "Google Search"

    def test_youtube_not_folded_into_search(self) -> None:
        assert oa._pmc_align_channel_label_for_xray_pivot("YouTube") == "YouTube"

    def test_pmax_aliases(self) -> None:
        assert oa._pmc_align_channel_label_for_xray_pivot("Performance Max") == "PMax"
        assert oa._pmc_align_channel_label_for_xray_pivot("PMax") == "PMax"

    def test_blank_passthrough(self) -> None:
        assert oa._pmc_align_channel_label_for_xray_pivot("") == ""
        assert oa._pmc_align_channel_label_for_xray_pivot("(blank)") == "(blank)"

    def test_google_organic_not_rewritten_to_search(self) -> None:
        assert oa._pmc_align_channel_label_for_xray_pivot("Google Organic") == "Google Organic"


class TestSpendSheetPivotByMonthChannel:
    def test_aggregates_google_rows(self) -> None:
        df = pd.DataFrame(
            {
                "month": ["2026-03", "2026-03"],
                "country": ["UAE", "UAE"],
                "channel": ["", ""],
                "platform": ["Google Ads", "Google Ads"],
                "cost": [4000.0, 2218.93],
                "clicks": [100, 50],
                "impressions": [1000, 500],
            }
        )
        out = oa._spend_sheet_pivot_by_month_channel(df)
        assert not out.empty
        row = out.loc[out["country"].astype(str).str.contains("Google", case=False, na=False)]
        assert len(row) == 1
        assert float(row["cost"].iloc[0]) == pytest.approx(6218.93, rel=1e-9)

    def test_keeps_distinct_channels(self) -> None:
        df = pd.DataFrame(
            {
                "month": ["2026-03", "2026-03"],
                "country": ["UAE", "UAE"],
                "channel": ["LinkedIn", "Meta"],
                "platform": ["", ""],
                "cost": [100.0, 200.0],
                "clicks": [0, 0],
                "impressions": [0, 0],
            }
        )
        out = oa._spend_sheet_pivot_by_month_channel(df)
        assert len(out) == 2
        by_ch = out.set_index("country")["cost"].to_dict()
        assert by_ch.get("LinkedIn", 0) == pytest.approx(100.0)
        assert by_ch.get("Meta", 0) == pytest.approx(200.0)

    def test_paid_media_placeholder_falls_back_to_platform(self) -> None:
        """Blend used to set channel='Paid media' for unknowns; pivot must still split by tab/platform."""
        df = pd.DataFrame(
            {
                "month": ["2026-03", "2026-03"],
                "country": ["UAE", "UAE"],
                "channel": ["Paid media", "Paid media"],
                "platform": ["Google Ads", "Meta Ads"],
                "source_tab": ["Google Ads", "Meta Ads"],
                "cost": [100.0, 200.0],
                "clicks": [0, 0],
                "impressions": [0, 0],
            }
        )
        out = oa._spend_sheet_pivot_by_month_channel(df)
        assert len(out) == 2
        by_ch = out.set_index("country")["cost"].to_dict()
        assert by_ch.get("Google Search", 0) == pytest.approx(100.0)
        assert by_ch.get("Meta Ads", 0) == pytest.approx(200.0)

    def test_row_level_platform_splits_rows_on_same_tab(self) -> None:
        """Same ``source_tab`` but different sheet **Platform** cells → distinct pivot channels."""
        df = pd.DataFrame(
            {
                "month": ["2026-03", "2026-03"],
                "country": ["UAE", "UAE"],
                "channel": ["", ""],
                "platform": ["Performance Max", "Google Ads"],
                "source_tab": ["Google Ads", "Google Ads"],
                "cost": [50.0, 150.0],
                "clicks": [0, 0],
                "impressions": [0, 0],
            }
        )
        blended = oa._mpo_blend_paid_media_for_master_df(df)
        out = oa._spend_sheet_pivot_by_month_channel(blended)
        assert len(out) == 2
        by_ch = out.set_index("country")["cost"].to_dict()
        assert by_ch.get("PMax", 0) == pytest.approx(50.0)
        assert by_ch.get("Google Search", 0) == pytest.approx(150.0)

    def test_unified_channel_column_wins_over_channel(self) -> None:
        df = pd.DataFrame(
            {
                "month": ["2026-03", "2026-03"],
                "country": ["UAE", "UAE"],
                "Unified Channel": ["Organic", "Meta"],
                "channel": ["Paid Search", "Paid Social"],
                "platform": ["Google Ads", "Meta Ads"],
                "source_tab": ["Spend", "Spend"],
                "cost": [10.0, 20.0],
                "clicks": [0, 0],
                "impressions": [0, 0],
            }
        )
        out = oa._spend_sheet_pivot_by_month_channel(df)
        assert len(out) == 2
        by_ch = out.set_index("country")["cost"].to_dict()
        assert by_ch.get("Organic", 0) == pytest.approx(10.0)
        assert by_ch.get("Meta", 0) == pytest.approx(20.0)


class TestMonthNormKey:
    def test_march_2026_string(self) -> None:
        assert oa._month_norm_key("2026-03") == "2026-03"

    def test_period_string(self) -> None:
        assert oa._month_norm_key(pd.Period("2026-03", freq="M")) == "2026-03"


class TestFilterSpendForDashboard:
    def test_prefers_month_in_window(self) -> None:
        from datetime import date

        df = pd.DataFrame(
            {
                "month": ["2026-03", "2025-01"],
                "cost": [100.0, 999.0],
                "date": [pd.NaT, pd.NaT],
            }
        )
        out = oa._filter_spend_for_dashboard(df, date(2026, 3, 1), date(2026, 3, 31))
        assert len(out) == 1
        assert float(out["cost"].iloc[0]) == pytest.approx(100.0)

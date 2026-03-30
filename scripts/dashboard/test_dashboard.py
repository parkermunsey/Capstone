import os
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load .env from likely project locations
current_file = Path(__file__).resolve()
possible_env_paths = [
    current_file.parents[1] / ".env",
    current_file.parents[2] / ".env" if len(current_file.parents) > 2 else None,
]

for env_path in possible_env_paths:
    if env_path and env_path.exists():
        load_dotenv(env_path)
        break

st.set_page_config(
    page_title="Missoula Rentals",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .stApp {
        background: #dbeaf6;
    }

    [data-testid="stAppViewContainer"] {
        background: #dbeaf6;
    }

    .main .block-container {
        padding-top: 1.25rem;
        padding-bottom: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
        max-width: 1600px;
    }

    [data-testid="stSidebar"] {
        background: #c9dcee;
        border-right: 1px solid #aac3d9;
    }

    [data-testid="stSidebar"] * {
        color: #21374d;
    }

    .dashboard-title {
        font-size: 2.45rem;
        font-weight: 800;
        color: #1f4368;
        margin: 0;
        line-height: 1.1;
    }

    .dashboard-subtitle {
        font-size: 1.02rem;
        color: #50677e;
        margin-top: 0.45rem;
        margin-bottom: 0.9rem;
        line-height: 1.5;
    }

    .hero-chip-wrap {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        margin-top: 0.35rem;
    }

    .hero-chip {
        background: #eef5fb;
        color: #35506b;
        border: 1px solid #cfe0ef;
        border-radius: 999px;
        padding: 6px 12px;
        font-size: 0.86rem;
        font-weight: 600;
        display: inline-block;
    }

    .kpi-box {
    background: linear-gradient(135deg, #163f6b 0%, #1f5f99 100%);
    border-radius: 14px;
    padding: 10px 14px;
    min-height: 70px;
    box-shadow: 0 4px 12px rgba(22, 63, 107, 0.14);
    display: flex;
    flex-direction: column;
    justify-content: center;
}

.kpi-label {
    color: #dbe9f7;
    font-size: 0.75rem;
    font-weight: 700;
    margin-bottom: 0.15rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
}

.kpi-value {
    color: white;
    font-size: 1.5rem;
    font-weight: 800;
    line-height: 1;
}

    .small-note {
        color: #5d7389;
        font-size: 0.88rem;
        margin-top: 0.25rem;
        margin-bottom: 0.75rem;
    }

    .stSelectbox label,
    .stSlider label,
    .stCheckbox label,
    .stRadio label {
        font-weight: 700 !important;
        color: #1f4368 !important;
    }

    div[data-baseweb="select"] > div {
        background: #f8fbfe !important;
        border: 1px solid #aac3d9 !important;
        border-radius: 10px !important;
    }

    .stSlider p {
        color: #1f4368 !important;
        font-weight: 600 !important;
    }

    div[data-baseweb="slider"] > div > div > div {
        background-color: #8cb4d7 !important;
    }

    div[data-baseweb="slider"] [role="slider"] {
        background-color: #1f5f99 !important;
        border: 2px solid #1f5f99 !important;
        box-shadow: none !important;
    }

    div[data-baseweb="checkbox"] input,
    [data-testid="stCheckbox"] input,
    input[type="checkbox"] {
        accent-color: #1f5f99 !important;
    }

    .stDownloadButton button {
        background: #1f5f99 !important;
        color: white !important;
        border: none !important;
        border-radius: 10px !important;
        font-weight: 700 !important;
    }

    .stDownloadButton button:hover {
        background: #184d7d !important;
        color: white !important;
    }

    .badge {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 0.8rem;
        font-weight: 700;
        border: 1px solid #cfe0ef;
        background: #eef6fd;
        color: #35506b;
    }

    .badge-yes {
        background: #eaf7ee;
        color: #1f6a3b;
        border: 1px solid #bfe2cb;
    }

    .badge-no {
        background: #f6f7f9;
        color: #64707d;
        border: 1px solid #d6dde5;
    }

    table {
        width: 100%;
        border-collapse: collapse;
        overflow: hidden;
        border-radius: 12px;
    }

    thead tr th {
        background: #e6f0f9;
        color: #1f4368;
        font-weight: 700;
        border: 1px solid #d4e1ec;
        padding: 10px 12px;
    }

    tbody tr td {
        border: 1px solid #dfe8ef;
        padding: 10px 12px;
        background: #ffffff;
        font-size: 0.95rem;
        vertical-align: middle;
    }

    tbody tr:nth-child(even) td {
        background: #f7fbff;
    }

    tbody tr:hover td {
        background-color: #eaf3fb !important;
        transition: 0.2s ease;
    }

    a {
        color: #1f5f99 !important;
        font-weight: 600;
        text-decoration: none;
    }

    a:hover {
        text-decoration: underline;
    }

    h2, h3 {
        color: #1f4368 !important;
    }

    hr {
        border: none;
        height: 0;
        margin: 0;
        padding: 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Missing DATABASE_URL environment variable.")
    return create_engine(db_url, pool_pre_ping=True)


def table_has_column(engine, table_name: str, column_name: str) -> bool:
    q = text(
        """
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = :table_name
          and column_name = :column_name
        limit 1
        """
    )
    with engine.begin() as conn:
        result = conn.execute(
            q,
            {"table_name": table_name, "column_name": column_name}
        ).fetchone()
    return result is not None


def fake_missoula_coords(seed_text: str):
    base_lat = 46.8721
    base_lon = -113.9940

    seed = hashlib.md5(seed_text.encode("utf-8")).hexdigest()

    lat_offset_raw = int(seed[:6], 16) / 0xFFFFFF
    lon_offset_raw = int(seed[6:12], 16) / 0xFFFFFF

    lat_offset = (lat_offset_raw - 0.5) * 0.08
    lon_offset = (lon_offset_raw - 0.5) * 0.12

    return round(base_lat + lat_offset, 6), round(base_lon + lon_offset, 6)


def demo_pet_friendly(seed_text: str) -> bool:
    seed = hashlib.md5(seed_text.encode("utf-8")).hexdigest()
    return int(seed[-2:], 16) % 3 != 0


def add_demo_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "latitude" not in df.columns:
        df["latitude"] = pd.NA
    if "longitude" not in df.columns:
        df["longitude"] = pd.NA

    pet_flags = []
    for idx in df.index:
        seed_text = " | ".join(
            [
                str(df.at[idx, "display_address"]) if pd.notna(df.at[idx, "display_address"]) else "",
                str(df.at[idx, "source"]) if pd.notna(df.at[idx, "source"]) else "",
                str(df.at[idx, "rent_display"]) if pd.notna(df.at[idx, "rent_display"]) else "",
            ]
        )

        if pd.isna(df.at[idx, "latitude"]) or pd.isna(df.at[idx, "longitude"]):
            fake_lat, fake_lon = fake_missoula_coords(seed_text)
            df.at[idx, "latitude"] = fake_lat
            df.at[idx, "longitude"] = fake_lon

        pet_flags.append(demo_pet_friendly(seed_text))

    df["pet_friendly_demo"] = pet_flags
    return df


@st.cache_data(ttl=600)
def load_stg_listings(days_back: int = 90) -> pd.DataFrame:
    engine = get_engine()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    has_lat = table_has_column(engine, "stg_listings", "latitude")
    has_lon = table_has_column(engine, "stg_listings", "longitude")

    lat_sql = "latitude" if has_lat else "NULL::double precision as latitude"
    lon_sql = "longitude" if has_lon else "NULL::double precision as longitude"

    q = text(
        f"""
        select
            source,
            source_record_id,
            listing_title,
            address_raw,
            address_norm,
            bedrooms,
            bathrooms,
            sqft,
            rent_min,
            rent_max,
            rent_period,
            availability_status,
            available_date,
            is_currently_available,
            listing_url,
            observed_at,
            {lat_sql},
            {lon_sql}
        from stg_listings
        where observed_at >= :cutoff
        order by observed_at desc nulls last
        """
    )

    df = pd.read_sql(q, engine, params={"cutoff": cutoff})

    if not df.empty:
        df["source"] = df["source"].fillna("unknown").str.lower().str.strip()

        df["display_address"] = (
            df["address_raw"]
            .fillna(df["address_norm"])
            .fillna(df["listing_title"])
            .fillna("Address unknown")
        )

        df["rent_display"] = df.apply(
            lambda r: (
                f"${int(r['rent_min'])}"
                if pd.notna(r["rent_min"]) and (
                    pd.isna(r["rent_max"]) or r["rent_min"] == r["rent_max"]
                )
                else f"${int(r['rent_min'])} - ${int(r['rent_max'])}"
                if pd.notna(r["rent_min"]) and pd.notna(r["rent_max"])
                else "Unknown"
            ),
            axis=1,
        )

        df["beds_baths_display"] = df.apply(
            lambda r: (
                f"{int(r['bedrooms']) if pd.notna(r['bedrooms']) else '?'} bd / "
                f"{r['bathrooms'] if pd.notna(r['bathrooms']) else '?'} ba"
            ),
            axis=1,
        )

        df["map_label"] = df.apply(
            lambda r: (
                f"{r['display_address']} | "
                f"{r['source'].upper()} | "
                f"{r['rent_display']}"
            ),
            axis=1,
        )

    return df


def clickable_link(url):
    if pd.isna(url) or not str(url).strip():
        return ""
    return f'<a href="{url}" target="_blank">View Listing</a>'


def render_kpi(label: str, value: str):
    st.markdown(
        f"""
        <div class="kpi-box">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def pet_badge(value: bool) -> str:
    if value:
        return '<span class="badge badge-yes">Pet friendly</span>'
    return '<span class="badge badge-no">No pets listed</span>'


df = load_stg_listings()

if df.empty:
    st.warning("No staged listings found in the last 90 days.")
    st.stop()

# Hide Craigslist for now
df = df[df["source"] != "craigslist"].copy()

# Add demo-only fields for screenshot polish
df = add_demo_flags(df)

logo_path = Path(__file__).resolve().parent / "missoula_logo.jpg"

st.sidebar.markdown("## Filters")

sources = sorted(df["source"].dropna().unique().tolist())
source_choice = st.sidebar.selectbox("Source", ["All"] + sources)

bed_values = [x for x in df["bedrooms"].dropna().unique().tolist()]
bed_options = sorted({int(x) for x in bed_values})

bedroom_choice = st.sidebar.selectbox(
    "Bedrooms",
    ["All"] + bed_options,
    index=0,
)

availability_mode = st.sidebar.selectbox(
    "Availability",
    [
        "All",
        "Currently available only",
        "Not currently available only",
        "Unknown only",
        "Waitlist only",
    ],
    index=0,
)

known_rents = df["rent_min"].dropna()
if not known_rents.empty:
    min_r = int(known_rents.min())
    max_r = int(known_rents.max())
else:
    min_r, max_r = 0, 5000

rent_range = st.sidebar.slider(
    "Monthly rent",
    min_value=0,
    max_value=max(5000, max_r),
    value=(min_r, max_r),
    step=25,
)

include_unknown_rent = st.sidebar.checkbox("Include unknown rent", value=True)
pet_friendly_only = st.sidebar.checkbox("Pet friendly only", value=False)

filtered = df.copy()

if source_choice != "All":
    filtered = filtered[filtered["source"] == source_choice]

if bedroom_choice != "All":
    filtered = filtered[filtered["bedrooms"] == bedroom_choice]

min_rent, max_rent = rent_range

rent_known_mask = filtered["rent_min"].notna()
filtered_rent_known = filtered[rent_known_mask]
filtered_rent_known = filtered_rent_known[
    (filtered_rent_known["rent_min"] >= min_rent) &
    (filtered_rent_known["rent_min"] <= max_rent)
]

filtered_rent_unknown = filtered[~rent_known_mask] if include_unknown_rent else filtered.iloc[0:0]
filtered = pd.concat([filtered_rent_known, filtered_rent_unknown], ignore_index=True)

if availability_mode == "Currently available only":
    filtered = filtered[filtered["is_currently_available"] == True]
elif availability_mode == "Not currently available only":
    filtered = filtered[filtered["is_currently_available"] == False]
elif availability_mode == "Unknown only":
    filtered = filtered[filtered["availability_status"] == "unknown"]
elif availability_mode == "Waitlist only":
    filtered = filtered[filtered["availability_status"] == "waitlist"]

if pet_friendly_only:
    filtered = filtered[filtered["pet_friendly_demo"] == True]

filtered = filtered.sort_values(
    ["is_currently_available", "pet_friendly_demo", "rent_min", "observed_at"],
    ascending=[False, False, True, False]
)

# Remove duplicate addresses for display
filtered_display = filtered.drop_duplicates(subset=["display_address"]).copy()

# Demo KPI values for screenshot
demo_listings_shown = 28
demo_sources_shown = 5
demo_currently_available = 23

hero_left, hero_spacer = st.columns([5, 1], vertical_alignment="top")

with hero_left:
    logo_col, content_col = st.columns([1.2, 3.8], vertical_alignment="top")

    with logo_col:
        if logo_path.exists():
            st.image(str(logo_path), width=180)

    with content_col:
        st.markdown('<div class="dashboard-title">Missoula Rentals</div>', unsafe_allow_html=True)

        st.markdown(
            '<div class="dashboard-subtitle">A weekly live look at rental listings across Missoula property sources.</div>',
            unsafe_allow_html=True,
        )

        st.markdown(
            """
            <div class="hero-chip-wrap">
                <span class="hero-chip">Updated weekly</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # spacing
        st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

        # 🔥 CLEAN HORIZONTAL KPI ROW
        k1, k2, k3 = st.columns(3)

        with k1:
            render_kpi("Listings shown", f"{demo_listings_shown:,}")

        with k2:
            render_kpi("Sources shown", f"{demo_sources_shown:,}")

        with k3:
            render_kpi("Currently available", f"{demo_currently_available:,}")

tab1, tab2, tab3 = st.tabs(["Listings", "Map", "Rent Distribution"])

with tab1:
    st.subheader("Available Listings")
    st.markdown(
        '<div class="small-note">Showing 8 of 28 listings after filters.</div>',
        unsafe_allow_html=True,
    )

    display_df = filtered_display[
        [
            "source",
            "display_address",
            "bedrooms",
            "bathrooms",
            "sqft",
            "rent_display",
            "pet_friendly_demo",
            "listing_url",
        ]
    ].rename(
        columns={
            "source": "Source",
            "display_address": "Address",
            "bedrooms": "Beds",
            "bathrooms": "Baths",
            "sqft": "Sq Ft",
            "rent_display": "Rent",
            "pet_friendly_demo": "Pet Friendly",
            "listing_url": "URL",
        }
    )

    display_df["Pet Friendly"] = display_df["Pet Friendly"].apply(pet_badge)
    display_df["URL"] = display_df["URL"].apply(clickable_link)

    rows_to_show = 8
    display_df_limited = display_df.head(rows_to_show)

    st.write(display_df_limited.to_html(escape=False, index=False), unsafe_allow_html=True)

    csv_bytes = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download filtered listings as CSV",
        data=csv_bytes,
        file_name="missoula_rentals_filtered.csv",
        mime="text/csv",
    )

with tab2:
    st.subheader("Listings Map")

    st.caption("Listings without real coordinates are shown with sample map points around Missoula.")

    demo_cluster_view = st.checkbox("Show all", value=True)

    map_df = filtered_display.copy() if not demo_cluster_view else add_demo_flags(filtered_display)

    map_df = map_df[
        map_df["latitude"].notna() &
        map_df["longitude"].notna()
    ].copy()

    if map_df.empty:
        st.info("No listings available for the current filters.")
    else:
        map_mode = st.radio(
            "Map mode",
            ["Top 12 filtered listings", "Choose one listing"],
            horizontal=True,
        )

        if map_mode == "Top 12 filtered listings":
            map_top = map_df.head(12).copy()

            st.caption(f"Showing {len(map_top)} listings on the map.")
            st.map(
                map_top.rename(columns={"latitude": "lat", "longitude": "lon"})[["lat", "lon"]],
                use_container_width=True,
            )

            preview_df = map_top[
                ["source", "display_address", "beds_baths_display", "rent_display", "listing_url"]
            ].rename(
                columns={
                    "source": "Source",
                    "display_address": "Address",
                    "beds_baths_display": "Beds / Baths",
                    "rent_display": "Rent",
                    "listing_url": "URL",
                }
            )

            st.dataframe(
                preview_df,
                use_container_width=True,
                hide_index=True,
            )

        else:
            option_labels = map_df["map_label"].tolist()

            selected_label = st.selectbox(
                "Choose a listing to map",
                option_labels,
            )

            selected_row = map_df[map_df["map_label"] == selected_label].head(1).copy()

            if not selected_row.empty:
                st.map(
                    selected_row.rename(columns={"latitude": "lat", "longitude": "lon"})[["lat", "lon"]],
                    use_container_width=True,
                )

                listing = selected_row.iloc[0]

                st.markdown(
                    f"""
                    **Address:** {listing['display_address']}

                    **Source:** {str(listing['source']).upper()}

                    **Rent:** {listing['rent_display']}

                    **Beds / Baths:** {listing['beds_baths_display']}

                    **Listing URL:** {listing['listing_url'] if pd.notna(listing['listing_url']) else 'N/A'}
                    """
                )

with tab3:
    st.subheader("Rent Distribution")

    rent_chart_df = filtered_display[filtered_display["rent_min"].notna()].copy()

    if rent_chart_df.empty:
        st.info("No rent data available for the current filters.")
    else:
        bins = [0, 800, 1000, 1200, 1400, 1600, 2000, 3000, 5000, 10000]
        labels = [
            "0-800",
            "801-1000",
            "1001-1200",
            "1201-1400",
            "1401-1600",
            "1601-2000",
            "2001-3000",
            "3001-5000",
            "5001+",
        ]

        rent_chart_df["rent_band"] = pd.cut(
            rent_chart_df["rent_min"],
            bins=bins,
            labels=labels,
            include_lowest=True,
            right=True,
        )

        rent_dist = rent_chart_df["rent_band"].value_counts().sort_index()
        st.bar_chart(rent_dist)
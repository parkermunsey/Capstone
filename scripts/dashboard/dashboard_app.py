import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import re
import pandas as pd
import requests
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

CURRENT_FILE = Path(__file__).resolve()
POSSIBLE_ENV_PATHS = [
    CURRENT_FILE.parent / ".env",
    CURRENT_FILE.parents[1] / ".env" if len(CURRENT_FILE.parents) > 1 else None,
    CURRENT_FILE.parents[2] / ".env" if len(CURRENT_FILE.parents) > 2 else None,
]

for env_path in POSSIBLE_ENV_PATHS:
    if env_path and env_path.exists():
        load_dotenv(env_path)
        break


def get_engine():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set in your .env file.")
    return create_engine(database_url, pool_pre_ping=True)


def insert_listing_flag(
    source: str,
    source_record_id: str,
    listing_fingerprint: str | None,
    cross_source_fingerprint: str | None,
    address: str | None,
    listing_title: str | None,
    listing_url: str | None,
    flag_reason: str,
    flag_notes: str | None = None,
    flag_scope: str = "listing",
    flagged_by: str | None = None,
):
    engine = get_engine()

    sql = text(
        """
        insert into listing_flags (
            source,
            source_record_id,
            listing_fingerprint,
            cross_source_fingerprint,
            address,
            listing_title,
            listing_url,
            flag_reason,
            flag_notes,
            flag_scope,
            flagged_by
        )
        values (
            :source,
            :source_record_id,
            :listing_fingerprint,
            :cross_source_fingerprint,
            :address,
            :listing_title,
            :listing_url,
            :flag_reason,
            :flag_notes,
            :flag_scope,
            :flagged_by
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "source": source,
                "source_record_id": source_record_id,
                "listing_fingerprint": listing_fingerprint,
                "cross_source_fingerprint": cross_source_fingerprint,
                "address": address,
                "listing_title": listing_title,
                "listing_url": listing_url,
                "flag_reason": flag_reason,
                "flag_notes": flag_notes,
                "flag_scope": flag_scope,
                "flagged_by": flagged_by,
            },
        )


st.set_page_config(
    page_title="Missoula Rental Dashboard",
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
        padding-top: 2.5rem;
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
    .stCheckbox label {
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
    </style>
    """,
    unsafe_allow_html=True,
)


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


def clickable_link(url):
    if pd.isna(url) or not str(url).strip():
        return ""
    return f'<a href="{url}" target="_blank">View Listing</a>'


def format_rent_label(row):
    if pd.notna(row.get("dashboard_rent_label")) and str(row["dashboard_rent_label"]).strip():
        return str(row["dashboard_rent_label"])

    rent_min = row.get("rent_min")
    rent_max = row.get("rent_max")

    if pd.notna(rent_min) and pd.notna(rent_max):
        if int(rent_min) == int(rent_max):
            return f"${int(rent_min):,}"
        return f"${int(rent_min):,} - ${int(rent_max):,}"

    if pd.notna(rent_min):
        return f"${int(rent_min):,}"

    if pd.notna(rent_max):
        return f"${int(rent_max):,}"

    return "Unknown"


def format_card_meta(row):
    beds = "?" if pd.isna(row.get("bedrooms")) else int(float(row["bedrooms"]))
    baths = "?" if pd.isna(row.get("bathrooms")) else int(float(row["bathrooms"]))
    sqft = "Unknown" if pd.isna(row.get("sqft")) else f"{int(float(row['sqft'])):,}"
    rent = row.get("rent_display", "Unknown")
    return f"{beds} bd • {baths} ba • {sqft} sq ft • {rent}"


def format_beds_baths(row):
    beds = "?" if pd.isna(row.get("bedrooms")) else int(row["bedrooms"])
    baths = "?" if pd.isna(row.get("bathrooms")) else int(row["bathrooms"])
    return f"{beds} bd / {baths} ba"


def format_availability_bucket(row):
    if bool(row.get("is_currently_available")):
        return "🟢 Available Now"
    if bool(row.get("is_available_soon")):
        return "🟡 Available Soon"
    return "⚪ Other"


@st.cache_data(ttl=600)
def load_dashboard_listings(days_back: int = 90) -> pd.DataFrame:
    engine = get_engine()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    query = text(
        """
        select
            source,
            source_record_id,
            listing_title,
            address,
            address_raw,
            unit,
            city,
            state,
            postal_code,
            bedrooms,
            bathrooms,
            sqft,
            rent_min,
            rent_max,
            rent_period,
            dashboard_rent,
            dashboard_rent_label,
            availability_status,
            available_date,
            availability_text_raw,
            is_currently_available,
            is_available_soon,
            contact_name,
            contact_phone,
            contact_email,
            listing_url,
            observed_at,
            listing_fingerprint,
            cross_source_fingerprint,
            dashboard_group_key,
            duplicate_count,
            latitude,
            longitude
        from dashboard_ready_listings
        where observed_at >= :cutoff
        order by observed_at desc nulls last
        """
    )

    df = pd.read_sql(query, engine, params={"cutoff": cutoff})

    if df.empty:
        return df

    df["source"] = df["source"].fillna("unknown").astype(str).str.lower().str.strip()
    df["address"] = (
        df["address"]
        .fillna(df["address_raw"])
        .fillna(df["listing_title"])
        .fillna("Address unknown")
    )

    df["rent_display"] = df.apply(format_rent_label, axis=1)
    df["beds_baths_display"] = df.apply(format_beds_baths, axis=1)

    return df


def clean_address_for_geocoding(address: str) -> str | None:
    if pd.isna(address) or not str(address).strip():
        return None

    query = str(address).strip()

    query = query.replace("~", " ")
    query = query.replace("–", " ")
    query = query.replace("-", " ")

    # Remove promos and everything after them
    query = re.sub(r"\$[0-9,]+\s*rent\s*credit.*$", "", query, flags=re.IGNORECASE)
    query = re.sub(r"\b\d+\s*weeks?\s*free\s*rent.*$", "", query, flags=re.IGNORECASE)
    query = re.sub(r"\bearly\s*term(?:ination|inaiton)?!?.*$", "", query, flags=re.IGNORECASE)

    # Remove unit text. Geocoder usually does better without apartment/unit numbers.
    query = re.sub(r",?\s*(unit|apt|apartment|#)\s*[A-Za-z0-9\-]+", "", query, flags=re.IGNORECASE)

    # Remove rental descriptors
    query = re.sub(r"\b\d+\s*(bed|bedroom|bedrooms|bath|bathroom|bathrooms|br|bd)\b.*$", "", query, flags=re.IGNORECASE)
    query = re.sub(r"\b(studios?|studio|efficiency)\b.*$", "", query, flags=re.IGNORECASE)

    query = re.sub(r"\s+", " ", query).strip(" ,.-!")

    if query.lower() in {"address unknown", "unknown", "0"}:
        return None

    known_cities = ["missoula", "polson", "florence", "milltown", "stevensville", "hamilton"]

    if not any(city in query.lower() for city in known_cities):
        query = f"{query}, Missoula, MT"
    elif "mt" not in query.lower():
        query = f"{query}, MT"

    return query

@st.cache_data(ttl=86400, show_spinner=False)
def geocode_address(address: str):
    query = clean_address_for_geocoding(address)

    if not query:
        return None

    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": query,
                "format": "jsonv2",
                "limit": 1,
                "countrycodes": "us",
            },
            headers={
                "User-Agent": "UMCapstoneHousing/0.1 academic dashboard geocoding"
            },
            timeout=20,
        )
        response.raise_for_status()
        results = response.json()

        if not results:
            return None

        return {
            "latitude": float(results[0]["lat"]),
            "longitude": float(results[0]["lon"]),
        }

    except Exception:
        return None


def add_geocoded_coordinates(source_df: pd.DataFrame, max_rows: int = 75) -> pd.DataFrame:
    if source_df.empty:
        return source_df

    out = source_df.copy()
    out = out.head(max_rows)

    for idx, row in out.iterrows():
        if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
            continue

        geo = geocode_address(row.get("address"))

        if geo:
            out.at[idx, "latitude"] = geo["latitude"]
            out.at[idx, "longitude"] = geo["longitude"]

    return out[out["latitude"].notna() & out["longitude"].notna()].copy()


def render_map(map_df: pd.DataFrame, label: str):
    if map_df.empty:
        st.info(f"No {label} with map coordinates available.")
        return

    map_df = map_df.copy()
    map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
    map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
    map_df = map_df.dropna(subset=["latitude", "longitude"])

    if map_df.empty:
        st.info(f"No {label} with valid map coordinates available.")
        return

    map_df["listing_title"] = map_df["listing_title"].fillna("Unknown listing").astype(str)
    map_df["rent_display"] = map_df["rent_display"].fillna("Unknown").astype(str)
    map_df["bedrooms"] = map_df["bedrooms"].fillna("Unknown").astype(str)
    map_df["source"] = map_df["source"].fillna("Unknown").astype(str)

    map_plot_df = map_df[
        ["listing_title", "rent_display", "bedrooms", "source", "latitude", "longitude"]
    ].copy()

    st.markdown(
        f'<div class="small-note">Showing {len(map_plot_df):,} {label}.</div>',
        unsafe_allow_html=True,
    )

    import pydeck as pdk

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_plot_df,
        get_position="[longitude, latitude]",
        get_radius=100,
        get_fill_color=[31, 95, 153, 180],
        pickable=True,
    )

    view_state = pdk.ViewState(
        latitude=46.87,
        longitude=-114.0,
        zoom=11,
    )

    tooltip = {
        "html": "<b>{listing_title}</b><br/>Rent: {rent_display}<br/>Beds: {bedrooms}<br/>Source: {source}",
        "style": {"backgroundColor": "white", "color": "black"},
    }

    st.pydeck_chart(
        pdk.Deck(
            map_style="light",
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
        ),
        use_container_width=True,
    )


df = load_dashboard_listings()

if df.empty:
    st.warning("No dashboard-ready listings were found in the last 90 days.")
    st.stop()

st.sidebar.markdown("## Filters")

sources = sorted(df["source"].dropna().unique().tolist())
main_sources = [s for s in sources if s != "craigslist"]

st.sidebar.markdown("**Source**")
source_choices = []

for source in main_sources:
    if st.sidebar.checkbox(source, value=True, key=f"source_{source}"):
        source_choices.append(source)

bed_values = [x for x in df["bedrooms"].dropna().unique().tolist()]
bed_options = sorted({int(x) for x in bed_values})

bedroom_choice = st.sidebar.selectbox(
    "Bedrooms",
    ["All"] + bed_options,
    index=0,
)

availability_view = st.sidebar.selectbox(
    "Availability window",
    ["Available Now", "Available Soon", "Now + Soon"],
    index=2,
)

known_rents = pd.concat([df["rent_min"], df["rent_max"]], axis=0).dropna()
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

main_df = df[df["source"] != "craigslist"].copy()
craigslist_df = df[df["source"] == "craigslist"].copy()

if source_choices:
    filtered = main_df[main_df["source"].isin(source_choices)].copy()
else:
    filtered = main_df.iloc[0:0].copy()

if bedroom_choice != "All":
    filtered = filtered[filtered["bedrooms"] == bedroom_choice]

min_rent, max_rent = rent_range

rent_known_mask = filtered["dashboard_rent"].notna()
filtered_rent_known = filtered[rent_known_mask]
filtered_rent_known = filtered_rent_known[
    (filtered_rent_known["dashboard_rent"] >= min_rent)
    & (filtered_rent_known["dashboard_rent"] <= max_rent)
]

filtered_rent_unknown = filtered[~rent_known_mask] if include_unknown_rent else filtered.iloc[0:0]
filtered = pd.concat([filtered_rent_known, filtered_rent_unknown], ignore_index=True)

if availability_view == "Available Now":
    filtered = filtered[filtered["is_currently_available"] == True]
elif availability_view == "Available Soon":
    filtered = filtered[filtered["is_available_soon"] == True]
else:
    filtered = filtered[
        (filtered["is_currently_available"] == True)
        | (filtered["is_available_soon"] == True)
    ]

filtered = filtered.sort_values(
    ["is_currently_available", "is_available_soon", "dashboard_rent", "observed_at"],
    ascending=[False, False, True, False],
).copy()

filtered["availability_bucket"] = filtered.apply(format_availability_bucket, axis=1)

craigslist_df = craigslist_df.sort_values(
    ["dashboard_rent", "observed_at"],
    ascending=[True, False],
).copy()

if not craigslist_df.empty:
    craigslist_df["availability_bucket"] = craigslist_df.apply(format_availability_bucket, axis=1)

st.markdown('<div class="dashboard-title">Missoula Rental Housing Dashboard</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="dashboard-subtitle">A centralized view of current rental listings across Missoula housing sources.</div>',
    unsafe_allow_html=True,
)

st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

k1, k2, k3 = st.columns(3)

with k1:
    render_kpi("Listings shown", f"{len(filtered):,}")

with k2:
    render_kpi("Sources shown", f"{filtered['source'].nunique():,}")

with k3:
    render_kpi("Listings with rent", f"{int(filtered['dashboard_rent'].notna().sum()):,}")

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Listings", "Craigslist", "Source Summary", "Map", "Rent Distribution"]
)

with tab1:
    st.subheader("Available Listings")
    st.markdown(
        f'<div class="small-note">Showing {len(filtered):,} listings after filters.</div>',
        unsafe_allow_html=True,
    )

    rows_to_show = st.selectbox("Listings to display", [8, 15, 25, 50], index=1)
    listings_to_show = filtered.head(rows_to_show).copy()

    for idx, row in listings_to_show.iterrows():
        with st.container():
            st.markdown("---")

            col1, col2 = st.columns([4, 1])

            with col1:
                st.markdown(f"### {row.get('address') or row.get('listing_title') or 'Unknown address'}")
                st.markdown(
                    f"**Source:** {str(row.get('source', '')).upper()}  \n"
                    f"**Availability:** {row.get('availability_bucket', 'Unknown')}  \n"
                    f"**Details:** {format_card_meta(row)}"
                )

                if pd.notna(row.get("listing_url")) and str(row.get("listing_url")).strip():
                    st.markdown(f"[View Listing]({row['listing_url']})")

            with col2:
                reason = st.selectbox(
                    "Reason",
                    ["Spam", "No longer active", "Duplicate", "Incorrect data", "Other"],
                    key=f"reason_{idx}",
                )

                notes = st.text_input("Notes", key=f"notes_{idx}")

                if st.button("Flag Listing", key=f"flag_{idx}"):
                    insert_listing_flag(
                        source=row["source"],
                        source_record_id=row["source_record_id"],
                        listing_fingerprint=row.get("listing_fingerprint"),
                        cross_source_fingerprint=row.get("cross_source_fingerprint"),
                        address=row.get("address"),
                        listing_title=row.get("listing_title"),
                        listing_url=row.get("listing_url"),
                        flag_reason=reason,
                        flag_notes=notes,
                        flag_scope="listing",
                        flagged_by="client",
                    )

                    st.cache_data.clear()
                    st.success("Listing flagged and removed from the dashboard.")
                    st.rerun()

    csv_bytes = filtered[
        [
            "source",
            "availability_bucket",
            "address",
            "bedrooms",
            "bathrooms",
            "sqft",
            "rent_display",
            "listing_url",
        ]
    ].to_csv(index=False).encode("utf-8")

    st.download_button(
        "Download filtered listings as CSV",
        data=csv_bytes,
        file_name="missoula_dashboard_filtered_listings.csv",
        mime="text/csv",
    )

with tab2:
    st.subheader("Craigslist Listings")
    st.markdown(
        f'<div class="small-note">Showing {len(craigslist_df):,} Craigslist listings currently available in the dashboard layer.</div>',
        unsafe_allow_html=True,
    )

    if craigslist_df.empty:
        st.info("No Craigslist listings are available right now.")
    else:
        craigslist_rows = st.selectbox(
            "Craigslist listings to display",
            [10, 20, 30, 50],
            index=1,
            key="craigslist_rows",
        )

        craigslist_to_show = craigslist_df.head(craigslist_rows).copy()

        for idx, row in craigslist_to_show.iterrows():
            with st.container():
                st.markdown("---")

                col1, col2 = st.columns([4, 1])

                with col1:
                    st.markdown(f"### {row.get('listing_title') or row.get('address') or 'Unknown listing'}")
                    st.markdown(
                        f"**Source:** {str(row.get('source', '')).upper()}  \n"
                        f"**Availability:** {row.get('availability_bucket', 'Unknown')}  \n"
                        f"**Address:** {row.get('address') or 'Unknown'}  \n"
                        f"**Details:** {format_card_meta(row)}"
                    )

                    if pd.notna(row.get("listing_url")) and str(row.get("listing_url")).strip():
                        st.markdown(f"[View Listing]({row['listing_url']})")

                with col2:
                    reason = st.selectbox(
                        "Reason",
                        ["Spam", "No longer active", "Duplicate", "Incorrect data", "Other"],
                        key=f"craigslist_reason_{idx}",
                    )

                    notes = st.text_input("Notes", key=f"craigslist_notes_{idx}")

                    if st.button("Flag Listing", key=f"craigslist_flag_{idx}"):
                        insert_listing_flag(
                            source=row["source"],
                            source_record_id=row["source_record_id"],
                            listing_fingerprint=row.get("listing_fingerprint"),
                            cross_source_fingerprint=row.get("cross_source_fingerprint"),
                            address=row.get("address"),
                            listing_title=row.get("listing_title"),
                            listing_url=row.get("listing_url"),
                            flag_reason=reason,
                            flag_notes=notes,
                            flag_scope="listing",
                            flagged_by="client",
                        )

                        st.cache_data.clear()
                        st.success("Craigslist listing flagged and removed from the dashboard.")
                        st.rerun()

with tab3:
    st.subheader("Source Summary")

    source_summary = (
        filtered.groupby("source", dropna=False)
        .agg(
            listings=("source_record_id", "count"),
            currently_available=("is_currently_available", lambda s: int(pd.Series(s).fillna(False).sum())),
            median_rent=("dashboard_rent", "median"),
        )
        .reset_index()
    )

    if not source_summary.empty:
        source_summary["median_rent"] = source_summary["median_rent"].apply(
            lambda x: f"${int(x):,}" if pd.notna(x) else "Unknown"
        )
        source_summary = source_summary.rename(
            columns={
                "source": "Source",
                "listings": "Listings",
                "currently_available": "Currently Available",
                "median_rent": "Median Rent",
            }
        )
        st.dataframe(source_summary, use_container_width=True, hide_index=True)
    else:
        st.info("No source summary available for the current filters.")

with tab4:
    st.subheader("Listing Map")

    map_tabs = st.tabs(["Property Managers", "Craigslist"])

    with map_tabs[0]:
        pm_map_df = (
            filtered[filtered["source"] != "craigslist"]
            .drop_duplicates(subset=["address"])
            .head(35)
            .copy()
        )

        pm_map_df = add_geocoded_coordinates(pm_map_df, max_rows=35)

        render_map(pm_map_df, "property management listings")

    with map_tabs[1]:
        craigslist_map_df = craigslist_df[
            craigslist_df["latitude"].notna() & craigslist_df["longitude"].notna()
        ].copy()

        render_map(craigslist_map_df, "Craigslist listings")

with tab5:
    st.subheader("Rent Distribution")

    rent_chart_df = filtered[filtered["dashboard_rent"].notna()].copy()

    if rent_chart_df.empty:
        st.info("No rent data available for the current filters.")
    else:
        bins = [0, 800, 1000, 1200, 1400, 1600, 1800, 2200, 3000, 5000, 10000]
        labels = [
            "0-800",
            "801-1000",
            "1001-1200",
            "1201-1400",
            "1401-1600",
            "1601-1800",
            "1801-2200",
            "2201-3000",
            "3001-5000",
            "5001+",
        ]

        rent_chart_df["rent_band"] = pd.cut(
            rent_chart_df["dashboard_rent"],
            bins=bins,
            labels=labels,
            include_lowest=True,
            right=True,
        )

        rent_dist = rent_chart_df["rent_band"].value_counts().sort_index()
        st.bar_chart(rent_dist)

        st.dataframe(
            rent_chart_df[["source", "address", "bedrooms", "rent_display"]].rename(
                columns={
                    "source": "Source",
                    "address": "Address",
                    "bedrooms": "Beds",
                    "rent_display": "Rent",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

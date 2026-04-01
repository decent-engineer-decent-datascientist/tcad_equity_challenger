import streamlit as st
import pandas as pd
import sqlite3
import json
import math
import plotly.express as px
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression

# ── PAGE CONFIG ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="TCAD Equity Challenger", layout="wide", page_icon="⚖️")

# ── DESIGN TOKENS & GLOBAL CSS ─────────────────────────────────────────────────
NAVY   = "#1a2744"
AMBER  = "#c8922a"
CREAM  = "#f7f4ef"
WHITE  = "#ffffff"
BORDER = "#e2d9c8"
MUTED  = "#6b7a99"
GREEN  = "#15803d"
RED    = "#7c3434"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700&family=Outfit:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');

:root {{
    --navy:    {NAVY};
    --amber:   {AMBER};
    --cream:   {CREAM};
    --white:   {WHITE};
    --border:  {BORDER};
    --muted:   {MUTED};
    --green:   {GREEN};
}}

/* ── App shell ── */
.stApp {{ background: var(--cream); font-family: 'Outfit', sans-serif; }}
.stApp > header {{ background: transparent !important; }}
.block-container {{ padding-top: 2rem !important; max-width: 1200px; }}

/* ── Sidebar ── */
[data-testid="stSidebar"] {{
    background: var(--navy) !important;
    border-right: none !important;
}}
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stRadio label span,
[data-testid="stSidebar"] .stSlider p {{
    color: #a0b0cc !important;
    font-family: 'Outfit', sans-serif;
    font-size: 0.85rem !important;
}}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h3 {{
    color: var(--amber) !important;
    font-family: 'Outfit', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.7rem !important;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    border-bottom: 1px solid rgba(200,146,42,0.25);
    padding-bottom: 0.5rem;
    margin: 1.25rem 0 0.75rem 0;
}}

/* ── Typography ── */
h1 {{
    font-family: 'Playfair Display', serif !important;
    color: var(--navy) !important;
    font-size: 2.2rem !important;
    line-height: 1.15 !important;
}}
h2, h3 {{
    font-family: 'Outfit', sans-serif !important;
    color: var(--navy) !important;
    font-weight: 600 !important;
}}

/* ── Native metrics (fallback) ── */
[data-testid="stMetric"] {{
    background: var(--white);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 1rem 1.25rem !important;
}}
[data-testid="stMetricLabel"] p {{
    font-size: 0.68rem !important;
    text-transform: uppercase;
    letter-spacing: 0.09em;
    color: var(--muted) !important;
    font-family: 'Outfit', sans-serif;
}}
[data-testid="stMetricValue"] {{
    font-family: 'DM Mono', monospace !important;
    color: var(--navy) !important;
    font-size: 1.35rem !important;
}}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {{
    gap: 2px;
    background: transparent;
    border-bottom: 2px solid var(--border);
    padding-bottom: 0;
}}
.stTabs [data-baseweb="tab"] {{
    font-family: 'Outfit', sans-serif;
    font-weight: 500;
    font-size: 0.83rem;
    color: var(--muted);
    background: transparent !important;
    border: none !important;
    border-bottom: 3px solid transparent !important;
    border-radius: 0 !important;
    padding: 0.55rem 1rem;
    margin-bottom: -2px;
    transition: color 0.15s;
}}
.stTabs [aria-selected="true"] {{
    color: var(--navy) !important;
    border-bottom: 3px solid var(--amber) !important;
    font-weight: 600 !important;
}}
.stTabs [data-baseweb="tab-panel"] {{ padding-top: 1.75rem; }}

/* ── Download button ── */
[data-testid="stDownloadButton"] > button {{
    background: var(--amber) !important;
    color: var(--white) !important;
    border: none !important;
    border-radius: 7px !important;
    font-family: 'Outfit', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.88rem !important;
    padding: 0.6rem 1.5rem !important;
    letter-spacing: 0.02em;
    transition: opacity 0.15s !important;
}}
[data-testid="stDownloadButton"] > button:hover {{ opacity: 0.85 !important; }}

/* ── Selectbox ── */
[data-testid="stSelectbox"] > div > div {{
    border-color: var(--border) !important;
    border-radius: 7px !important;
    font-family: 'Outfit', sans-serif !important;
    background: var(--white) !important;
    font-size: 0.9rem;
}}

/* ── DataFrame / DataEditor ── */
[data-testid="stDataFrame"],
[data-testid="stDataEditor"] {{
    border-radius: 10px !important;
    border: 1px solid var(--border) !important;
    overflow: hidden;
}}

/* ── Alerts ── */
[data-testid="stAlert"] {{
    border-radius: 8px !important;
    font-family: 'Outfit', sans-serif;
    font-size: 0.9rem;
}}

/* ── Divider ── */
hr {{ border-color: var(--border) !important; margin: 1.75rem 0 !important; }}

/* ── Scrollbar ── */
::-webkit-scrollbar {{ width: 5px; height: 5px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 3px; }}
</style>
""", unsafe_allow_html=True)


# ── CHART THEME HELPERS ────────────────────────────────────────────────────────
_LAYOUT_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Outfit, sans-serif", color=NAVY, size=12),
    margin=dict(l=16, r=16, t=48, b=16),
    hoverlabel=dict(bgcolor=NAVY, font_color=WHITE,
                    font_family="Outfit, sans-serif", bordercolor=NAVY),
    xaxis=dict(gridcolor="#ede8e0", linecolor=BORDER,
               tickfont=dict(size=11), zeroline=False),
    yaxis=dict(gridcolor="#ede8e0", linecolor=BORDER,
               tickfont=dict(size=11), zeroline=False),
)

def apply_chart_theme(fig: go.Figure, title: str = "") -> go.Figure:
    updates = dict(_LAYOUT_BASE)
    if title:
        updates["title"] = dict(
            text=title,
            font=dict(family="Playfair Display, serif", size=15, color=NAVY),
            x=0, xanchor="left",
        )
    fig.update_layout(**updates)
    return fig


# ── CUSTOM HTML COMPONENTS ─────────────────────────────────────────────────────
def metric_card(label: str, value: str, icon: str = "") -> str:
    return (
        f'<div style="background:{WHITE}; border:1px solid {BORDER}; border-radius:10px; '
        f'padding:1.1rem 1.4rem; box-shadow:0 1px 3px rgba(26,39,68,0.05);">'
        f'<div style="font-family:\'Outfit\',sans-serif; font-size:0.68rem; text-transform:uppercase; '
        f'letter-spacing:0.09em; color:{MUTED}; margin-bottom:0.45rem;">{icon}&nbsp;&nbsp;{label}</div>'
        f'<div style="font-family:\'DM Mono\',monospace; font-size:1.3rem; color:{NAVY}; '
        f'font-weight:500; line-height:1.2;">{value}</div>'
        f'</div>'
    )

def section_heading(title: str, subtitle: str = "") -> str:
    sub = (
        f'<div style="font-size:0.83rem; color:{MUTED}; margin-top:0.25rem; '
        f'font-weight:400; line-height:1.5;">{subtitle}</div>'
    ) if subtitle else ""
    return (
        f'<div style="border-left:3px solid {AMBER}; padding-left:0.85rem; margin-bottom:1.25rem;">'
        f'<div style="font-family:\'Outfit\',sans-serif; font-weight:600; '
        f'font-size:1rem; color:{NAVY};">{title}</div>'
        f'{sub}</div>'
    )

def result_card_win(reduction: float, total: float, n: int) -> str:
    return (
        f'<div style="background:linear-gradient(135deg,{NAVY} 0%,#243560 100%); '
        f'border-radius:12px; padding:1.75rem 2rem; margin:1.25rem 0; border-left:4px solid {AMBER};">'
        f'<div style="font-family:\'Outfit\',sans-serif; font-size:0.68rem; text-transform:uppercase; '
        f'letter-spacing:0.12em; color:{AMBER}; margin-bottom:0.35rem;">Proposed Reduction</div>'
        f'<div style="font-family:\'DM Mono\',monospace; font-size:2.4rem; font-weight:500; '
        f'color:{WHITE}; line-height:1.1; margin-bottom:0.75rem;">${reduction:,.0f}</div>'
        f'<div style="font-family:\'Outfit\',sans-serif; font-size:0.88rem; color:#a0b4cc; line-height:1.6;">'
        f'Based on <strong style="color:{WHITE};">{n} comparable properties</strong> — '
        f'target equalized value:&nbsp;'
        f'<span style="font-family:\'DM Mono\',monospace; color:{AMBER};">${total:,.0f}</span>'
        f'</div></div>'
    )

def result_card_neutral(total: float, n: int) -> str:
    return (
        f'<div style="background:{WHITE}; border-radius:12px; border:1px solid {BORDER}; '
        f'padding:1.5rem 2rem; margin:1.25rem 0; border-left:4px solid {MUTED};">'
        f'<div style="font-family:\'Outfit\',sans-serif; font-size:0.68rem; text-transform:uppercase; '
        f'letter-spacing:0.1em; color:{MUTED}; margin-bottom:0.35rem;">Valuation Result</div>'
        f'<div style="font-family:\'Outfit\',sans-serif; font-size:0.92rem; color:{NAVY};">'
        f'Based on <strong>{n} properties</strong>, TCAD\'s valuation appears equitable. '
        f'Target value: <span style="font-family:\'DM Mono\',monospace;">${total:,.0f}</span>.'
        f'</div></div>'
    )


# ── DATA LOADING ───────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    conn  = sqlite3.connect("tcad_data.db")
    query = """
        SELECT
            g.pAccountID,
            g.pID,
            g.name AS ownerName,
            g.nameSecondary AS ownerNameSecondary,
            g.streetAddress,
            g.legalDescription,
            g.geoID,
            v.ownerAppraisedValue,
            v.ownerImprovementValue,
            v.ownerLandValue,
            i.livingArea,
            i.imprvSpecificDescription,
            l.sizeSqft as lotSizeSqft,
            MAX(d.actualYearBuilt) as yearBuilt,
            p.geometry,
            MAX(CASE WHEN d.detailTypeDescription = 'BATHROOM'        THEN d.area ELSE 0 END) as bath_count,
            MAX(CASE WHEN d.detailTypeDescription = 'HALF BATHROOM'   THEN d.area ELSE 0 END) as half_bath_count,
            MAX(CASE WHEN d.detailTypeDescription = 'BEDROOMS'        THEN d.area ELSE 0 END) as bed_count,
            MAX(CASE WHEN d.detailTypeDescription = 'POOL RES CONC'   THEN 1    ELSE 0 END) as has_pool,
            MAX(CASE WHEN d.detailTypeDescription = 'SPA CONCRETE'    THEN 1    ELSE 0 END) as has_spa,
            MAX(CASE WHEN d.detailTypeDescription = 'OUTDOOR KITCHEN' THEN 1    ELSE 0 END) as has_outdoor_kitchen,
            MAX(CASE WHEN d.detailTypeDescription = 'FIREPLACE'       THEN d.area ELSE 0 END) as fireplace_count,
            SUM(CASE WHEN d.detailTypeDescription LIKE '%GARAGE%'     THEN d.area ELSE 0 END) as garage_area
        FROM general g
        LEFT JOIN value_history v       ON g.pAccountID = v.pAccountID AND v.pYear = (SELECT MAX(pYear) FROM value_history)
        LEFT JOIN improvement i         ON g.pAccountID = i.pAccountID
        LEFT JOIN land l                ON g.pAccountID = l.pAccountID
        LEFT JOIN improvement_details d ON g.pAccountID = d.pAccountID
        LEFT JOIN parcel p              ON g.pAccountID = p.pAccountID
        GROUP BY g.pAccountID
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    numeric_cols = [
        'ownerAppraisedValue', 'ownerImprovementValue', 'ownerLandValue',
        'livingArea', 'lotSizeSqft', 'yearBuilt', 'bath_count', 'half_bath_count',
        'bed_count', 'has_pool', 'has_spa', 'has_outdoor_kitchen', 'fireplace_count', 'garage_area',
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df = df.dropna(subset=['livingArea', 'yearBuilt', 'streetAddress'])
    df = df[df['ownerAppraisedValue'] > 0]
    df['PricePerSqFt'] = df['ownerImprovementValue'] / df['livingArea']
    df['pAccountID']   = df['pAccountID'].astype(str)
    df['tcad_link']    = "https://travis.prodigycad.com/property-detail/" + df['pID'].astype(str) + "/2026"

    # BUILDER INVENTORY FILTER — removes active builder inventory to clean dataset
    if 'ownerName' in df.columns:
        builders = ['TOLL', 'TAYLOR MORRISON', 'PULTE', 'LENNAR', 'DR HORTON', 'D R HORTON',
                    'MERITAGE', 'KB HOME', 'ASHTON WOODS', 'PERRY HOMES']
        pattern     = '|'.join(builders)
        mask_p      = df['ownerName'].astype(str).str.upper().str.contains(pattern, na=False)
        mask_s      = df['ownerNameSecondary'].astype(str).str.upper().str.contains(pattern, na=False)
        df = df[~(mask_p | mask_s)]

    return df

df = load_data()


# ── HERO HEADER ────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="padding:0 0 1.75rem 0; border-bottom:1px solid {BORDER}; margin-bottom:2rem;">
    <div style="display:flex; align-items:baseline; gap:0.85rem; margin-bottom:0.6rem;">
        <span style="font-size:1.9rem; line-height:1;">⚖️</span>
        <h1 style="margin:0; font-family:'Playfair Display',serif; font-size:2.2rem;
                   color:{NAVY}; line-height:1.1; font-weight:700;">
            TCAD Equity Challenger
        </h1>
    </div>
    <p style="margin:0; color:{MUTED}; font-family:'Outfit',sans-serif; font-size:0.92rem;
              max-width:700px; line-height:1.65;">
        Generate data-driven evidence to challenge your property taxes under&nbsp;
        <code style="font-family:'DM Mono',monospace; font-size:0.8rem; background:{AMBER}18;
                     color:{AMBER}; padding:2px 8px; border-radius:4px; border:1px solid {AMBER}44;">
            Tex. Tax Code § 41.43(b)(3)
        </code>
        &nbsp;— Unequal Appraisal.
    </p>
</div>
""", unsafe_allow_html=True)


# ── SUBJECT PROPERTY SELECTION ─────────────────────────────────────────────────
addresses        = sorted(df[df['streetAddress'].str[0].str.isdigit()]['streetAddress'].unique())
selected_address = st.selectbox("Search for your property:", addresses)
subject          = df[df['streetAddress'] == selected_address].iloc[0]

st.markdown(
    f"<div style='font-family:\"Outfit\",sans-serif; font-size:0.68rem; text-transform:uppercase; "
    f"letter-spacing:0.1em; color:{MUTED}; margin:1.5rem 0 0.75rem 0;'>Subject Property</div>",
    unsafe_allow_html=True,
)

c1, c2, c3, c4, c5 = st.columns(5)
for col, label, val, icon in [
    (c1, "Total Appraised",   f"${subject['ownerAppraisedValue']:,.0f}",  "🏷"),
    (c2, "Improvement Value", f"${subject['ownerImprovementValue']:,.0f}", "🏗"),
    (c3, "Living Area",       f"{subject['livingArea']:,.0f} ft²",         "📐"),
    (c4, "Lot Size",          f"{subject['lotSizeSqft']:,.0f} ft²",        "🌿"),
    (c5, "Year Built",        f"{subject['yearBuilt']:.0f}",               "📅"),
]:
    col.markdown(metric_card(label, val, icon), unsafe_allow_html=True)

st.markdown("<div style='margin-top:1rem;'></div>", unsafe_allow_html=True)
st.divider()


# ── SIDEBAR ────────────────────────────────────────────────────────────────────
st.sidebar.markdown(f"""
<div style="padding:1.25rem 0 1rem 0; border-bottom:1px solid rgba(200,146,42,0.2);
            margin-bottom:0.5rem;">
    <div style="font-family:'Playfair Display',serif; font-size:1.1rem;
                color:{AMBER}; font-weight:600;">Engine Config</div>
    <div style="font-size:0.75rem; color:#7a8aaa; margin-top:0.2rem;">
        Comps selection &amp; tolerances
    </div>
</div>
""", unsafe_allow_html=True)

mode = st.sidebar.radio(
    "Selection Engine",
    ["Tax Advocate Strategy (Recommended)", "Simple (Manual Filters)"],
    label_visibility="collapsed",
)


# ── ENGINE LOGIC ───────────────────────────────────────────────────────────────
neighborhood_df = df[df['pAccountID'] != subject['pAccountID']].copy()

hedonic_features = [
    'livingArea', 'yearBuilt', 'lotSizeSqft', 'bath_count', 'half_bath_count',
    'bed_count', 'has_pool', 'has_spa', 'has_outdoor_kitchen', 'fireplace_count', 'garage_area',
]

comps = pd.DataFrame()  # always defined

if len(neighborhood_df) < 5:
    st.error("Database contains fewer than 5 comparable properties. Check your SQL extraction process.")
else:
    if mode == "Simple (Manual Filters)":
        st.sidebar.markdown("### Manual Filters")
        sqft_variance = st.sidebar.slider("SqFt Tolerance (+/- %)", 5, 30, 10) / 100.0
        age_variance  = st.sidebar.slider("Age Tolerance (+/- Years)", 0, 10, 3)

        comps = neighborhood_df[
            neighborhood_df['livingArea'].between(
                subject['livingArea'] * (1 - sqft_variance),
                subject['livingArea'] * (1 + sqft_variance),
            ) &
            neighborhood_df['yearBuilt'].between(
                subject['yearBuilt'] - age_variance,
                subject['yearBuilt'] + age_variance,
            )
        ].copy()
        if len(comps) > 0:
            comps['Adjusted Imprv Value'] = comps['ownerImprovementValue']

    elif mode == "Tax Advocate Strategy (Recommended)":
        st.sidebar.markdown("### Equity Optimization")
        num_comps     = st.sidebar.slider("Comps for Median", 3, 15, 5)
        sqft_variance = st.sidebar.slider("SqFt Tolerance (+/- %)", 10, 50, 20) / 100.0
        age_variance  = st.sidebar.slider("Age Tolerance (+/- Years)", 5, 50, 10)

        legal_pool = neighborhood_df[
            neighborhood_df['livingArea'].between(
                subject['livingArea'] * (1 - sqft_variance),
                subject['livingArea'] * (1 + sqft_variance),
            ) &
            neighborhood_df['yearBuilt'].between(
                subject['yearBuilt'] - age_variance,
                subject['yearBuilt'] + age_variance,
            )
        ].copy()

        if len(legal_pool) < num_comps:
            st.warning(
                f"Only **{len(legal_pool)}** properties match your current tolerances, "
                f"but you requested **{num_comps}** comps. "
                f"Try reducing *Comps for Median* to **{max(3, len(legal_pool))}**, "
                f"or widen the SqFt / Age tolerances."
            )
        else:
            # One-sided trim: remove only the top 2.5% of $/SqFt (preserve favorable low-end data)
            upper_bound = neighborhood_df['PricePerSqFt'].quantile(0.975)
            reg_data    = neighborhood_df[neighborhood_df['PricePerSqFt'] <= upper_bound].copy()

            if len(reg_data) >= 10:
                reg   = LinearRegression().fit(reg_data[hedonic_features], reg_data['ownerImprovementValue'])
                coefs = dict(zip(hedonic_features, reg.coef_))
            else:
                coefs = {f: 0 for f in hedonic_features}
                coefs.update({'livingArea': 50.0, 'yearBuilt': -1000.0})

            legal_pool['Total Adjustments'] = 0
            for feat in hedonic_features:
                legal_pool[f'{feat}_adj']       = (subject[feat] - legal_pool[feat]) * coefs[feat]
                legal_pool['Total Adjustments'] += legal_pool[f'{feat}_adj']

            legal_pool['Adjusted Imprv Value'] = (
                legal_pool['ownerImprovementValue'] + legal_pool['Total Adjustments']
            )
            comps = legal_pool.sort_values('Adjusted Imprv Value').head(num_comps).copy()


# ── MAIN TABS ──────────────────────────────────────────────────────────────────
if len(comps) > 0:
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋  Comp Selection",
        "🗺  Geographic Evidence",
        "⚖  Adjustment Methodology",
        "🔍  Deep Dive & Export",
    ])

    # ── TAB 1 ────────────────────────────────────────────────────────────────
    with tab1:
        st.markdown(section_heading(
            "Evidence Review & Selection",
            "Uncheck any row to remove it from the analysis. Changes propagate across all tabs."
        ), unsafe_allow_html=True)

        display_cols = [
            'tcad_link', 'streetAddress', 'yearBuilt', 'livingArea',
            'ownerAppraisedValue', 'ownerImprovementValue', 'PricePerSqFt', 'Adjusted Imprv Value',
        ]
        editor_df = comps[display_cols].copy()
        editor_df.insert(0, 'Include', True)
        editor_df = editor_df.rename(columns={
            'streetAddress':         'Address',
            'yearBuilt':             'Year',
            'livingArea':            'SqFt',
            'ownerAppraisedValue':   'Total Value',
            'ownerImprovementValue': 'Imprv Value',
            'PricePerSqFt':          '$/SqFt',
        })

        edited_df = st.data_editor(
            editor_df,
            column_config={
                "Include":              st.column_config.CheckboxColumn("✓",           default=True),
                "tcad_link":            st.column_config.LinkColumn("TCAD",            display_text="↗ View"),
                "Year":                 st.column_config.NumberColumn("Year",           format="%d"),
                "SqFt":                 st.column_config.NumberColumn("SqFt",           format="%d"),
                "Total Value":          st.column_config.NumberColumn("Total Value",    format="$%d"),
                "Imprv Value":          st.column_config.NumberColumn("Imprv Value",    format="$%d"),
                "$/SqFt":               st.column_config.NumberColumn("$/SqFt",         format="$%.2f"),
                "Adjusted Imprv Value": st.column_config.NumberColumn("Adj. Value",     format="$%d"),
            },
            disabled=["Address", "Year", "SqFt", "Total Value", "Imprv Value",
                      "$/SqFt", "Adjusted Imprv Value", "tcad_link"],
            hide_index=True,
            use_container_width=True,
        )

        kept_addresses = edited_df[edited_df['Include']]['Address'].tolist()
        final_comps    = comps[comps['streetAddress'].isin(kept_addresses)].copy()

        if len(final_comps) > 0:
            suggested_imprv_value = final_comps['Adjusted Imprv Value'].median()
            suggested_total_value = suggested_imprv_value + subject['ownerLandValue']
            reduction             = subject['ownerAppraisedValue'] - suggested_total_value

            st.markdown("<div style='margin-top:1.5rem;'></div>", unsafe_allow_html=True)
            st.markdown(section_heading("The Bottom Line"), unsafe_allow_html=True)

            if reduction > 0:
                st.markdown(result_card_win(reduction, suggested_total_value, len(final_comps)),
                            unsafe_allow_html=True)
            else:
                st.markdown(result_card_neutral(suggested_total_value, len(final_comps)),
                            unsafe_allow_html=True)
        else:
            suggested_imprv_value = None
            st.warning("No properties selected. Check at least one comp to continue.")

    # ── TAB 2 ────────────────────────────────────────────────────────────────
    with tab2:
        if len(final_comps) == 0 or suggested_imprv_value is None:
            st.warning("No comps selected. Return to **Comp Selection** and include at least one property.")
        else:
            subj_plot = subject.to_dict()
            subj_plot.update({
                'streetAddress':        f"{subj_plot['streetAddress']} (Subject)",
                'Adjusted Imprv Value': subj_plot['ownerImprovementValue'],
                'is_subject':           'Subject Property',
                'dot_size':             20,
            })
            comps_plot = final_comps.copy()
            comps_plot['is_subject'] = 'Comparable Property'
            comps_plot['dot_size']   = 8
            plot_df = pd.concat([pd.DataFrame([subj_plot]), comps_plot])

            # ── Parcel map ──────────────────────────────────────────────────
            st.markdown(section_heading(
                "Geographic Parcel Map",
                "Amber parcel = subject property.  Navy parcels = selected comparables."
            ), unsafe_allow_html=True)

            plot_df['calc_lat'] = None
            plot_df['calc_lon'] = None
            features = []

            for idx, row in plot_df.iterrows():
                if pd.notnull(row.get('geometry')):
                    try:
                        geom = json.loads(row['geometry'])
                        features.append({
                            "type": "Feature", "id": row['pAccountID'],
                            "geometry": geom,
                            "properties": {"address": row['streetAddress']},
                        })
                        ring = geom['coordinates'][0][0]
                        plot_df.at[idx, 'calc_lon'] = sum(p[0] for p in ring) / len(ring)
                        plot_df.at[idx, 'calc_lat'] = sum(p[1] for p in ring) / len(ring)
                    except Exception:
                        pass

            valid_scatter_df = plot_df.dropna(subset=['calc_lat', 'calc_lon'])
            if not valid_scatter_df.empty:
                lats = valid_scatter_df['calc_lat']
                lons = valid_scatter_df['calc_lon']
                smart_center_lat = (lats.min() + lats.max()) / 2
                smart_center_lon = (lons.min() + lons.max()) / 2
                max_spread       = max(lats.max() - lats.min(), lons.max() - lons.min())
                smart_zoom       = math.log2(360 / max_spread) - 1 if max_spread > 0 else 16
            else:
                smart_center_lat, smart_center_lon, smart_zoom = 30.2672, -97.7431, 15

            if features:
                color_map = {'Subject Property': AMBER, 'Comparable Property': NAVY}
                fig_map   = px.choropleth_mapbox(
                    plot_df,
                    geojson={"type": "FeatureCollection", "features": features},
                    locations="pAccountID", color="is_subject",
                    color_discrete_map=color_map,
                    hover_name="streetAddress",
                    center={"lat": smart_center_lat, "lon": smart_center_lon},
                    zoom=smart_zoom, mapbox_style="open-street-map",
                    opacity=0.55, height=520,
                )
                if not valid_scatter_df.empty:
                    fig_scatter = px.scatter_mapbox(
                        valid_scatter_df, lat="calc_lat", lon="calc_lon",
                        color="is_subject", size="dot_size", size_max=14,
                        color_discrete_map={
                            'Subject Property':  '#a06418',
                            'Comparable Property': '#0d1c38',
                        },
                    )
                    for trace in fig_scatter.data:
                        trace.showlegend    = False
                        trace.hoverinfo     = 'skip'
                        trace.hovertemplate = None
                        fig_map.add_trace(trace)

                fig_map.update_layout(
                    showlegend=False,
                    margin={"r": 0, "t": 0, "l": 0, "b": 0},
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.info("Parcel geometries could not be parsed for this selection.")

            st.divider()

            # ── Equity gap bar chart ─────────────────────────────────────────
            st.markdown(section_heading(
                "The Equity Gap",
                "Adjusted improvement values across subject and comparable properties."
            ), unsafe_allow_html=True)

            plot_df_bar = plot_df.sort_values('Adjusted Imprv Value')
            fig_bar = px.bar(
                plot_df_bar, x='streetAddress', y='Adjusted Imprv Value', color='is_subject',
                color_discrete_map={'Subject Property': AMBER, 'Comparable Property': NAVY},
            )
            fig_bar.add_hline(
                y=suggested_imprv_value,
                line_dash="dot", line_color=GREEN, line_width=2,
                annotation_text=f"Target Median  ${suggested_imprv_value:,.0f}",
                annotation_font=dict(color=GREEN, size=11, family="DM Mono, monospace"),
            )
            fig_bar.update_traces(marker_line_width=0)
            apply_chart_theme(fig_bar)
            fig_bar.update_layout(
                showlegend=False,
                xaxis_title="", yaxis_title="Adjusted Improvement Value ($)",
                bargap=0.35,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    # ── TAB 3 ────────────────────────────────────────────────────────────────
    with tab3:
        if mode != "Tax Advocate Strategy (Recommended)":
            st.info("Adjustment methodology is only available in **Tax Advocate Strategy** mode.")
        elif len(final_comps) == 0 or 'Total Adjustments' not in final_comps.columns:
            st.warning("No comps selected.")
        else:
            st.markdown(section_heading(
                "Step 1 — Normalization Coefficients",
                "Multivariate OLS regression across the neighborhood — the same methodology TCAD uses internally."
            ), unsafe_allow_html=True)

            coef_df = pd.DataFrame([coefs]).T.reset_index()
            coef_df.columns = ['Feature', 'Coefficient ($)']
            st.dataframe(
                coef_df.style.format({'Coefficient ($)': '${:,.2f}'}),
                use_container_width=True, hide_index=True,
            )

            st.divider()
            st.markdown(section_heading(
                "Step 2 — Adjustment Waterfall",
                "How each physical difference between subject and comparable translates to a dollar adjustment."
            ), unsafe_allow_html=True)

            waterfall_addr = st.selectbox(
                "Comparable property:", final_comps['streetAddress'].tolist(),
                label_visibility="collapsed",
            )
            rep = final_comps[final_comps['streetAddress'] == waterfall_addr].iloc[0]

            wx, wy, wm, wt = ["Base Value"], [rep['ownerImprovementValue']], ["relative"], [f"${rep['ownerImprovementValue']/1000:.1f}k"]
            shown_sum = 0
            for feat in hedonic_features:
                v = rep[f'{feat}_adj']
                if abs(v) > 100:
                    wx.append(feat.replace('_', ' ').title())
                    wy.append(v)
                    wm.append("relative")
                    wt.append(f"${v/1000:+.1f}k")
                    shown_sum += v

            residual = rep['Total Adjustments'] - shown_sum
            if abs(residual) > 1:
                wx.append("Other"); wy.append(residual); wm.append("relative"); wt.append(f"${residual/1000:+.1f}k")

            wx.append("Final Adjusted"); wy.append(rep['Adjusted Imprv Value']); wm.append("total"); wt.append(f"${rep['Adjusted Imprv Value']/1000:.1f}k")

            fig_water = go.Figure(go.Waterfall(
                orientation="v", measure=wm, x=wx, y=wy,
                textposition="outside", text=wt,
                textfont=dict(family="DM Mono, monospace", size=11),
                connector=dict(line=dict(color=BORDER, width=1)),
                increasing=dict(marker=dict(color=AMBER)),
                decreasing=dict(marker=dict(color=RED)),
                totals=dict(marker=dict(color=NAVY)),
            ))
            apply_chart_theme(fig_water, title=f"Adjustments — {rep['streetAddress']}")
            fig_water.update_layout(height=440, showlegend=False)
            st.plotly_chart(fig_water, use_container_width=True)

            st.markdown(section_heading("Complete Adjustment Ledger"), unsafe_allow_html=True)
            ledger_cols = (
                ['streetAddress', 'ownerImprovementValue'] +
                [f'{f}_adj' for f in hedonic_features] +
                ['Total Adjustments', 'Adjusted Imprv Value']
            )
            ledger_df = final_comps[ledger_cols].rename(columns={
                'streetAddress': 'Address', 'ownerImprovementValue': 'Base Imprv Value',
            })
            st.dataframe(
                ledger_df.style.format(lambda x: f"${x:,.0f}" if isinstance(x, (int, float)) else x),
                use_container_width=True, hide_index=True,
            )

    # ── TAB 4 ────────────────────────────────────────────────────────────────
    with tab4:
        if len(final_comps) == 0:
            st.warning("No comps selected. Return to **Comp Selection** first.")
        else:
            st.markdown(section_heading(
                "Download Evidence Package",
                "CSV includes adjusted values, regression coefficients, and TCAD links — ready for your protest hearing."
            ), unsafe_allow_html=True)

            export_cols = [
                'pAccountID', 'tcad_link', 'ownerName', 'streetAddress', 'legalDescription',
                'yearBuilt', 'livingArea', 'lotSizeSqft', 'ownerAppraisedValue', 'PricePerSqFt',
            ]
            if 'Total Adjustments' in final_comps.columns:
                export_cols.extend(
                    [f'{f}_adj' for f in hedonic_features] + ['Total Adjustments', 'Adjusted Imprv Value']
                )
            else:
                export_cols.append('Adjusted Imprv Value')

            st.download_button(
                label="⬇  Download Evidence Report (CSV)",
                data=final_comps[export_cols].to_csv(index=False).encode('utf-8'),
                file_name='tcad_equity_evidence.csv',
                mime='text/csv',
                type="primary",
            )

            st.divider()
            st.markdown(section_heading(
                "Property Deep Dive",
                "Inspect every raw field for any property in the analysis."
            ), unsafe_allow_html=True)

            view_address = st.selectbox(
                "Select property:",
                [subject['streetAddress']] + final_comps['streetAddress'].tolist(),
            )
            detail_data = (
                subject
                if view_address == subject['streetAddress']
                else final_comps[final_comps['streetAddress'] == view_address].iloc[0]
            )

            pid  = detail_data.get('pID', '')
            link = detail_data.get('tcad_link', '')
            st.markdown(
                f'<a href="{link}" target="_blank" '
                f'style="font-family:\'Outfit\',sans-serif; font-size:0.88rem; color:{AMBER}; '
                f'text-decoration:none; border:1px solid {AMBER}55; padding:6px 16px; '
                f'border-radius:6px; background:{AMBER}0d; display:inline-block;">'
                f'↗&nbsp;&nbsp;View Property {pid} on TCAD</a>',
                unsafe_allow_html=True,
            )
            st.markdown("<div style='margin:0.85rem 0;'></div>", unsafe_allow_html=True)

            display_data = pd.DataFrame([detail_data])
            if 'geometry' in display_data.columns:
                display_data = display_data.drop(columns=['geometry'])
            st.dataframe(display_data, hide_index=True, use_container_width=True)

else:
    st.warning("No properties match your current filters. Widen the tolerances in the sidebar.")
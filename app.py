import streamlit as st
import pandas as pd
import sqlite3
import json
import math
import requests
import time
import zipfile
import io
import plotly.express as px
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression

# --- PAGE CONFIG ---
st.set_page_config(page_title="TCAD Equity Challenger", layout="wide",
                   page_icon="⚖️")

# # --- PRINT CSS HACK ---
# st.markdown("""
# <style>
# @media print {
#     /* Hide sidebar, top header, buttons, and alert boxes (like st.info tips) */
#     [data-testid="stSidebar"], header, .stAlert, .stButton { display: none !important; }
    
#     /* Maximize printable area */
#     .block-container {
#         padding-top: 0rem !important; 
#         padding-left: 0rem !important; 
#         padding-right: 0rem !important;
#     }
    
#     /* Prevent charts from being split across two pages */
#     .js-plotly-plot { page-break-inside: avoid; }
# }
# </style>
# """, unsafe_allow_html=True)

# --- TCAD API FUNCTIONS ---
def get_tcad_token():
    url = 'https://prod-container.trueprodigyapi.com/trueprodigy/cadpublic/auth/token'
    headers = {
        'Content-Type': 'application/json',
        'Origin': 'https://travis.prodigycad.com',
        'Referer': 'https://travis.prodigycad.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
    try:
        response = requests.post(url, headers=headers, json={"office": "Travis"}, timeout=10)
        if response.ok:
            return response.json().get('user', {}).get('token')
        else:
            st.error(f"TCAD Auth Rejected [HTTP {response.status_code}]: {response.text}")
    except Exception as e:
        st.error(f"Network error connecting to TCAD Auth: {e}")
    return None

def fetch_property_card_pdf(token, pid, account_id):
    url = 'https://prod-container.trueprodigyapi.com/public/runreport'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': token,
        'Origin': 'https://travis.prodigycad.com',
        'Referer': 'https://travis.prodigycad.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
    
    payload = {
        "jasperSoftParams": {
            "reportUnitUri": "/public/TrueProdigy/ProdigyAppraisal/Reports/ClientUIPublicCard",
            "async": True,
            "allowInlineScripts": False,
            "markupType": "embeddable",
            "interactive": True,
            "freshData": False,
            "saveDataSnapshot": False,
            "transformerKey": None,
            "attachmentsPrefix": "https://www.trueprodigy-reporting.com/jasperserver-pro/rest_v2/reportExecutions/{reportExecutionId}/exports/{exportExecutionId}/attachments/",
            "baseUrl": "https://www.trueprodigy-reporting.com/jasperserver-pro",
            "outputFormat": "pdf",
            "parameters": {
                "reportParameter": [
                    {"name": "TP_DATABASE", "value": ["travis_appraisal"]},
                    {"name": "TP_OFFICE_NAME", "value": ["travis"]},
                    {"name": "TP_SELECTED_QUERY", "value": [f"pid = {pid} and pYear = 2026 limit 1"]},
                    {"name": "TP_SELECTED_PID", "value": [int(pid)]},
                    {"name": "TP_SELECTED_PYEAR", "value": ["2026"]},
                    {"name": "TP_ACCOUNT_ID", "value": [int(account_id)]},
                    {"name": "TP_SHOW_PROTEST", "value": ["true"]},
                    {"name": "TP_SHOW_ZONING", "value": [""]}
                ]
            }
        }
    }
    
    try:
        response = requests.put(url, headers=headers, json=payload, timeout=20)
        if response.ok:
            if response.content.startswith(b'%PDF'):
                return response.content
            else:
                st.error("TCAD Firewall blocked the request from this cloud server IP address.")
                return None
        else:
            st.error(f"TCAD PDF Rejected [HTTP {response.status_code}]: {response.text[:200]}")
    except Exception as e:
        st.error(f"Network error fetching PDF from TCAD: {e}")
    return None

# --- DATA LOADING ---
@st.cache_data
def load_data():
    conn = sqlite3.connect("tcad_data.db")
    query = """
        SELECT
            g.pAccountID, g.pID, g.name AS ownerName, g.nameSecondary AS ownerNameSecondary,
            g.streetAddress, g.legalDescription, g.geoID,
            v.ownerAppraisedValue, v.ownerImprovementValue, v.ownerLandValue,
            i.livingArea, i.imprvSpecificDescription, l.sizeSqft as lotSizeSqft,
            MAX(d.actualYearBuilt) as yearBuilt, p.geometry,
            MAX(CASE WHEN d.detailTypeDescription = 'BATHROOM' THEN d.area ELSE 0 END) as bath_count,
            MAX(CASE WHEN d.detailTypeDescription = 'HALF BATHROOM' THEN d.area ELSE 0 END) as half_bath_count,
            MAX(CASE WHEN d.detailTypeDescription = 'BEDROOMS' THEN d.area ELSE 0 END) as bed_count,
            MAX(CASE WHEN d.detailTypeDescription = 'POOL RES CONC' THEN 1 ELSE 0 END) as has_pool,
            MAX(CASE WHEN d.detailTypeDescription = 'SPA CONCRETE' THEN 1 ELSE 0 END) as has_spa,
            MAX(CASE WHEN d.detailTypeDescription = 'OUTDOOR KITCHEN' THEN 1 ELSE 0 END) as has_outdoor_kitchen,
            MAX(CASE WHEN d.detailTypeDescription = 'FIREPLACE' THEN d.area ELSE 0 END) as fireplace_count,
            SUM(CASE WHEN d.detailTypeDescription LIKE '%GARAGE%' THEN d.area ELSE 0 END) as garage_area
        FROM general g
        LEFT JOIN value_history v ON g.pAccountID = v.pAccountID AND v.pYear = (SELECT MAX(pYear) FROM value_history)
        LEFT JOIN improvement i ON g.pAccountID = i.pAccountID
        LEFT JOIN land l ON g.pAccountID = l.pAccountID
        LEFT JOIN improvement_details d ON g.pAccountID = d.pAccountID
        LEFT JOIN parcel p ON g.pAccountID = p.pAccountID
        GROUP BY g.pAccountID
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    numeric_cols = [
        'ownerAppraisedValue', 'ownerImprovementValue', 'ownerLandValue',
        'livingArea', 'lotSizeSqft', 'yearBuilt', 'bath_count', 'half_bath_count',
        'bed_count', 'has_pool', 'has_spa', 'has_outdoor_kitchen', 'fireplace_count', 'garage_area'
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df = df.dropna(subset=['livingArea', 'yearBuilt', 'streetAddress'])
    df = df[df['ownerAppraisedValue'] > 0]
    df['PricePerSqFt'] = df['ownerImprovementValue'] / df['livingArea']
    df['pAccountID'] = df['pAccountID'].astype(str)
    df['tcad_link'] = "https://travis.prodigycad.com/property-detail/" + df['pID'].astype(str) + "/2026"

    if 'ownerName' in df.columns:
        builders = ['TOLL', 'TAYLOR MORRISON', 'PULTE', 'LENNAR', 'DR HORTON', 'D R HORTON', 'MERITAGE', 'KB HOME', 'ASHTON WOODS', 'PERRY HOMES']
        pattern = '|'.join(builders)
        mask_primary = df['ownerName'].astype(str).str.upper().str.contains(pattern, na=False)
        mask_secondary = df['ownerNameSecondary'].astype(str).str.upper().str.contains(pattern, na=False)
        df = df[~(mask_primary | mask_secondary)]

    return df

df = load_data()

# --- STATE MANAGEMENT ---
# Track user-excluded comps so exclusions persist when switching tabs/modes
if 'excluded_comps' not in st.session_state:
    st.session_state['excluded_comps'] = []

# --- SIDEBAR & ENGINE SETUP ---
app_mode = st.sidebar.radio("View Mode:", ["Interactive Dashboard", "Printable Report"])
st.sidebar.info("💡 **Tip:** Press `Ctrl+P` (or `Cmd+P` on Mac) to print this report. The sidebar will hide so only your clean evidence packet prints.")
st.sidebar.divider()

st.sidebar.header("1. Select Subject Property")
addresses = sorted(df[df['streetAddress'].str[0].str.isdigit()]['streetAddress'].unique())
selected_address = st.sidebar.selectbox("Address:", addresses)
subject = df[df['streetAddress'] == selected_address].copy().iloc[0]

st.sidebar.header("2. Engine Configuration")
mode = st.sidebar.radio("Comps Selection Engine:", ["Tax Advocate Strategy (Recommended)", "Simple (Manual Filters)"])

neighborhood_df = df[df['pAccountID'] != subject['pAccountID']].copy()

hedonic_features = [
    'livingArea', 'yearBuilt', 'lotSizeSqft', 'bath_count', 'half_bath_count',
    'bed_count', 'has_pool', 'has_spa', 'has_outdoor_kitchen', 'fireplace_count', 'garage_area'
]

comps = pd.DataFrame() 
subject_ratio = 1.0
sqft_variance = 0.20
age_variance = 10

if len(neighborhood_df) < 5:
    st.sidebar.error("Database contains fewer than 5 other properties.")
else:
    if mode == "Simple (Manual Filters)":
        sqft_variance = st.sidebar.slider("SqFt Tolerance (+/- %)", 5, 30, 10) / 100.0
        age_variance = st.sidebar.slider("Age Tolerance (+/- Years)", 0, 10, 3)
        min_sqft, max_sqft = subject['livingArea'] * (1 - sqft_variance), subject['livingArea'] * (1 + sqft_variance)
        min_year, max_year = subject['yearBuilt'] - age_variance, subject['yearBuilt'] + age_variance

        comps = neighborhood_df[
            (neighborhood_df['livingArea'].between(min_sqft, max_sqft)) &
            (neighborhood_df['yearBuilt'].between(min_year, max_year))
        ].copy()

        if len(comps) > 0:
            comps['Adjusted Imprv Value'] = comps['ownerImprovementValue']
            comps['Total Adjustments'] = 0
            comps['Assessment Ratio'] = 1.0 

    elif mode == "Tax Advocate Strategy (Recommended)":
        num_comps = st.sidebar.slider("Number of Comps for Median", 3, 15, 5)
        sqft_variance = st.sidebar.slider("Defensible SqFt Tolerance (+/- %)", 10, 50, 20) / 100.0
        age_variance = st.sidebar.slider("Defensible Age Tolerance (+/- Years)", 5, 50, 10)
        min_sqft, max_sqft = subject['livingArea'] * (1 - sqft_variance), subject['livingArea'] * (1 + sqft_variance)
        min_year, max_year = subject['yearBuilt'] - age_variance, subject['yearBuilt'] + age_variance

        legal_pool = neighborhood_df[
            (neighborhood_df['livingArea'].between(min_sqft, max_sqft)) &
            (neighborhood_df['yearBuilt'].between(min_year, max_year))
        ].copy()

        if len(legal_pool) < num_comps:
            st.sidebar.warning(f"Only **{len(legal_pool)}** properties match your tolerances. Widen them.")
        else:
            upper_bound = neighborhood_df['PricePerSqFt'].quantile(0.975)
            reg_data = neighborhood_df[neighborhood_df['PricePerSqFt'] <= upper_bound].copy()

            if len(reg_data) >= 10:
                X_reg = reg_data[hedonic_features]
                y_reg = reg_data['ownerImprovementValue']
                reg = LinearRegression().fit(X_reg, y_reg)
                coefs = dict(zip(hedonic_features, reg.coef_))
                
                subject_pred = reg.predict(pd.DataFrame([subject[hedonic_features]]))
                subject_ratio = subject['ownerImprovementValue'] / subject_pred[0]
                
                legal_pool['Predicted Imprv'] = reg.predict(legal_pool[hedonic_features])
                legal_pool['Assessment Ratio'] = legal_pool['ownerImprovementValue'] / legal_pool['Predicted Imprv']
            else:
                coefs = {feat: 0 for feat in hedonic_features}
                coefs['livingArea'] = 50.0; coefs['yearBuilt'] = -1000.0
                legal_pool['Assessment Ratio'] = 1.0
                subject_ratio = 1.0

            legal_pool['Total Adjustments'] = 0
            for feature in hedonic_features:
                adj_col = f'{feature}_adj'
                legal_pool[adj_col] = (subject[feature] - legal_pool[feature]) * coefs[feature]
                legal_pool['Total Adjustments'] += legal_pool[adj_col]

            legal_pool['Adjusted Imprv Value'] = legal_pool['ownerImprovementValue'] + legal_pool['Total Adjustments']
            legal_pool = legal_pool.sort_values(by='Adjusted Imprv Value', ascending=True)
            comps = legal_pool.head(num_comps).copy()

# --- CHART GENERATION HELPER ---
def build_visuals(final_comps, subject, subject_ratio, target_imprv, median_ratio):
    subj_plot = subject.to_dict()
    subj_plot['streetAddress'] = f"{subj_plot['streetAddress']} (Subject)"
    subj_plot['Adjusted Imprv Value'] = subj_plot['ownerImprovementValue']
    subj_plot['Assessment Ratio'] = subject_ratio
    subj_plot['is_subject'] = 'Subject Property'
    subj_plot['dot_size'] = 20

    comps_plot = final_comps.copy()
    comps_plot['is_subject'] = 'Comparable Property'
    comps_plot['dot_size'] = 8

    plot_df = pd.concat([pd.DataFrame([subj_plot]), comps_plot])

    plot_df_bar = plot_df.sort_values(by='Adjusted Imprv Value', ascending=True)
    fig_bar = px.bar(
        plot_df_bar, x='streetAddress', y='Adjusted Imprv Value', color='is_subject',
        color_discrete_map={'Subject Property': '#ef4444', 'Comparable Property': '#3b82f6'},
        labels={'streetAddress': 'Property', 'Adjusted Imprv Value': 'Equalized Imprv Value ($)'}
    )
    fig_bar.add_hline(y=target_imprv, line_dash="dash", line_color="green", annotation_text=f"Target Median: ${target_imprv:,.0f}")
    fig_bar.update_layout(showlegend=False)

    plot_df_ratio = plot_df.sample(frac=1, random_state=123) 
    fig_ratio = px.scatter(
        plot_df_ratio, x='streetAddress', y='Assessment Ratio', color='is_subject', size='dot_size',
        color_discrete_map={'Subject Property': '#ef4444', 'Comparable Property': '#3b82f6'},
        labels={'streetAddress': 'Property', 'Assessment Ratio': 'Assessment Ratio (Actual / Predicted)'}
    )
    fig_ratio.add_hline(y=1.0, line_dash="solid", line_color="green", annotation_text="1.0 = Perfect Equity")
    fig_ratio.add_hline(y=median_ratio, line_dash="dash", line_color="orange", annotation_text=f"Comp Median ({median_ratio:.2f})")
    fig_ratio.update_yaxes(tickformat=".2f")
    fig_ratio.update_layout(showlegend=True, legend_title_text='')

    plot_df['calc_lat'] = None; plot_df['calc_lon'] = None
    features = []

    for idx, row in plot_df.iterrows():
        if pd.notnull(row['geometry']):
            try:
                geom = json.loads(row['geometry'])
                features.append({"type": "Feature", "id": row['pAccountID'], "geometry": geom, "properties": {"address": row['streetAddress']}})
                ring = geom['coordinates'][0][0]
                plot_df.at[idx, 'calc_lon'] = sum([p[0] for p in ring]) / len(ring)
                plot_df.at[idx, 'calc_lat'] = sum([p[1] for p in ring]) / len(ring)
            except Exception:
                pass

    geojson_data = {"type": "FeatureCollection", "features": features}
    valid_scatter_df = plot_df.dropna(subset=['calc_lat', 'calc_lon'])
    
    if not valid_scatter_df.empty:
        min_lat, max_lat = valid_scatter_df['calc_lat'].min(), valid_scatter_df['calc_lat'].max()
        min_lon, max_lon = valid_scatter_df['calc_lon'].min(), valid_scatter_df['calc_lon'].max()
        smart_center_lat, smart_center_lon = (min_lat + max_lat) / 2, (min_lon + max_lon) / 2
        max_spread = max(max_lat - min_lat, max_lon - min_lon)
        smart_zoom = math.log2(360 / max_spread) - 1 if max_spread > 0 else 16
    else:
        smart_center_lat, smart_center_lon, smart_zoom = 30.2672, -97.7431, 15

    fig_map = px.choropleth_mapbox(
        plot_df, geojson=geojson_data, locations="pAccountID", color="is_subject",
        color_discrete_map={'Subject Property': '#ef4444', 'Comparable Property': '#3b82f6'},
        hover_name="streetAddress", center={"lat": smart_center_lat, "lon": smart_center_lon},
        zoom=smart_zoom, mapbox_style="open-street-map", opacity=0.6
    )
    if not valid_scatter_df.empty:
        fig_scatter = px.scatter_mapbox(
            valid_scatter_df, lat="calc_lat", lon="calc_lon", color="is_subject", size="dot_size", size_max=12,
            color_discrete_map={'Subject Property': '#991b1b', 'Comparable Property': '#1d4ed8'}
        )
        for trace in fig_scatter.data:
            trace.showlegend = False; trace.hoverinfo = 'skip'; trace.hovertemplate = None
            fig_map.add_trace(trace)
    fig_map.update_layout(showlegend=False, margin={"r": 0, "t": 0, "l": 0, "b": 0})

    return fig_map, fig_bar, fig_ratio


# --- INTERACTIVE DASHBOARD MODE ---
if app_mode == "Interactive Dashboard":
    st.info("💡 **Tip:** Be sure to select your specific address from the sidebar")
    st.title("⚖️ TCAD Equity Appraisal Challenger")
    
    st.subheader("Subject Property Details")
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Appraised", f"${subject['ownerAppraisedValue']:,.0f}")
    col2.metric("Improvement Value", f"${subject['ownerImprovementValue']:,.0f}")
    col3.metric("Living Area", f"{subject['livingArea']:,.0f} SqFt")
    col4.metric("Lot Size", f"{subject['lotSizeSqft']:,.0f} SqFt")
    col5.metric("Year Built", f"{subject['yearBuilt']:.0f}")
    st.divider()

    if len(comps) > 0:
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📋 1. Comp Selection", "🗺️ 2. Visuals & Ratios", "⚖️ 3. Methodology", "🔍 4. Export", "📄 5. TCAD Property Cards"
        ])

        with tab1:
            st.markdown("### Evidence Review & Selection")
            display_cols = ['tcad_link', 'streetAddress', 'yearBuilt', 'livingArea', 'ownerAppraisedValue', 'ownerImprovementValue', 'Assessment Ratio', 'Adjusted Imprv Value']
            editor_df = comps[display_cols].copy()
            
            # Map exclusions from session state
            editor_df.insert(0, 'Include', ~editor_df['streetAddress'].isin(st.session_state['excluded_comps']))

            rename_map = {'streetAddress': 'Address', 'yearBuilt': 'Year', 'livingArea': 'SqFt', 'ownerAppraisedValue': 'Total Value', 'ownerImprovementValue': 'Imprv Value'}
            editor_df = editor_df.rename(columns=rename_map)

            edited_df = st.data_editor(
                editor_df,
                column_config={
                    "Include": st.column_config.CheckboxColumn("Include", default=True),
                    "tcad_link": st.column_config.LinkColumn("TCAD", display_text="View"),
                    "Assessment Ratio": st.column_config.NumberColumn("Assessment Ratio", format="%.2f"),
                    "Adjusted Imprv Value": st.column_config.NumberColumn("Equalized Imprv Value", format="$%d"),
                },
                disabled=list(rename_map.values()) + ["Adjusted Imprv Value", "tcad_link", "Assessment Ratio"],
                hide_index=True, width="stretch"
            )
            
            # Update exclusions based on user interaction
            st.session_state['excluded_comps'] = edited_df[edited_df['Include'] == False]['Address'].tolist()
            kept_addresses = edited_df[edited_df['Include'] == True]['Address'].tolist()
            final_comps = comps[comps['streetAddress'].isin(kept_addresses)].copy()

            median_comp_ratio = 1.0; suggested_imprv_value = None; suggested_total_value = 0; reduction = 0
            if len(final_comps) > 0:
                suggested_imprv_value = final_comps['Adjusted Imprv Value'].median()
                suggested_total_value = suggested_imprv_value + subject['ownerLandValue']
                reduction = subject['ownerAppraisedValue'] - suggested_total_value
                median_comp_ratio = final_comps['Assessment Ratio'].median()

                st.markdown("### 💰 The Bottom Line")
                if reduction > 0:
                    st.success(f"Based on **{len(final_comps)}** selected properties, your target Equalized Total Value is **${suggested_total_value:,.0f}**.\n\nProposed Reduction: **${reduction:,.0f}**.")
                else:
                    st.error(f"Based on the selected properties, TCAD's valuation appears equitable. Target value: **${suggested_total_value:,.0f}**.")
            else:
                st.warning("Please include at least one property.")

        # Shared Visuals Processing
        if len(final_comps) > 0 and suggested_imprv_value is not None:
            fig_map, fig_bar, fig_ratio = build_visuals(final_comps, subject, subject_ratio, suggested_imprv_value, median_comp_ratio)
        else:
            fig_map = fig_bar = fig_ratio = None

        with tab2:
            if fig_map is not None:
                st.markdown("### The Equity Gap (Equalized Value)")
                st.plotly_chart(fig_bar, width="stretch", key="bar_tab2")
                st.divider()
                st.markdown("### The TCAD Assessment Ratio")
                st.plotly_chart(fig_ratio, width="stretch", key="ratio_tab2")
                st.divider()
                st.markdown("### Neighborhood Map")
                fig_map.update_layout(height=550)
                st.plotly_chart(fig_map, width="stretch", key="map_tab2")

        with tab3:
            st.markdown("### Equity Adjustment Methodology (Hedonic Pricing Model)")
            st.write("To ensure an objective comparison, the engine utilizes a **Multivariate Hedonic Pricing Model** to isolate the marginal contributory value of specific property characteristics. This approach mathematically eliminates subjective appraiser bias by deriving adjustment values directly from TCAD's own neighborhood data.")
            
            st.markdown("#### Step 1: Defining the Neighborhood Baseline")
            st.write("An Ordinary Least Squares (OLS) regression is performed on the universe of relevant neighborhood properties to determine the baseline value equation:")
            st.latex(r"\hat{V}_{imprv} = \beta_0 + \beta_1(\text{SqFt}) + \beta_2(\text{Age}) + \beta_3(\text{Baths}) + \dots + \epsilon")
            
            st.markdown("#### Step 2: Extracting Objective Adjustment Rates")
            st.write("The extracted $\\beta$ coefficients represent precisely how much TCAD penalizes or rewards physical differences in your specific subdivision (e.g., the assessed value of one additional square foot or bathroom).")
            if mode == "Tax Advocate Strategy (Recommended)" and len(final_comps) > 0:
                coef_df = pd.DataFrame([coefs]).T.reset_index()
                coef_df.columns = ['Feature', 'Extracted Value ($)']
                st.dataframe(coef_df.style.format({'Extracted Value ($)': '${:,.2f}'}), hide_index=True)
            
            st.markdown("#### Step 3: Property Normalization & Equalization")
            st.write("These coefficients are applied to the physical deltas between the Subject Property and the Comparable Properties to calculate precise adjustments:")
            st.latex(r"Adj_i = (\text{Subject}_i - \text{Comp}_i) \times \beta_i")
            st.write("These adjustments are summed and applied to the comparable property's base assessment to yield the **Equalized Improvement Value** (the assessed value of the comparable property as if it were physically identical to the subject):")
            st.latex(r"\text{Equalized Value}_{comp} = \text{Base TCAD Value}_{comp} + \sum Adj_i")

        with tab4:
            st.markdown("### 📥 Download Evidence Report")
            if len(final_comps) > 0:
                export_cols = ['pAccountID', 'tcad_link', 'ownerName', 'streetAddress', 'legalDescription', 'yearBuilt', 'livingArea', 'lotSizeSqft', 'ownerAppraisedValue', 'PricePerSqFt']
                if 'Total Adjustments' in final_comps.columns:
                    export_cols.extend([f"{f}_adj" for f in hedonic_features] + ['Total Adjustments', 'Adjusted Imprv Value'])
                export_df = final_comps[export_cols].copy()
                st.download_button(label="Download Full Evidence Report (CSV)", data=export_df.to_csv(index=False).encode('utf-8'), file_name='tcad_equity_evidence.csv', mime='text/csv', type="primary")

        with tab5:
            st.markdown("### 📄 Official TCAD Property Cards")
            st.write("Download the official Travis Central Appraisal District PDF record for your selected comparables. These documents serve as the certified baseline data for your evidence packet.")
            
            if len(final_comps) > 0:
                st.markdown("#### Batch Download (Comparables Only)")
                if 'comps_zip' not in st.session_state:
                    if st.button("🔄 Generate ZIP of All Comparable PDFs", type="primary"):
                        with st.spinner("Connecting to TCAD Servers..."):
                            token = get_tcad_token()
                            if token:
                                my_bar = st.progress(0, text="Fetching official PDFs from TCAD... Please wait.")
                                zip_buffer = io.BytesIO()
                                with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                                    total_comps = len(final_comps)
                                    for i, (idx, prop) in enumerate(final_comps.iterrows()):
                                        my_bar.progress(i / total_comps, text=f"Fetching {prop['streetAddress']} ({i+1}/{total_comps})...")
                                        pdf_data = fetch_property_card_pdf(token, prop['pID'], prop['pAccountID'])
                                        if pdf_data:
                                            clean_address = prop['streetAddress'].replace(" ", "_")
                                            zip_file.writestr(f"TCAD_Comp_{prop['pAccountID']}_{clean_address}.pdf", pdf_data)
                                        time.sleep(0.5) 
                                    my_bar.progress(1.0, text="✅ All PDFs retrieved and packaged successfully!")
                                st.session_state['comps_zip'] = zip_buffer.getvalue()
                            else:
                                st.error("Failed to authenticate with TCAD.")
                
                if 'comps_zip' in st.session_state:
                    st.download_button(label="⬇️ Download All Comps (ZIP File)", data=st.session_state['comps_zip'], file_name="TCAD_Comparable_Evidence.zip", type="primary")

                st.divider()
                st.markdown("#### Individual Property Cards")
                all_properties = pd.concat([pd.DataFrame([subject]), final_comps])
                
                for idx, prop in all_properties.iterrows():
                    col1, col2, col3 = st.columns([4, 2, 2])
                    is_sub = "⭐ (Subject Property)" if prop['pAccountID'] == subject['pAccountID'] else ""
                    col1.markdown(f"**{prop['streetAddress']}** {is_sub}")
                    col1.caption(f"Account ID: {prop['pAccountID']} | PID: {prop['pID']}")
                    
                    pdf_key = f"pdf_bytes_{prop['pAccountID']}"
                    
                    if pdf_key not in st.session_state:
                        if col2.button("Retrieve PDF", key=f"fetch_btn_{prop['pAccountID']}"):
                            with st.spinner("Generating..."):
                                token = get_tcad_token()
                                if token:
                                    pdf_data = fetch_property_card_pdf(token, prop['pID'], prop['pAccountID'])
                                    if pdf_data:
                                        st.session_state[pdf_key] = pdf_data
                                        col3.download_button(label="⬇️ Download PDF", data=pdf_data, file_name=f"TCAD_Card_{prop['pAccountID']}.pdf", key=f"dl_btn_{prop['pAccountID']}_temp")
                                    else:
                                        st.error("Failed to generate PDF.")
                                else:
                                    st.error("Failed to connect.")
                    else:
                        col2.success("✅ PDF Retrieved")
                        col3.download_button(label="⬇️ Download PDF File", data=st.session_state[pdf_key], file_name=f"TCAD_Card_{prop['pAccountID']}.pdf", key=f"dl_btn_{prop['pAccountID']}")
                    st.divider()


# --- PRINTABLE REPORT MODE ---
elif app_mode == "Printable Report":
    st.markdown("""
    <style>
    .block-container { max-width: 48rem !important; margin: auto; }
    @media print {
        .block-container { max-width: 100% !important; width: 100% !important; }
        [data-testid="stSidebar"], header, .stAlert, .stButton { display: none !important; }
    }
    </style>
    """, unsafe_allow_html=True)
    
    final_comps = comps[~comps['streetAddress'].isin(st.session_state['excluded_comps'])].copy()

    if len(final_comps) > 0:
        median_comp_ratio = final_comps['Assessment Ratio'].median()
        suggested_imprv_value = final_comps['Adjusted Imprv Value'].median()
        suggested_total_value = suggested_imprv_value + subject['ownerLandValue']
        reduction = subject['ownerAppraisedValue'] - suggested_total_value
        
        fig_map, fig_bar, fig_ratio = build_visuals(final_comps, subject, subject_ratio, suggested_imprv_value, median_comp_ratio)
        shuffled_comps = final_comps.sample(frac=1, random_state=42).copy()
        
        st.markdown("<h1 style='text-align: center;'>EVIDENCE PACKET: UNEQUAL APPRAISAL PROTEST</h1>", unsafe_allow_html=True)
        st.markdown(f"<h3 style='text-align: center; color: gray;'>Texas Property Tax Code § 41.43(b)(3)</h3>", unsafe_allow_html=True)
        st.divider()
        
        st.markdown("### I. Subject Property Summary")
        r_col1, r_col2 = st.columns(2)
        with r_col1:
            st.markdown(f"**Address:** {subject['streetAddress']}<br>**Account ID:** {subject['pAccountID']}", unsafe_allow_html=True)
        with r_col2:
            st.markdown(f"**Living Area:** {subject['livingArea']:,.0f} SqFt<br>**Year Built:** {subject['yearBuilt']:.0f}", unsafe_allow_html=True)

        st.markdown("### II. Requested Valuation")
        req_col1, req_col2, req_col3 = st.columns(3)
        req_col1.metric("Current TCAD Total Value", f"${subject['ownerAppraisedValue']:,.0f}")
        req_col2.metric("Target Equalized Total Value", f"${suggested_total_value:,.0f}")
        req_col3.metric("Reduction Requested", f"${reduction:,.0f}" if reduction > 0 else "$0")
        st.divider()
        
        st.markdown("### III. Comparable Selection Criteria (The \"Universe of Properties\")")
        sqft_pct_display = int(sqft_variance * 100)
        st.write(f"""
        To ensure a legally "reasonable number" of comparable properties without selection bias, a strict physical and geographic filter was applied. The comparable properties selected represent homes within the immediate neighborhood built within **+/- {age_variance} years** of the subject property, and within **+/- {sqft_pct_display}%** of the subject's gross living area. 

        The **{len(final_comps)} properties** included in this analysis represent the complete universe of highly comparable properties meeting this strict baseline criteria. This objectively satisfies the statutory requirement for a reasonable number of appropriately adjusted comparable properties, free from cherry-picking.
        """)
        st.divider()

        st.markdown("### IV. Equity Adjustment Methodology (Hedonic Pricing Model)")
        st.write("To ensure an objective comparison, this report utilizes a **Multivariate Hedonic Pricing Model** to isolate the marginal contributory value of specific property characteristics. This approach mathematically eliminates subjective appraiser bias.")
        
        st.markdown("#### Step 1: Defining the Neighborhood Baseline")
        st.write("An Ordinary Least Squares (OLS) regression is performed on the universe of relevant neighborhood properties to determine the baseline value equation:")
        st.latex(r"\hat{V}_{imprv} = \beta_0 + \beta_1(\text{SqFt}) + \beta_2(\text{Age}) + \beta_3(\text{Baths}) + \dots + \epsilon")
        
        st.markdown("#### Step 2: Property Normalization & Equalization")
        st.write("The extracted $\\beta$ coefficients are applied to the physical deltas between the Subject Property and the Comparable Properties to calculate precise adjustments:")
        st.latex(r"Adj_i = (\text{Subject}_i - \text{Comp}_i) \times \beta_i")
        st.write("These adjustments are summed and applied to the comparable property's base assessment to yield the **Equalized Improvement Value** (the assessed value of the comparable property as if it were physically identical to the subject):")
        st.latex(r"\text{Equalized Value}_{comp} = \text{Base TCAD Value}_{comp} + \sum Adj_i")
        st.divider()
        
        st.markdown("### V. Equity Assessment Ratio Analysis")
        reduction_pct = (reduction / subject['ownerAppraisedValue']) * 100 if subject['ownerAppraisedValue'] > 0 else 0
        st.write(f"""
        The Assessment Ratio measures the actual appraised value against the model's predicted equitable value. A ratio of 1.0 represents perfect equity. 

        As demonstrated in the scatter plot below, the appraisal district has assessed the subject property at **{subject_ratio * 100:.1f}%** of its equitable modeled value. Conversely, the district is consistently assessing the immediate, highly comparable properties at a significantly discounted rate, with a median assessment ratio of **{median_comp_ratio * 100:.1f}%**. 

        To establish true equity and remedy this unequal appraisal, the subject property's valuation must be reduced to reflect the exact same median assessment ratio discount applied to these comparable properties. **This equates to a requested reduction of ${reduction:,.0f}, or {reduction_pct:.1f}% of the current assessed value.**
        """)
        fig_ratio.update_layout(height=350, margin={"r": 0, "t": 20, "l": 0, "b": 0})
        st.plotly_chart(fig_ratio, width="stretch", key="ratio_tab5", 
    config={'responsive': True})
        st.divider()

        st.markdown("### VI. Geographic Neighborhood Map")
        fig_map.update_layout(height=400, margin={"r": 0, "t": 0, "l": 0, "b": 0})
        st.plotly_chart(fig_map, width="stretch", key="map_tab5")
        st.divider()

        st.markdown("### VII. Comparable Analysis Summary")
        report_table = shuffled_comps[['pAccountID', 'streetAddress', 'livingArea', 'ownerImprovementValue', 'Total Adjustments', 'Adjusted Imprv Value']].copy()
        report_table.columns = ['Account ID', 'Address', 'SqFt', 'TCAD Imprv Value', 'Adjustments Applied', 'Equalized Imprv Value']
        st.dataframe(
            report_table.style.format({
                'TCAD Imprv Value': '${:,.0f}', 'Adjustments Applied': '${:,.0f}', 'Equalized Imprv Value': '${:,.0f}', 'SqFt': '{:,.0f}'
            }), hide_index=True, width="stretch"
        )
        st.divider()
        
        st.markdown("### VIII. Comparable Property Details & Adjustment Ledgers")
        st.write("The following charts detail the specific physical adjustments made to align each comparable property's assessed value with the subject property, following the equations outlined in Section IV.")
        
        for idx, comp in shuffled_comps.iterrows():
            st.markdown(f"**Comp:** {comp['streetAddress']} | **Base:** \${comp['ownerImprovementValue']:,.0f} | **Equalized:** \${comp['Adjusted Imprv Value']:,.0f}")
            
            w_x = ["Base Imprv"]; w_y = [comp['ownerImprovementValue']]; w_measure = ["relative"]
            w_text = [f"${comp['ownerImprovementValue'] / 1000:.1f}k"]

            displayed_adj_sum = 0
            for feature in hedonic_features:
                adj_val = comp[f'{feature}_adj']
                if abs(adj_val) > 100: 
                    w_x.append(feature.replace('_', ' ').title()); w_y.append(adj_val)
                    w_measure.append("relative"); w_text.append(f"${adj_val / 1000:.1f}k")
                    displayed_adj_sum += adj_val

            residual = comp['Total Adjustments'] - displayed_adj_sum
            if abs(residual) > 1:
                w_x.append("Other Adjs"); w_y.append(residual)
                w_measure.append("relative"); w_text.append(f"${residual / 1000:.1f}k")

            w_x.append("Equalized Value"); w_y.append(comp['Adjusted Imprv Value'])
            w_measure.append("total"); w_text.append(f"${comp['Adjusted Imprv Value'] / 1000:.1f}k")

            comp_waterfall = go.Figure(go.Waterfall(
                orientation="v", measure=w_measure, x=w_x, textposition="outside", text=w_text, y=w_y,
                connector={"line": {"color": "rgb(63, 63, 63)"}}
            ))
            comp_waterfall.update_layout(height=280, margin={"r": 0, "t": 20, "l": 0, "b": 0})
            st.plotly_chart(comp_waterfall, width="stretch", key=f"waterfall_tab5_{comp['pAccountID']}",
    config={'responsive': True})

        st.divider()
        # st.markdown("### IX. Affirmation")
        # st.write("I affirm that the evidence presented above is derived directly from the Travis Central Appraisal District's certified records and accurately reflects an objective equity analysis.")
        # st.write("")
        # st.write("")
        # st.markdown("**Signature:** ___________________________________________________      **Date:** ___________________")
        # st.write(f"*{subject.get('ownerName', 'Property Owner')}*")
    else:
        st.error("No valid comparable properties selected. Switch back to Interactive Dashboard to adjust filters.")
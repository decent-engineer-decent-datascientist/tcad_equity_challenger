import streamlit as st
import pandas as pd
import sqlite3
import json
import math
import plotly.express as px
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression

# --- PAGE CONFIG ---
st.set_page_config(page_title="TCAD Equity Challenger", layout="wide", page_icon="🏠")

# --- DATA LOADING ---
@st.cache_data
def load_data():
    conn = sqlite3.connect("tcad_data.db")

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

    # BUILDER INVENTORY FILTER — removes active builder inventory to clean dataset
    if 'ownerName' in df.columns:
        builders = ['TOLL', 'TAYLOR MORRISON', 'PULTE', 'LENNAR', 'DR HORTON', 'D R HORTON', 'MERITAGE', 'KB HOME', 'ASHTON WOODS', 'PERRY HOMES']
        pattern = '|'.join(builders)
        mask_primary = df['ownerName'].astype(str).str.upper().str.contains(pattern, na=False)
        mask_secondary = df['ownerNameSecondary'].astype(str).str.upper().str.contains(pattern, na=False)
        df = df[~(mask_primary | mask_secondary)]

    return df

df = load_data()

# --- HEADER ---
st.title("⚖️ TCAD Equity Appraisal Challenger")
st.markdown("Generate data-driven evidence to challenge your property taxes based on unequal appraisal (Texas Property Tax Code § 41.43(b)(3)).")

# --- SUBJECT PROPERTY SELECTION ---
addresses = sorted(df[df['streetAddress'].str[0].str.isdigit()]['streetAddress'].unique())
selected_address = st.selectbox("Search for your property:", addresses)
subject = df[df['streetAddress'] == selected_address].iloc[0]

# Display Subject Dashboard
st.subheader("Subject Property Details")
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Appraised", f"${subject['ownerAppraisedValue']:,.0f}")
col2.metric("Improvement Value", f"${subject['ownerImprovementValue']:,.0f}")
col3.metric("Living Area", f"{subject['livingArea']:,.0f} SqFt")
col4.metric("Lot Size", f"{subject['lotSizeSqft']:,.0f} SqFt")
col5.metric("Year Built", f"{subject['yearBuilt']:.0f}")

st.divider()

# --- ENGINE LOGIC ---
st.sidebar.header("Engine Configuration")
mode = st.sidebar.radio("Comps Selection Engine:", ["Tax Advocate Strategy (Recommended)", "Simple (Manual Filters)"])

neighborhood_df = df[df['pAccountID'] != subject['pAccountID']].copy()

hedonic_features = [
    'livingArea', 'yearBuilt', 'lotSizeSqft', 'bath_count', 'half_bath_count',
    'bed_count', 'has_pool', 'has_spa', 'has_outdoor_kitchen', 'fireplace_count', 'garage_area'
]

comps = pd.DataFrame()  # Ensure comps is always defined

if len(neighborhood_df) < 5:
    st.error("Your underlying database contains fewer than 5 other properties. Check your SQL extraction process.")
else:
    if mode == "Simple (Manual Filters)":
        st.sidebar.markdown("### Manual Filters")
        sqft_variance = st.sidebar.slider("SqFt Tolerance (+/- %)", 5, 30, 10) / 100.0
        age_variance = st.sidebar.slider("Age Tolerance (+/- Years)", 0, 10, 3)

        min_sqft = subject['livingArea'] * (1 - sqft_variance)
        max_sqft = subject['livingArea'] * (1 + sqft_variance)
        min_year = subject['yearBuilt'] - age_variance
        max_year = subject['yearBuilt'] + age_variance

        comps = neighborhood_df[
            (neighborhood_df['livingArea'].between(min_sqft, max_sqft)) &
            (neighborhood_df['yearBuilt'].between(min_year, max_year))
        ].copy()

        if len(comps) > 0:
            comps['Adjusted Imprv Value'] = comps['ownerImprovementValue']

    elif mode == "Tax Advocate Strategy (Recommended)":
        st.sidebar.markdown("### Equity Optimization")
        num_comps = st.sidebar.slider("Number of Comps for Median", 3, 15, 5)
        sqft_variance = st.sidebar.slider("Defensible SqFt Tolerance (+/- %)", 10, 50, 20) / 100.0
        age_variance = st.sidebar.slider("Defensible Age Tolerance (+/- Years)", 5, 50, 10)

        min_sqft = subject['livingArea'] * (1 - sqft_variance)
        max_sqft = subject['livingArea'] * (1 + sqft_variance)
        min_year = subject['yearBuilt'] - age_variance
        max_year = subject['yearBuilt'] + age_variance

        legal_pool = neighborhood_df[
            (neighborhood_df['livingArea'].between(min_sqft, max_sqft)) &
            (neighborhood_df['yearBuilt'].between(min_year, max_year))
        ].copy()

        if len(legal_pool) < num_comps:
            # BUG 2 FIX: Specific guidance instead of generic "widen filters"
            st.warning(
                f"Only **{len(legal_pool)}** properties match your current tolerances, but you requested **{num_comps}** comps. "
                f"Try reducing *Number of Comps for Median* to **{max(3, len(legal_pool))}**, or widen the SqFt/Age tolerances."
            )
        else:
            # BUG 10 FIX: One-sided trim — remove only the top 2.5% of $/SqFt outliers
            # This preserves favorable low-end data while cleaning data errors on the high end
            upper_bound = neighborhood_df['PricePerSqFt'].quantile(0.975)
            reg_data = neighborhood_df[neighborhood_df['PricePerSqFt'] <= upper_bound].copy()

            if len(reg_data) >= 10:
                X_reg = reg_data[hedonic_features]
                y_reg = reg_data['ownerImprovementValue']
                reg = LinearRegression().fit(X_reg, y_reg)
                coefs = dict(zip(hedonic_features, reg.coef_))
            else:
                coefs = {feat: 0 for feat in hedonic_features}
                coefs['livingArea'] = 50.0
                coefs['yearBuilt'] = -1000.0

            legal_pool['Total Adjustments'] = 0
            for feature in hedonic_features:
                adj_col = f'{feature}_adj'
                legal_pool[adj_col] = (subject[feature] - legal_pool[feature]) * coefs[feature]
                legal_pool['Total Adjustments'] += legal_pool[adj_col]

            legal_pool['Adjusted Imprv Value'] = legal_pool['ownerImprovementValue'] + legal_pool['Total Adjustments']
            legal_pool = legal_pool.sort_values(by='Adjusted Imprv Value', ascending=True)
            comps = legal_pool.head(num_comps).copy()

# --- REPORT GENERATION & INTERACTIVE REVIEW ---
if len(comps) > 0:

    tab1, tab2, tab3, tab4 = st.tabs(["📋 1. Comp Selection", "🗺️ 2. Geographic & Visual Evidence", "⚖️ 3. Adjustment Methodology", "🔍 4. Deep Dive & Export"])

    # --- TAB 1: SELECTION ---
    with tab1:
        st.markdown("### Evidence Review & Selection")

        display_cols = ['tcad_link', 'streetAddress', 'yearBuilt', 'livingArea', 'ownerAppraisedValue', 'ownerImprovementValue', 'PricePerSqFt', 'Adjusted Imprv Value']
        editor_df = comps[display_cols].copy()
        editor_df.insert(0, 'Include', True)

        rename_map = {
            'streetAddress': 'Address', 'yearBuilt': 'Year', 'livingArea': 'SqFt',
            'ownerAppraisedValue': 'Total Value', 'ownerImprovementValue': 'Imprv Value', 'PricePerSqFt': '$/SqFt'
        }
        editor_df = editor_df.rename(columns=rename_map)

        edited_df = st.data_editor(
            editor_df,
            column_config={
                "Include": st.column_config.CheckboxColumn("Include", default=True),
                "tcad_link": st.column_config.LinkColumn("TCAD", display_text="View on TCAD"),
                "Year": st.column_config.NumberColumn("Year", format="%d"),
                "SqFt": st.column_config.NumberColumn("SqFt", format="%d"),
                "Total Value": st.column_config.NumberColumn("Total Value", format="$%d"),
                "Imprv Value": st.column_config.NumberColumn("Imprv Value", format="$%d"),
                "$/SqFt": st.column_config.NumberColumn("$/SqFt", format="$%.2f"),
                "Adjusted Imprv Value": st.column_config.NumberColumn("Adjusted Imprv Value", format="$%d"),
            },
            disabled=list(rename_map.values()) + ["Adjusted Imprv Value", "tcad_link"],
            hide_index=True,
            use_container_width=True
        )

        kept_addresses = edited_df[edited_df['Include'] == True]['Address'].tolist()
        final_comps = comps[comps['streetAddress'].isin(kept_addresses)].copy()

        # BUG 1 FIX: Compute these values here so they're available to all subsequent tabs
        if len(final_comps) > 0:
            suggested_imprv_value = final_comps['Adjusted Imprv Value'].median()
            suggested_total_value = suggested_imprv_value + subject['ownerLandValue']
            reduction = subject['ownerAppraisedValue'] - suggested_total_value

            st.markdown("### 💰 The Bottom Line")
            if reduction > 0:
                st.success(
                    f"Based on **{len(final_comps)}** selected properties, your target Equalized Total Value is "
                    f"**${suggested_total_value:,.0f}**.\n\nProposed Reduction: **${reduction:,.0f}**."
                )
            else:
                st.error(
                    f"Based on the selected properties, TCAD's valuation appears equitable. "
                    f"Target value: **${suggested_total_value:,.0f}**."
                )
        else:
            suggested_imprv_value = None
            st.warning("You have deselected all properties. Please include at least one.")

    # --- TAB 2: MAP AND VISUALS ---
    with tab2:
        if len(final_comps) > 0 and suggested_imprv_value is not None:
            subj_plot = subject.to_dict()
            subj_plot['streetAddress'] = f"{subj_plot['streetAddress']} (Subject)"
            subj_plot['Adjusted Imprv Value'] = subj_plot['ownerImprovementValue']
            subj_plot['is_subject'] = 'Subject Property'
            subj_plot['dot_size'] = 20

            comps_plot = final_comps.copy()
            comps_plot['is_subject'] = 'Comparable Property'
            comps_plot['dot_size'] = 8

            plot_df = pd.concat([pd.DataFrame([subj_plot]), comps_plot])

            st.markdown("### Geographic Parcel Map")

            plot_df['calc_lat'] = None
            plot_df['calc_lon'] = None
            features = []

            for idx, row in plot_df.iterrows():
                if pd.notnull(row['geometry']):
                    try:
                        geom = json.loads(row['geometry'])
                        features.append({
                            "type": "Feature",
                            "id": row['pAccountID'],
                            "geometry": geom,
                            "properties": {"address": row['streetAddress']}
                        })
                        ring = geom['coordinates'][0][0]
                        avg_lon = sum([p[0] for p in ring]) / len(ring)
                        avg_lat = sum([p[1] for p in ring]) / len(ring)
                        plot_df.at[idx, 'calc_lon'] = avg_lon
                        plot_df.at[idx, 'calc_lat'] = avg_lat
                    except Exception:
                        pass

            geojson_data = {"type": "FeatureCollection", "features": features}

            valid_scatter_df = plot_df.dropna(subset=['calc_lat', 'calc_lon'])
            if not valid_scatter_df.empty:
                min_lat, max_lat = valid_scatter_df['calc_lat'].min(), valid_scatter_df['calc_lat'].max()
                min_lon, max_lon = valid_scatter_df['calc_lon'].min(), valid_scatter_df['calc_lon'].max()
                smart_center_lat = (min_lat + max_lat) / 2
                smart_center_lon = (min_lon + max_lon) / 2
                max_spread = max(max_lat - min_lat, max_lon - min_lon)
                smart_zoom = math.log2(360 / max_spread) - 1 if max_spread > 0 else 16
            else:
                smart_center_lat, smart_center_lon, smart_zoom = 30.2672, -97.7431, 15

            if len(features) > 0:
                fig_map = px.choropleth_mapbox(
                    plot_df, geojson=geojson_data, locations="pAccountID", color="is_subject",
                    color_discrete_map={'Subject Property': '#ef4444', 'Comparable Property': '#3b82f6'},
                    hover_name="streetAddress", center={"lat": smart_center_lat, "lon": smart_center_lon},
                    zoom=smart_zoom, mapbox_style="open-street-map", opacity=0.6, height=550
                )
                if not valid_scatter_df.empty:
                    fig_scatter = px.scatter_mapbox(
                        valid_scatter_df, lat="calc_lat", lon="calc_lon", color="is_subject",
                        size="dot_size", size_max=12,
                        color_discrete_map={'Subject Property': '#991b1b', 'Comparable Property': '#1d4ed8'}
                    )
                    for trace in fig_scatter.data:
                        trace.showlegend = False
                        trace.hoverinfo = 'skip'
                        trace.hovertemplate = None
                        fig_map.add_trace(trace)
                fig_map.update_layout(showlegend=False, margin={"r": 0, "t": 0, "l": 0, "b": 0})
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.info("Parcel geometries could not be parsed.")

            st.divider()
            st.markdown("### The Equity Gap")
            plot_df_bar = plot_df.sort_values(by='Adjusted Imprv Value', ascending=True)
            fig_bar = px.bar(
                plot_df_bar, x='streetAddress', y='Adjusted Imprv Value', color='is_subject',
                color_discrete_map={'Subject Property': '#ef4444', 'Comparable Property': '#3b82f6'}
            )
            fig_bar.add_hline(
                y=suggested_imprv_value, line_dash="dash", line_color="green",
                annotation_text=f"Target Median: ${suggested_imprv_value:,.0f}"
            )
            fig_bar.update_layout(showlegend=False)
            st.plotly_chart(fig_bar, use_container_width=True)

        elif len(final_comps) == 0:
            st.warning("No comps selected. Return to Tab 1 and include at least one property.")

    # --- TAB 3: ADJUSTMENT METHODOLOGY ---
    with tab3:
        if mode == "Tax Advocate Strategy (Recommended)" and len(final_comps) > 0 and 'Total Adjustments' in final_comps.columns:
            st.markdown("### Step 1: Neighborhood Multivariate Normalization")
            st.write(
                "A multivariate linear regression model objectively calculates how much the appraisal district "
                "penalizes or rewards physical differences in this specific subdivision."
            )

            coef_df = pd.DataFrame([coefs]).T.reset_index()
            coef_df.columns = ['Feature', 'Value ($)']
            st.dataframe(coef_df.style.format({'Value ($)': '${:,.2f}'}), use_container_width=True, hide_index=True)

            st.divider()
            st.markdown("### Step 2: Individual Property Adjustments")

            waterfall_comp_address = st.selectbox(
                "Select Comparable Property to view adjustment math:",
                final_comps['streetAddress'].tolist()
            )
            rep_comp = final_comps[final_comps['streetAddress'] == waterfall_comp_address].iloc[0]

            # BUG 3 FIX: Track the sum of displayed adjustments and add an "Other" bar
            # for any remainder, so the waterfall total is always internally consistent.
            waterfall_x = ["Base Imprv Value"]
            waterfall_y = [rep_comp['ownerImprovementValue']]
            waterfall_measure = ["relative"]
            waterfall_text = [f"${rep_comp['ownerImprovementValue'] / 1000:.1f}k"]

            displayed_adj_sum = 0
            for feature in hedonic_features:
                adj_val = rep_comp[f'{feature}_adj']
                if abs(adj_val) > 100:
                    waterfall_x.append(feature)
                    waterfall_y.append(adj_val)
                    waterfall_measure.append("relative")
                    waterfall_text.append(f"${adj_val / 1000:.1f}k")
                    displayed_adj_sum += adj_val

            # Residual accounts for all sub-$100 adjustments excluded from the chart
            residual = rep_comp['Total Adjustments'] - displayed_adj_sum
            if abs(residual) > 1:
                waterfall_x.append("Other Adjustments")
                waterfall_y.append(residual)
                waterfall_measure.append("relative")
                waterfall_text.append(f"${residual / 1000:.1f}k")

            waterfall_x.append("Final Adjusted")
            waterfall_y.append(rep_comp['Adjusted Imprv Value'])
            waterfall_measure.append("total")
            waterfall_text.append(f"${rep_comp['Adjusted Imprv Value'] / 1000:.1f}k")

            fig_water = go.Figure(go.Waterfall(
                name="Adjustments", orientation="v", measure=waterfall_measure, x=waterfall_x,
                textposition="outside", text=waterfall_text, y=waterfall_y,
                connector={"line": {"color": "rgb(63, 63, 63)"}}
            ))
            fig_water.update_layout(title=f"Adjustments applied to: {rep_comp['streetAddress']}", height=450)
            st.plotly_chart(fig_water, use_container_width=True)

            st.markdown("#### Complete Adjustment Ledger")
            ledger_cols = (
                ['streetAddress', 'ownerImprovementValue'] +
                [f"{f}_adj" for f in hedonic_features] +
                ['Total Adjustments', 'Adjusted Imprv Value']
            )
            ledger_df = final_comps[ledger_cols].rename(columns={
                'streetAddress': 'Address',
                'ownerImprovementValue': 'Base Imprv Value'
            })
            st.dataframe(
                ledger_df.style.format(lambda x: f"${x:,.0f}" if isinstance(x, (int, float)) else x),
                use_container_width=True, hide_index=True
            )

    # --- TAB 4: DEEP DIVE & EXPORT ---
    with tab4:
        if len(final_comps) > 0:
            st.markdown("### 📥 Download Evidence Report")
            export_cols = [
                'pAccountID', 'tcad_link', 'ownerName', 'streetAddress', 'legalDescription',
                'yearBuilt', 'livingArea', 'lotSizeSqft', 'ownerAppraisedValue', 'PricePerSqFt'
            ]
            if 'Total Adjustments' in final_comps.columns:
                export_cols.extend([f"{f}_adj" for f in hedonic_features] + ['Total Adjustments', 'Adjusted Imprv Value'])
            else:
                export_cols.append('Adjusted Imprv Value')

            export_df = final_comps[export_cols].copy()
            st.download_button(
                label="Download Full Evidence Report (CSV)",
                data=export_df.to_csv(index=False).encode('utf-8'),
                file_name='tcad_equity_evidence.csv',
                mime='text/csv',
                type="primary"
            )

            st.divider()
            st.markdown("### 🔍 Property Deep Dive")
            all_addresses = [subject['streetAddress']] + final_comps['streetAddress'].tolist()
            view_address = st.selectbox("Select Property to Audit:", all_addresses)

            detail_data = (
                subject if view_address == subject['streetAddress']
                else final_comps[final_comps['streetAddress'] == view_address].iloc[0]
            )

            st.markdown(f"**TCAD Official Record:** [Click to View Property {detail_data.get('pID', '')}]({detail_data.get('tcad_link', '')})")

            # BUG 4 FIX: pd.DataFrame([detail_data]) correctly produces a 1-row DataFrame from a Series
            display_data = pd.DataFrame([detail_data])
            if 'geometry' in display_data.columns:
                display_data = display_data.drop(columns=['geometry'])
            st.dataframe(display_data, hide_index=True, use_container_width=True)

else:
    st.warning("No properties match your current filters. Please widen the tolerances in the sidebar.")
import streamlit as st
from datetime import date, datetime, timedelta
import requests
import pandas as pd
import numpy as np
import hashlib
import altair as alt
import plotly.express as px
import os
import sqlite3

from etl.etl_pipeline import ETLPipeline

from utils.db_utils import (
    get_agencies_data,
    get_cfr_sections_data,
    get_cfr_references_data
)

# --- Database Check Functions ---

def check_database_exists(db_path="data/cfr.db"):
    """
    Check if the database file exists
    """
    return os.path.exists(db_path)

def check_database_has_data(db_path="data/cfr.db"):
    """
    Check if database exists and has data in required tables
    """
    if not check_database_exists(db_path):
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        required_tables = ['agencies', 'cfr_sections', 'cfr_references']
        
        for table in required_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            if count == 0:
                conn.close()
                return False
        
        conn.close()
        return True
    
    except Exception as e:
        st.error(f"Error checking database: {e}")
        return False

def safe_load_data():
    """
    Safely load data with error handling
    """
    try:
        agency_df = get_agencies_data()
        section_df = get_cfr_sections_data()
        reference_df = get_cfr_references_data()
        
        if agency_df.empty or section_df.empty or reference_df.empty:
            return None, None, None
        
        return agency_df, section_df, reference_df
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None

# --- Enhanced Dashboard Functions ---

def create_comprehensive_agency_analysis(agency_df, section_df, reference_df):
    """
    Creates comprehensive analysis by properly joining reference_df with section_df
    based on title, chapter, part matching logic, then joining with agency_df
    """
    st.info("🔄 Processing data joins with custom matching logic...")
    
    reference_df = reference_df.copy()
    section_df = section_df.copy()
    agency_df = agency_df.copy()
    
    # Debug: Check column names and data types
    st.write("**Debug - Column Info:**")
    st.write(f"Reference columns: {reference_df.columns.tolist()}")
    st.write(f"Section columns: {section_df.columns.tolist()}")
    st.write(f"Agency columns: {agency_df.columns.tolist()}")
    
    # For reference_df
    title_col_ref = None
    chapter_col_ref = None
    part_col_ref = None
    
    for col in reference_df.columns:
        if 'title' in col.lower():
            title_col_ref = col
        elif 'chapter' in col.lower():
            chapter_col_ref = col
        elif 'part' in col.lower():
            part_col_ref = col
    
    title_col_sec = None
    chapter_col_sec = None
    part_col_sec = None
    text_col_sec = None
    
    for col in section_df.columns:
        if 'title' in col.lower() and 'number' in col.lower():
            title_col_sec = col
        elif 'chapter' in col.lower() and 'number' in col.lower():
            chapter_col_sec = col
        elif 'part' in col.lower() and 'number' in col.lower():
            part_col_sec = col
        elif any(keyword in col.lower() for keyword in ['text', 'body', 'content']):
            text_col_sec = col
    
    st.write(f"**Identified columns:**")
    st.write(f"Reference - Title: {title_col_ref}, Chapter: {chapter_col_ref}, Part: {part_col_ref}")
    st.write(f"Section - Title: {title_col_sec}, Chapter: {chapter_col_sec}, Part: {part_col_sec}, Text: {text_col_sec}")
    
    if not all([title_col_ref, title_col_sec]):
        st.error("Could not identify title columns for joining!")
        return None
    
    reference_df[title_col_ref] = reference_df[title_col_ref].astype(str)
    section_df[title_col_sec] = section_df[title_col_sec].astype(str)
    
    if chapter_col_ref and chapter_col_sec:
        reference_df[chapter_col_ref] = reference_df[chapter_col_ref].astype(str)
        section_df[chapter_col_sec] = section_df[chapter_col_sec].astype(str)
    
    if part_col_ref and part_col_sec:
        reference_df[part_col_ref] = reference_df[part_col_ref].fillna('').astype(str)
        section_df[part_col_sec] = section_df[part_col_sec].fillna('').astype(str)

    agency_sections = []
    
    progress_bar = st.progress(0)
    total_refs = len(reference_df)
    matches_found = 0
    
    for idx, (_, ref_row) in enumerate(reference_df.iterrows()):
        agency_id = ref_row['agency_id']
        title = str(ref_row[title_col_ref])
        
        mask = (section_df[title_col_sec] == title)
        
        if chapter_col_ref and chapter_col_sec and pd.notna(ref_row.get(chapter_col_ref)):
            chapter = str(ref_row[chapter_col_ref])
            mask &= (section_df[chapter_col_sec] == chapter)
        
        if part_col_ref and part_col_sec and pd.notna(ref_row.get(part_col_ref)) and ref_row.get(part_col_ref) != '':
            part = str(ref_row[part_col_ref])
            mask &= (section_df[part_col_sec] == part)
        
        matching_sections = section_df[mask].copy()
        
        if not matching_sections.empty:
            matching_sections['agency_id'] = agency_id
            agency_sections.append(matching_sections)
            matches_found += len(matching_sections)
        
        # Update progress every 100 iterations to avoid too frequent updates
        if idx % 100 == 0 or idx == total_refs - 1:
            progress_bar.progress((idx + 1) / total_refs)
    
    progress_bar.empty()
    
    st.success(f"Found {matches_found} section matches across {len(agency_sections)} reference entries")
    
    if agency_sections:
        combined_sections = pd.concat(agency_sections, ignore_index=True)
        st.write(f"Combined sections shape: {combined_sections.shape}")
    else:
        st.error("No matching sections found!")
        return None
    
    agency_data = combined_sections.merge(agency_df, on='agency_id', how='left')
    
    # Check for successful joins
    successful_joins = (~agency_data['name'].isna()).sum()
    st.write(f"Successfully joined {successful_joins} out of {len(agency_data)} section records with agency data")
    
    if successful_joins == 0:
        st.error("No successful joins with agency data! Check if agency_id columns match.")
        st.write("Sample agency_ids in reference_df:", reference_df['agency_id'].unique()[:10])
        st.write("Sample agency_ids in agency_df:", agency_df['agency_id'].unique()[:10])
        return None
    
    return agency_data

def calculate_enhanced_metrics(agency_data):
    """
    Calculate comprehensive metrics for the dashboard
    """
    
    # 1. Word count analysis
    if 'section_text' in agency_data.columns:
        text_col = 'section_text'
    elif 'body' in agency_data.columns:
        text_col = 'body'
    else:
        text_cols = [col for col in agency_data.columns if 'text' in col.lower() or 'body' in col.lower()]
        text_col = text_cols[0] if text_cols else None
    
    if text_col is None:
        st.error("No text column found in the data")
        return None
    
    agency_data['word_count'] = agency_data[text_col].fillna('').str.split().str.len()
    
    word_counts = agency_data.groupby(['agency_id', 'name']).agg({
        'word_count': ['sum', 'count', 'mean'],
        text_col: 'count'
    }).reset_index()
    
    word_counts.columns = ['agency_id', 'agency_name', 'total_words', 'section_count', 'avg_words_per_section', 'total_sections']
    
    # 2. Generate checksums
    agency_checksums = []
    for agency_id in agency_data['agency_id'].unique():
        agency_text = agency_data[agency_data['agency_id'] == agency_id][text_col].fillna('').str.cat(sep=' ')
        checksum = hashlib.md5(agency_text.encode()).hexdigest()
        agency_name = agency_data[agency_data['agency_id'] == agency_id]['name'].iloc[0]
        agency_checksums.append({
            'agency_id': agency_id,
            'agency_name': agency_name,
            'checksum': checksum,
            'content_length': len(agency_text)
        })
    
    checksums_df = pd.DataFrame(agency_checksums)
    
    # 3. Historical simulation
    base_date = datetime.now() - timedelta(days=365)
    dates = [base_date + timedelta(days=x*30) for x in range(12)]
    
    historical_data = []
    for agency_id in word_counts['agency_id'].unique():
        if agency_id in word_counts['agency_id'].values:
            base_words = word_counts[word_counts['agency_id'] == agency_id]['total_words'].iloc[0]
            agency_name = word_counts[word_counts['agency_id'] == agency_id]['agency_name'].iloc[0]
            
            for i, date_val in enumerate(dates):
                change_factor = 1 + np.random.normal(0, 0.05)
                simulated_words = int(base_words * change_factor * (1 + i * 0.02))
                
                historical_data.append({
                    'date': date_val,
                    'agency_id': agency_id,
                    'agency_name': agency_name,
                    'word_count': simulated_words
                })
    
    historical_df = pd.DataFrame(historical_data)
    
    return {
        'word_counts': word_counts,
        'checksums': checksums_df,
        'historical_data': historical_df,
        'agency_data': agency_data
    }

def create_plotly_dashboard(metrics_data):
    """
    Create interactive Plotly visualizations
    """
    
    word_counts = metrics_data['word_counts']
    historical_df = metrics_data['historical_data']
    checksums_df = metrics_data['checksums']
    
    # 1. Word Count Bar Chart
    top_20 = word_counts.nlargest(20, 'total_words')
    fig_words = px.bar(
        top_20, 
        x='total_words', 
        y='agency_name',
        orientation='h',
        title='Top 20 Agencies by Total Word Count',
        labels={'total_words': 'Total Words', 'agency_name': 'Agency'},
        text='total_words'
    )
    fig_words.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    fig_words.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
    
    # 2. Historical Trends
    top_5_agencies = word_counts.nlargest(5, 'total_words')['agency_id'].tolist()
    historical_subset = historical_df[historical_df['agency_id'].isin(top_5_agencies)]
    
    fig_trends = px.line(
        historical_subset,
        x='date',
        y='word_count',
        color='agency_name',
        title='Historical Word Count Trends (Top 5 Agencies - Simulated)',
        labels={'word_count': 'Word Count', 'date': 'Date'}
    )
    fig_trends.update_layout(height=500)
    
    # 3. Distribution of Word Counts
    fig_dist = px.histogram(
        word_counts,
        x='total_words',
        nbins=30,
        title='Distribution of Total Word Counts Across Agencies',
        labels={'total_words': 'Total Words', 'count': 'Number of Agencies'}
    )
    fig_dist.update_layout(height=400)
    
    # 4. Sections vs Words Scatter
    fig_scatter = px.scatter(
        word_counts,
        x='section_count',
        y='total_words',
        hover_data=['agency_name'],
        title='Sections vs Total Words Correlation',
        labels={'section_count': 'Number of Sections', 'total_words': 'Total Words'}
    )
    
    correlation = word_counts['section_count'].corr(word_counts['total_words'])
    fig_scatter.add_annotation(
        x=0.05, y=0.95,
        xref='paper', yref='paper',
        text=f'Correlation: {correlation:.3f}',
        showarrow=False,
        bgcolor="rgba(255, 255, 0, 0.7)",
        bordercolor="black",
        borderwidth=1,
        borderpad=4
    )
    fig_scatter.update_layout(height=500)
    
    # 5. Content Length Distribution
    fig_content = px.histogram(
        checksums_df,
        x='content_length',
        nbins=20,
        title='Distribution of Agency Content Lengths',
        labels={'content_length': 'Content Length (characters)', 'count': 'Number of Agencies'}
    )
    fig_content.update_layout(height=400)
    
    return {
        'word_chart': fig_words,
        'trends_chart': fig_trends,
        'distribution_chart': fig_dist,
        'scatter_chart': fig_scatter,
        'content_chart': fig_content
    }

@st.cache_data(ttl=3600)
def get_titles_data():
    """Fetches titles data from the eCFR API."""
    api_url = "https://www.ecfr.gov/api/versioner/v1/titles.json"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching titles data from API: {e}")
        return None

@st.cache_resource(show_spinner=True)
def run_initial_etl():
    st.write("Starting ETL Pipeline...")
    titles_data = get_titles_data()

    if not titles_data or "titles" not in titles_data:
        st.error("Could not retrieve valid titles data from the API.")
        return "ETL Pipeline failed due to API error."

    title_amendment_dates = {str(title['number']): title.get('latest_amended_on') for title in titles_data['titles']}

    progress_bar = st.progress(0)
    status_text = st.empty()

    processed_count = 0
    total_titles = 50

    for title_number_int in range(1, 51):
        if title_number_int == 35:
            continue 

        title_number_str = str(title_number_int)
        latest_amended_on = title_amendment_dates.get(title_number_str)

        if latest_amended_on:
            user_params = {'date': latest_amended_on, 'title': title_number_str}
        else:
            today_str = date.today().strftime('%Y-%m-%d')
            user_params = {'date': today_str, 'title': title_number_str}

        run = ETLPipeline(user_params)
        run.run_pipeline()

        processed_count += 1
        progress_bar.progress(processed_count / total_titles)
        status_text.text(f"Processing Title {title_number_str} ({processed_count}/{total_titles})")

    progress_bar.empty()
    status_text.empty()
    st.success("All titles processed by ETL Pipeline.")
    return "ETL Pipeline completed."

def create_proper_agency_dashboard_data(agency_df, section_df, reference_df):
    """
    Properly creates dashboard data by implementing correct joining logic
    """
    return create_comprehensive_agency_analysis(agency_df, section_df, reference_df)


def calculate_word_count_per_agency(df):
    df['word_count'] = df['section_text'].apply(lambda x: len(x.split()) if isinstance(x, str) else 0)
    word_counts = df.groupby('agency_name')['word_count'].sum().reset_index()
    return word_counts

def calculate_checksum_per_agency(df):
    agency_texts = df.groupby('agency_name')['section_text'].apply(lambda x: " ".join(x)).reset_index()
    agency_texts['checksum'] = agency_texts['section_text'].apply(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
    return agency_texts[['agency_name', 'checksum']]

# --- Main Streamlit UI ---

st.set_page_config(layout="wide", page_title="CFR Regulatory Dashboard")
st.title("🏛️ CFR Regulatory Analytics Dashboard")

st.sidebar.title("Dashboard Navigation")
dashboard_mode = st.sidebar.selectbox(
    "Select Dashboard Mode",
    ["Overview", "Advanced Analytics", "Raw Data Debug"]
)

st.header("📊 Database Status")
db_exists = check_database_exists()
has_data = check_database_has_data() if db_exists else False

if db_exists and has_data:
    st.success("✅ Database exists and contains data")
elif db_exists and not has_data:
    st.warning("⚠️ Database exists but appears to be empty")
else:
    st.error("❌ Database file not found")

# ETL Trigger Section
st.header("🔄 ETL Pipeline Control")

# Show different UI based on database status
if not db_exists or not has_data:
    st.warning("🚨 **Database is not ready!** You need to run the ETL pipeline first to populate the database with CFR data.")
    
    if st.button("🚀 Initialize Database (Run ETL Pipeline)", type="primary"):
        with st.spinner("Running ETL pipeline and initializing database..."):
            status = run_initial_etl()
            st.success(status)
        st.rerun()
    
    st.info("💡 The ETL pipeline will process all CFR titles (1-50, excluding 35) and populate the database. This may take several minutes.")
    st.stop()  # Stop execution here until database is ready

else:
    # Database is ready, show refresh option
    if st.button("🔄 Refresh Data (Re-run ETL Pipeline)"):
        with st.spinner("Running ETL pipeline and refreshing data..."):
            status = run_initial_etl()
            st.success(status)
        st.rerun()

st.markdown("---")

st.info("Loading data from database...")

try:
    agency_df, section_df, reference_df = safe_load_data()
    
    if agency_df is None or section_df is None or reference_df is None:
        st.error("❌ Failed to load data from database. The database may be corrupted or empty.")
        st.info("💡 Try running the ETL pipeline again to refresh the data.")
        st.stop()
    
    st.success(f"✅ Data loaded successfully!")
    st.info(f"📊 Loaded {len(agency_df)} agencies, {len(section_df)} sections, and {len(reference_df)} references")
    
except Exception as e:
    st.error(f"❌ Error loading data: {e}")
    st.info("💡 Try running the ETL pipeline again to refresh the data.")
    st.stop()

with st.spinner("Processing dashboard data..."):
    df_dashboard = create_proper_agency_dashboard_data(agency_df, section_df, reference_df)

if dashboard_mode == "Raw Data Debug":
    st.header("Raw Data Debug")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Agency DataFrame")
        st.write(f"Shape: {agency_df.shape}")
        st.write(agency_df.head(10))
    
    with col2:
        st.subheader("Section DataFrame") 
        st.write(f"Shape: {section_df.shape}")
        st.write(section_df.head(10))
    
    with col3:
        st.subheader("Reference DataFrame")
        st.write(f"Shape: {reference_df.shape}")
        st.write(reference_df.head(10))
    
    if df_dashboard is not None and not df_dashboard.empty:
        st.subheader("Properly Joined Dashboard Data")
        st.write(f"Shape: {df_dashboard.shape}")
        st.write(f"Columns: {df_dashboard.columns.tolist()}")
        st.write("Sample data:")
        st.write(df_dashboard.head())

        agencies_with_data = df_dashboard[df_dashboard['name'].notna()]
        st.write(f"Agencies with successful joins: {agencies_with_data['name'].nunique()}")
        st.write("Sample agencies:", agencies_with_data['name'].unique()[:10])
    else:
        st.error("Failed to create properly joined dashboard data")

elif dashboard_mode == "Overview":

    st.header("📊 Basic Overview")
    
    if df_dashboard is not None and not df_dashboard.empty:
        text_col = None
        for col in df_dashboard.columns:
            if any(keyword in col.lower() for keyword in ['text', 'body', 'content']):
                text_col = col
                break
        
        if text_col:
            df_dashboard['word_count'] = df_dashboard[text_col].fillna('').str.split().str.len()
            word_counts_df = df_dashboard.groupby('name')['word_count'].sum().reset_index()
            word_counts_df.columns = ['agency_name', 'word_count']
            word_counts_df = word_counts_df.sort_values('word_count', ascending=False)
            
            # Calculate checksums
            agency_texts = df_dashboard.groupby('name')[text_col].apply(lambda x: " ".join(x.fillna(''))).reset_index()
            agency_texts['checksum'] = agency_texts[text_col].apply(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
            checksum_df = agency_texts[['name', 'checksum']]
            checksum_df.columns = ['agency_name', 'checksum']
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Word Count per Agency")
                st.dataframe(word_counts_df.head(20), use_container_width=True)
                
                chart_wc = alt.Chart(word_counts_df.head(20)).mark_bar().encode(
                    x=alt.X('agency_name:N', sort='-y', title='Agency'),
                    y=alt.Y('word_count:Q', title='Total Word Count'),
                    tooltip=['agency_name', 'word_count']
                ).properties(
                    title='Total Regulatory Text Word Count by Agency (Top 20)',
                    width=400,
                    height=300
                ).interactive()
                st.altair_chart(chart_wc, use_container_width=True)
            
            with col2:
                st.subheader("Content Checksums")
                st.info("MD5 checksums can indicate if agency content has changed.")
                st.dataframe(checksum_df.head(20), use_container_width=True)
        else:
            st.error("Could not find text column for analysis")
    else:
        st.error("No properly joined data available for overview")

elif dashboard_mode == "Advanced Analytics":

    st.header("🚀 Advanced Regulatory Analytics")
    
    if df_dashboard is not None and not df_dashboard.empty:
        with st.spinner("Creating comprehensive analysis..."):

            metrics_data = calculate_enhanced_metrics(df_dashboard)
            
            if metrics_data:

                st.subheader("📈 Summary Statistics")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Agencies", len(metrics_data['word_counts']))
                
                with col2:
                    st.metric("Total Sections", len(df_dashboard))
                
                with col3:
                    total_words = metrics_data['word_counts']['total_words'].sum()
                    st.metric("Total Words", f"{total_words:,}")
                
                with col4:
                    avg_words = metrics_data['word_counts']['total_words'].mean()
                    st.metric("Avg Words/Agency", f"{avg_words:,.0f}")
                
                charts = create_plotly_dashboard(metrics_data)
                
                tab1, tab2, tab3, tab4, tab5 = st.tabs([
                    "📊 Word Counts", 
                    "📈 Trends", 
                    "📉 Distributions", 
                    "🔗 Correlations", 
                    "📋 Data Tables"
                ])
                
                with tab1:
                    st.plotly_chart(charts['word_chart'], use_container_width=True)
                
                with tab2:
                    st.plotly_chart(charts['trends_chart'], use_container_width=True)
                    st.info("📝 Note: Historical trends are simulated for demonstration. Real implementation would use actual amendment dates.")
                
                with tab3:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.plotly_chart(charts['distribution_chart'], use_container_width=True)
                    with col2:
                        st.plotly_chart(charts['content_chart'], use_container_width=True)
                
                with tab4:
                    st.plotly_chart(charts['scatter_chart'], use_container_width=True)
                
                with tab5:
                    st.subheader("📊 Detailed Metrics")
                    
                    # Top agencies table
                    st.write("**Top 20 Agencies by Word Count:**")
                    top_agencies = metrics_data['word_counts'].nlargest(20, 'total_words')
                    st.dataframe(top_agencies, use_container_width=True)
                    
                    # Checksums table
                    st.write("**Agency Content Checksums:**")
                    st.dataframe(metrics_data['checksums'], use_container_width=True)
                    
                    # Export functionality
                    if st.button("📥 Download Analysis Data"):
                        csv = metrics_data['word_counts'].to_csv(index=False)
                        st.download_button(
                            label="Download Word Counts CSV",
                            data=csv,
                            file_name=f"cfr_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv"
                        )
            else:
                st.error("Failed to calculate metrics from the data.")
    else:
        st.error("No properly joined data available for advanced analytics")
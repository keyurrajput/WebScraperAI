# app.py

import os
import time
import json
import base64
import threading
from typing import Dict, Any

import streamlit as st
import pandas as pd
from PIL import Image

# Import custom modules
from core.agent import ScrapingAgent
from config.config import OUTPUT_DIR

# Set page configuration
st.set_page_config(
    page_title="AI Web Scraping Agent",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Create a lock for thread safety
lock = threading.Lock()

def update_status(status: str, progress: int):
    """Update the session state with the current status and progress"""
    with lock:
        st.session_state.status = status
        st.session_state.progress = progress

def get_download_link(file_path: str, label: str) -> str:
    """
    Generate a download link for a file
    
    Args:
        file_path: Path to the file
        label: Label for the download link
        
    Returns:
        HTML string with the download link
    """
    with open(file_path, "rb") as f:
        data = f.read()
    
    b64 = base64.b64encode(data).decode()
    filename = os.path.basename(file_path)
    mime_type = "application/zip" if file_path.endswith(".zip") else "application/octet-stream"
    
    href = f'<a href="data:{mime_type};base64,{b64}" download="{filename}">{label}</a>'
    return href

def display_dataset_preview(file_path: str):
    """
    Display a preview of the dataset
    
    Args:
        file_path: Path to the dataset file
    """
    file_ext = os.path.splitext(file_path)[1].lower()
    
    try:
        if file_ext == '.csv':
            df = pd.read_csv(file_path)
        elif file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        elif file_ext == '.json':
            df = pd.read_json(file_path)
        else:
            st.warning(f"Cannot preview file with extension {file_ext}")
            return
        
        st.subheader("Dataset Preview")
        st.dataframe(df.head(10))
        
        st.subheader("Dataset Statistics")
        st.write(f"Number of records: {len(df)}")
        st.write(f"Number of columns: {len(df.columns)}")
        
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            st.write("Numeric columns statistics:")
            st.dataframe(df[numeric_cols].describe())
    except Exception as e:
        st.error(f"Error previewing dataset: {e}")

def display_media_preview(media_files: list, limit: int = 5):
    """
    Display a preview of media files
    
    Args:
        media_files: List of media file paths
        limit: Maximum number of files to preview
    """
    if not media_files:
        return
    
    preview_files = media_files[:limit]
    
    st.subheader(f"Media Preview ({len(media_files)} files total)")
    
    columns = st.columns(min(len(preview_files), 3))
    
    for i, file_path in enumerate(preview_files):
        col_idx = i % 3
        
        file_ext = os.path.splitext(file_path)[1].lower()
        file_name = os.path.basename(file_path)
        
        with columns[col_idx]:
            if file_ext in ['.jpg', '.jpeg', '.png', '.gif']:
                try:
                    img = Image.open(file_path)
                    st.image(img, caption=file_name, use_column_width=True)
                except:
                    st.warning(f"Cannot preview {file_name}")
            else:
                st.text(file_name)
                st.markdown(get_download_link(file_path, "Download"), unsafe_allow_html=True)

def run_scraping_job(agent: ScrapingAgent, task_info: Dict[str, Any]):
    """
    Run a scraping job in a separate thread
    
    Args:
        agent: ScrapingAgent instance
        task_info: Task information
    """
    try:
        results = agent.execute_task(task_info, callback=update_status)
        
        with lock:
            st.session_state.results = results
            st.session_state.job_completed = True
    except Exception as e:
        with lock:
            st.session_state.error = str(e)
            st.session_state.job_completed = True

def main():
    """Main application function"""
    # Custom CSS
    st.markdown("""
        <style>
        .main-header {
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 1rem;
        }
        .sub-header {
            font-size: 1.5rem;
            font-weight: 600;
            margin-bottom: 1rem;
            color: #4b6584;
        }
        .info-box {
            background-color: #f1f2f6;
            padding: 1.5rem;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
        }
        .success-box {
            background-color: #c8e6c9;
            padding: 1.5rem;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
        }
        .sidebar-info {
            padding: 1rem;
            background-color: #f1f2f6;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown('<div class="main-header">🔍 AI Web Scraping Agent</div>', unsafe_allow_html=True)
    st.markdown("""
        This AI-powered tool can create custom datasets by scraping the web based on your natural language requests.
        Just describe what data you want, and the agent will handle the rest!
    """)
    
    # Initialize session state
    if 'agent' not in st.session_state:
        st.session_state.agent = ScrapingAgent(OUTPUT_DIR)
    
    if 'job_running' not in st.session_state:
        st.session_state.job_running = False
    
    if 'job_completed' not in st.session_state:
        st.session_state.job_completed = False
    
    if 'task_info' not in st.session_state:
        st.session_state.task_info = None
    
    if 'results' not in st.session_state:
        st.session_state.results = None
    
    if 'status' not in st.session_state:
        st.session_state.status = None
    
    if 'progress' not in st.session_state:
        st.session_state.progress = 0
    
    if 'error' not in st.session_state:
        st.session_state.error = None
    
    # Sidebar
    with st.sidebar:
        st.markdown('<div class="sub-header">About</div>', unsafe_allow_html=True)
        st.markdown(
            """
            <div class="sidebar-info">
            This tool can scrape various types of data:
            
            - Text data (articles, details, etc.)
            - Images
            - Videos
            - Audio
            - Mixed content
            
            Supported output formats:
            - CSV
            - Excel
            - JSON
            </div>
            """,
            unsafe_allow_html=True
        )
        
        st.markdown('<div class="sub-header">Examples</div>', unsafe_allow_html=True)
        st.markdown(
            """
            Example requests:
            - "Get all F1 race results from the 2023 season, including driver, team, position, and points"
            - "Create a dataset of Pokemon with their types, abilities, and base stats"
            - "Collect images of national parks in the United States with their names and locations"
            - "Get price data for popular smartphone models with their specs and release dates"
            """
        )
    
    # Main content
    if not st.session_state.job_running and not st.session_state.job_completed:
        st.markdown('<div class="sub-header">What data would you like to collect?</div>', unsafe_allow_html=True)
        
        request = st.text_area(
            "Describe the dataset you want to create:",
            height=150,
            help="Be as specific as possible about what data you want to collect and any filters or criteria to apply."
        )
        
        output_format = st.selectbox(
            "Select output format:",
            ["CSV", "Excel", "JSON"],
            help="Choose the format for your exported dataset."
        )
        
        if st.button("Create Dataset", type="primary"):
            if request:
                with st.spinner("Analyzing request..."):
                    # Update request with output format
                    full_request = f"{request}\nOutput format: {output_format}"
                    
                    # Process request
                    task_info = st.session_state.agent.process_request(full_request)
                    st.session_state.task_info = task_info
                    
                    # Show task information
                    st.markdown('<div class="info-box">', unsafe_allow_html=True)
                    st.markdown(f"**Task ID:** {task_info['task_id']}")
                    st.markdown(f"**Topic:** {task_info['task']['topic']}")
                    st.markdown(f"**Data Type:** {task_info['task']['data_type']}")
                    st.markdown(f"**Complexity:** {task_info['complexity']}")
                    st.markdown(f"**Estimated Time:** {task_info['estimated_time']} seconds")
                    st.markdown('</div>', unsafe_allow_html=True)
                    
                    # Start the job
                    st.session_state.job_running = True
                    st.session_state.error = None
                    
                    # Run in a separate thread
                    threading.Thread(
                        target=run_scraping_job,
                        args=(st.session_state.agent, task_info)
                    ).start()
                    
                    # Force a rerun to show the progress
                    st.rerun()
            else:
                st.error("Please enter a request.")
    
    # Show progress if job is running
    if st.session_state.job_running and not st.session_state.job_completed:
        st.markdown('<div class="sub-header">Creating your dataset...</div>', unsafe_allow_html=True)
        
        # Show task info
        if st.session_state.task_info:
            task_info = st.session_state.task_info
            
            st.markdown('<div class="info-box">', unsafe_allow_html=True)
            st.markdown(f"**Task ID:** {task_info['task_id']}")
            st.markdown(f"**Topic:** {task_info['task']['topic']}")
            st.markdown(f"**Data Type:** {task_info['task']['data_type']}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Show progress bar
        progress = st.session_state.progress
        status = st.session_state.status or "Initializing..."
        
        st.progress(progress / 100)
        st.markdown(f"**Status:** {status}")
        
        # Check if job completed
        if st.session_state.job_completed:
            st.rerun()
        else:
            # Auto-refresh
            time.sleep(1)
            st.rerun()
    
    # Show results if job completed
    if st.session_state.job_completed:
        if st.session_state.error:
            st.error(f"Error: {st.session_state.error}")
            if st.button("Try Again"):
                st.session_state.job_running = False
                st.session_state.job_completed = False
                st.session_state.task_info = None
                st.session_state.results = None
                st.session_state.status = None
                st.session_state.progress = 0
                st.session_state.error = None
                st.rerun()
        elif st.session_state.results:
            results = st.session_state.results
            
            if results['status'] == 'completed':
                st.markdown('<div class="sub-header">Dataset Created Successfully!</div>', unsafe_allow_html=True)
                
                st.markdown('<div class="success-box">', unsafe_allow_html=True)
                st.markdown(f"**Dataset Path:** {results['dataset_path']}")
                st.markdown(f"**Records:** {results['record_count']}")
                st.markdown(f"**Columns:** {', '.join(results['columns'])}")
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Display download link
                st.markdown(
                    get_download_link(results['dataset_path'], "📥 Download Dataset"),
                    unsafe_allow_html=True
                )
                
                # Preview tabs
                tab1, tab2 = st.tabs(["Data Preview", "Media Preview"])
                
                with tab1:
                    display_dataset_preview(results['data_file'])
                
                with tab2:
                    if results['media_files']:
                        display_media_preview(results['media_files'])
                    else:
                        st.info("No media files in this dataset.")
                
                # Start a new job
                if st.button("Create Another Dataset"):
                    st.session_state.job_running = False
                    st.session_state.job_completed = False
                    st.session_state.task_info = None
                    st.session_state.results = None
                    st.session_state.status = None
                    st.session_state.progress = 0
                    st.session_state.error = None
                    st.rerun()
            else:
                st.error(f"Job failed: {results.get('error', 'Unknown error')}")
                
                if st.button("Try Again"):
                    st.session_state.job_running = False
                    st.session_state.job_completed = False
                    st.session_state.task_info = None
                    st.session_state.results = None
                    st.session_state.status = None
                    st.session_state.progress = 0
                    st.session_state.error = None
                    st.rerun()
        else:
            st.error("No results found. Please try again.")
            
            if st.button("Try Again"):
                st.session_state.job_running = False
                st.session_state.job_completed = False
                st.session_state.task_info = None
                st.session_state.results = None
                st.session_state.status = None
                st.session_state.progress = 0
                st.session_state.error = None
                st.rerun()

if __name__ == "__main__":
    main()

# WebScraperAI

**AI-powered web scraping with natural language processing capabilities**

## Overview
WebScraperAI is an intelligent web scraping platform that transforms natural language requests into custom datasets. By combining the power of Large Language Models (LLMs) with advanced web scraping techniques, this application enables users to collect, process, and export web data without writing a single line of code. Simply describe what data you need, and the AI agent handles the complexities of crawling websites, extracting information, and organizing it into structured datasets.

## Key Features

### Intuitive Natural Language Interface
- **Plain English Requests**: Describe your data needs in everyday language
- **Automatic Task Analysis**: AI processes your request and identifies required data points
- **Smart Source Selection**: System autonomously determines the best websites to scrape
- **Complexity Estimation**: Provides time estimates based on request complexity

### Versatile Data Collection
- **Multiple Data Types**: Extract text, images, videos, and audio content
- **Intelligent Crawling**: Navigates websites efficiently to find relevant information
- **Multi-Source Integration**: Combines data from multiple websites into cohesive datasets
- **Parallel Processing**: Utilizes multi-threading for faster data collection

### Comprehensive Data Processing
- **Automated Cleaning**: Handles missing values, standardizes formats, and removes duplicates
- **Smart Type Conversion**: Detects and converts appropriate data types
- **Media File Management**: Downloads and organizes media content with metadata
- **Structured Output**: Creates well-organized datasets ready for analysis

### Streamlined User Experience
- **Interactive Web Interface**: Elegant Streamlit UI with real-time progress tracking
- **Live Status Updates**: Provides detailed feedback during the scraping process
- **Dataset Preview**: Examine collected data directly in the browser
- **One-Click Download**: Export results in CSV, Excel, or JSON formats

## Technical Architecture
The application is built on a modular architecture with several key components:

1. **Core Agent System**: Orchestrates the entire data collection workflow
2. **LLM Processor**: Analyzes requests and generates scraping strategies using OpenAI models
3. **Scraper Orchestrator**: Manages different scraper types based on content requirements
4. **Data Processor**: Cleans and transforms raw scraped data
5. **Dataset Exporter**: Packages data into organized, downloadable files

The system supports multiple scraping methods:
- **RequestsScraper**: For basic HTML content
- **SeleniumScraper**: For JavaScript-rendered pages
- **PlaywrightScraper**: For complex single-page applications
- **MediaScraper**: Specialized for images, videos, and audio files

## Getting Started
1. Install dependencies using `pip install -r requirements.txt`
2. Create a `.env` file with your OpenAI API key: `OPENAI_API_KEY=your_key_here`
3. Launch the application with `streamlit run app.py`
4. Enter your data request in the text area
5. Select your preferred output format
6. Click "Create Dataset" and watch the AI do its work

## Use Cases
- **Market Research**: Gather product information, pricing data, and customer reviews
- **Content Creation**: Collect reference materials, images, and facts on specific topics
- **Competitive Analysis**: Monitor competitor offerings and pricing strategies
- **Academic Research**: Compile datasets for research projects and analysis
- **Business Intelligence**: Track industry trends and gather market intelligence

## Ethical Considerations
This tool is designed for legitimate data collection purposes only. Always ensure compliance with websites' terms of service, respect robots.txt directives, and maintain appropriate request rates to avoid overloading servers. The application incorporates built-in rate limiting and ethical scraping practices.

WebScraperAI demonstrates how artificial intelligence can simplify complex technical tasks, making powerful data collection capabilities accessible to users regardless of their programming expertise.

# core/llm.py

import json
from typing import Dict, List, Any, Optional
from openai import OpenAI
from langchain.prompts import PromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field
from config.config import OPENAI_API_KEY, LLM_MODEL, LLM_TEMPERATURE

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)

class ScrapingTask(BaseModel):
    """Schema for a web scraping task"""
    topic: str = Field(description="Main topic or subject of the data")
    data_type: str = Field(description="Type of data: text, images, video, audio, or mixed")
    sources: List[str] = Field(description="Potential websites to scrape data from")
    attributes: List[str] = Field(description="Specific data points to extract")
    filters: Dict[str, Any] = Field(description="Filters to apply to the data")
    output_format: str = Field(description="Preferred output format (csv, excel, json, etc.)")
    search_queries: List[str] = Field(description="Search queries to use for finding relevant pages")

class LLMProcessor:
    """Class for processing natural language requests using LLMs"""
    
    def __init__(self):
        """Initialize the LLM processor"""
        self.parser = PydanticOutputParser(pydantic_object=ScrapingTask)
    
    def analyze_request(self, user_request: str) -> ScrapingTask:
        """
        Analyze a user request and convert it to a structured scraping task
        
        Args:
            user_request: Natural language request from the user
            
        Returns:
            Structured scraping task object
        """
        prompt_template = """
        You are an AI assistant that helps users create web scraping tasks. 
        Given the following request, extract the relevant information to create a web scraping plan.
        
        User Request: {user_request}
        
        Create a detailed plan for scraping the requested data. Be specific and comprehensive.
        {format_instructions}
        """
        
        prompt = PromptTemplate(
            template=prompt_template,
            input_variables=["user_request"],
            partial_variables={"format_instructions": self.parser.get_format_instructions()}
        )
        
        formatted_prompt = prompt.format(user_request=user_request)
        
        # Use OpenAI directly instead of LangChain's LLM abstraction
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": "You are a helpful AI assistant that analyzes web scraping requests."},
                {"role": "user", "content": formatted_prompt}
            ],
            temperature=LLM_TEMPERATURE,
        )
        
        llm_response = response.choices[0].message.content
        
        try:
            return self.parser.parse(llm_response)
        except Exception as e:
            # Fallback to direct OpenAI completion if parsing fails
            return self._direct_openai_analysis(user_request)

    def _direct_openai_analysis(self, user_request: str) -> ScrapingTask:
        """
        Fallback method using direct OpenAI completion API for request analysis
        
        Args:
            user_request: Natural language request from the user
            
        Returns:
            Structured scraping task object
        """
        prompt = f"""
        Analyze this web scraping request and provide a structured JSON response with the following fields:
        - topic: Main topic or subject of the data
        - data_type: Type of data (text, images, video, audio, or mixed)
        - sources: List of potential websites to scrape data from
        - attributes: List of specific data points to extract
        - filters: Dictionary of filters to apply to the data
        - output_format: Preferred output format (csv, excel, json, etc.)
        - search_queries: List of search queries to use for finding relevant pages
        
        User Request: {user_request}
        
        JSON Response:
        """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful AI assistant that analyzes web scraping requests."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
        )
        
        try:
            content = response.choices[0].message.content
            # Extract JSON from the response
            json_str = content.strip()
            if "```json" in json_str:
                json_str = json_str.split("```json")[1].split("```")[0].strip()
            elif "```" in json_str:
                json_str = json_str.split("```")[1].split("```")[0].strip()
            
            data = json.loads(json_str)
            return ScrapingTask(**data)
        except Exception as e:
            # Create a default task with basic information if parsing fails
            return ScrapingTask(
                topic=user_request,
                data_type="text",
                sources=[],
                attributes=[],
                filters={},
                output_format="csv",
                search_queries=[user_request]
            )

    def generate_scraping_strategy(self, task: ScrapingTask) -> Dict[str, Any]:
        """
        Generate a detailed scraping strategy for a given task
        
        Args:
            task: Structured scraping task
            
        Returns:
            Dictionary with scraping strategy details
        """
        prompt = f"""
        Generate a detailed web scraping strategy for the following task:
        
        Topic: {task.topic}
        Data Type: {task.data_type}
        Sources: {', '.join(task.sources) if task.sources else 'No specific sources provided'}
        Attributes: {', '.join(task.attributes)}
        Filters: {task.filters}
        
        Provide a JSON response with the following structure:
        - priority_sources: List of specific URLs to scrape in order of priority
        - search_strategy: How to find additional relevant pages
        - selectors: CSS or XPath selectors for each attribute to extract
        - pagination_strategy: How to handle pagination on listing pages
        - handling_special_content: How to handle special content like images, videos, etc.
        """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a web scraping expert that creates detailed scraping strategies."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
        )
        
        try:
            content = response.choices[0].message.content
            # Extract JSON from the response
            json_str = content.strip()
            if "```json" in json_str:
                json_str = json_str.split("```json")[1].split("```")[0].strip()
            elif "```" in json_str:
                json_str = json_str.split("```")[1].split("```")[0].strip()
            
            return json.loads(json_str)
        except Exception as e:
            # Return a basic strategy if parsing fails
            return {
                "priority_sources": task.sources,
                "search_strategy": "Use Google search with the provided queries",
                "selectors": {},
                "pagination_strategy": "Look for 'Next' links or numbered pagination",
                "handling_special_content": "Download files directly when possible"
            }
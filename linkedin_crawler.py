import requests
from bs4 import BeautifulSoup
import json
import os
import time
import random
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from pathlib import Path


class LinkedInJobCrawler:
    def __init__(self, config_file=None):
        """Initialize the LinkedIn job crawler with configuration."""
        # Create base directory path
        base_dir = Path.cwd() / "data" 
        
        # Create directory if it doesn't exist
        os.makedirs(base_dir, exist_ok=True)
        
        # Set file paths
        if config_file is None:
            config_file = base_dir / "carwler.json"
        
        database_path = base_dir / "database.json"
        
        # Default configuration
        self.config = {
            'job_url': 'https://www.linkedin.com/jobs/search/?f_TPR=r3600&f_E=2%2C3&keywords=data%20engineer',
            'keywords': ['python', 'developer', 'engineer', 'data engineer', 'airflow', 'etl', 'aws', 'snowflake', 'databricks'],
            'excluded_keywords': ['5+ years', '4+ years', 'manager', 'director'],
            'database_file': str(database_path),
            'user_agents': [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
            ],
            'request_delay': {
                'min_seconds': 2,
                'max_seconds': 5
            }
        }
        
        # Convert config_file to string if it's a Path object
        config_file = str(config_file)
        
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                custom_config = json.load(f)
                self.config.update(custom_config)
        else:
            # Create parent directory if it doesn't exist
            os.makedirs(os.path.dirname(config_file), exist_ok=True)
            # Save default configuration
            with open(config_file, 'w') as f:
                json.dump(self.config, f, indent=4)
                
        # Initialize the webdriver
        self.driver = None
        
        # Load previous jobs
        self.previous_jobs = self.load_previous_jobs()

        
    def setup_driver(self):
        """Set up Selenium WebDriver for JavaScript rendering."""
        if self.driver is not None:
            try:
                self.driver.quit()
            except:
                pass
                
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Add random user agent
        user_agent = random.choice(self.config['user_agents'])
        chrome_options.add_argument(f"--user-agent={user_agent}")
        
        try:
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        except Exception as e:
            print(f"Error setting up Chrome driver: {e}")
            print("Trying with direct ChromeDriver...")
            self.driver = webdriver.Chrome(options=chrome_options)
            
    def load_previous_jobs(self):
        """Load previously scraped jobs from database file."""
        if not os.path.exists(self.config['database_file']):
            return []
        try:
            with open(self.config['database_file'], 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading previous jobs: {e}")
            return []
            
    def save_jobs(self, jobs):
        """Save jobs to the database file."""
        try:
            with open(self.config['database_file'], 'w') as f:
                json.dump(jobs, f, indent=4)
            print(f"Jobs saved to {self.config['database_file']}")
        except Exception as e:
            print(f"Error saving jobs: {e}")
            
    def is_new_job(self, job):
        """Check if a job is new by comparing with previous jobs."""
        for prev_job in self.previous_jobs:
            # Compare key fields to determine if it's the same job
            if (job['title'] == prev_job['title'] and 
                job['company'] == prev_job['company'] and
                job['location'] == prev_job['location']):
                return False
        return True
        
    def is_job_relevant(self, job_title):
        """Check if job title contains desired keywords and not excluded keywords."""
        title_lower = job_title.lower()
        
        # Check if any keyword is in the title (less strict for LinkedIn since we already filtered by keyword in URL)
        has_keyword = True
        
        # Check if any excluded keyword is in the title
        has_excluded = any(keyword.lower() in title_lower for keyword in self.config['excluded_keywords'])
        
        return has_keyword and not has_excluded
        
    def scrape_linkedin_jobs(self):
        """Scrape job data from LinkedIn."""
        jobs = []
        try:
            if self.driver is None:
                self.setup_driver()
                
            print(f"Fetching LinkedIn jobs from: {self.config['job_url']}")
            self.driver.get(self.config['job_url'])
            
            # Wait for page to load
            time.sleep(5)
            
            # Scroll down to load more results (LinkedIn uses infinite scroll)
            print("Scrolling to load more job listings...")
            scroll_count = 0
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            while scroll_count < 5:  # Limit scrolling to 5 times
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
                scroll_count += 1
                
            # Get the page source after JavaScript execution
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # LinkedIn job cards
            job_cards = soup.find_all('div', class_='base-card')
            print(f"Found {len(job_cards)} job cards on the page")
            
            for card in job_cards:
                try:
                    # Extract job title
                    title_element = card.find('h3', class_='base-search-card__title')
                    if not title_element:
                        continue
                        
                    title = title_element.text.strip()
                    
                    if not self.is_job_relevant(title):
                        continue
                        
                    # Extract company name
                    company_element = card.find('h4', class_='base-search-card__subtitle')
                    company = company_element.text.strip() if company_element else "Unknown Company"
                    
                    # Extract location
                    location_element = card.find('span', class_='job-search-card__location')
                    location = location_element.text.strip() if location_element else "Unknown Location"
                    
                    # Extract job URL
                    link_element = card.find('a', class_='base-card__full-link', href=True)
                    job_url = link_element['href'] if link_element and 'href' in link_element.attrs else ""
                    
                    # Extract posting date (LinkedIn typically shows "1d ago", "2h ago", etc.)
                    date_element = card.find('time', class_='job-search-card__listdate')
                    date_posted = date_element.text.strip() if date_element else "Recent"
                    
                    # Only add if we have a valid URL
                    if job_url:
                        jobs.append({
                            'title': title,
                            'company': company,
                            'location': location,
                            'date_posted': date_posted,
                            'url': job_url,
                            'source': 'LinkedIn',
                            'scraped_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        print(f"Found job: {title} at {company}")
                except Exception as e:
                    print(f"Error extracting job data: {e}")
            
        except Exception as e:
            print(f"Error scraping LinkedIn: {e}")
            # Try to restart the driver if it failed
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
            
        return jobs
        
    def run_once(self):
        """Run the LinkedIn job crawler once."""
        print(f"Starting LinkedIn job scraping at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Looking for jobs posted in the last 24 hours matching 'python developer'")
        
        # Scrape LinkedIn jobs
        current_jobs = self.scrape_linkedin_jobs()
        
        # Identify new jobs
        new_jobs = []
        for job in current_jobs:
            if self.is_new_job(job):
                job['email_sent'] = False
                new_jobs.append(job)
        

        one_hour_ago = datetime.now() - timedelta(hours=1)
        filtered_previous_jobs = [
            job for job in self.previous_jobs 
            if datetime.strptime(job['scraped_date'], '%Y-%m-%d %H:%M:%S') >= one_hour_ago
]
        all_jobs = filtered_previous_jobs + new_jobs
        self.save_jobs(all_jobs)
        
        print(f"\nFound {len(current_jobs)} total job listings")
        print(f"Identified {len(new_jobs)} new job postings")
        
        return new_jobs
        
    def cleanup(self):
        """Clean up resources."""
        if self.driver:
            try:
                self.driver.quit()
                print("Browser closed successfully")
            except:
                pass
            self.driver = None



# Run the LinkedIn crawler once
if __name__ == "__main__":
    try:
        print("=== LinkedIn Job Crawler - Python Developer Jobs (Last 24 Hours) ===")
        crawler = LinkedInJobCrawler()
        new_jobs = crawler.run_once()
        
        # Print a summary of results
        if new_jobs:
            print("\n===== NEW JOBS FOUND =====")
            for i, job in enumerate(new_jobs, 1):
                print(f"{i}. {job['title']} at {job['company']}")
                print(f"   Location: {job['location']}")
                print(f"   Posted: {job['date_posted']}")
                print(f"   URL: {job['url']}")
                print()
        else:
            print("\nNo new Python developer jobs found on LinkedIn in the last 24 hours.")
            
    except KeyboardInterrupt:
        print("\nJob crawler stopped by user")
    except Exception as e:
        print(f"\nError in job crawler: {e}")
    finally:
        # Clean up resources
        if 'crawler' in locals():
            crawler.cleanup()



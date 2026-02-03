import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import os
import schedule
import time

class EventDiscoveryTool:
    def __init__(self, city_slug, storage_file="events_data.xlsx"):
        self.city_slug = city_slug
        self.storage_file = storage_file

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def fetch_events(self):
        """
        Scrapes event data. 
        Note: In a production environment, this would need Selenium 
        to handle dynamic JS rendering on sites like BookMyShow.
        """
        print(f"Fetching events for {self.city_slug}...")
        
        
        url = f"https://in.bookmyshow.com/explore/events-{self.city_slug}"
        
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                print(f"Error fetching page: {response.status_code}")
                return []

            soup = BeautifulSoup(response.content, 'html.parser')
            events = []

            
            card_list = soup.find_all('div', class_='commonStyles__ItemWrapper-sc-133848s-1') 

            for card in card_list:
                try:
                    
                    name = card.find('div', class_='commonStyles__VerticalTileHeader-sc-133848s-0').text.strip()
                    link = "https://in.bookmyshow.com" + card.find('a')['href']
                    category = card.find('div', class_='commonStyles__VerticalTileDescription-sc-133848s-2').text.strip()
                    
                    
                    events.append({
                        'Event Name': name,
                        'Date': datetime.date.today().strftime("%Y-%m-%d"), 
                        'Venue': "TBD", 
                        'City': self.city_slug,
                        'Category': category,
                        'URL': link,
                        'Status': 'Active',
                        'Last Updated': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                except AttributeError:
                    continue 
            
            print(f"Found {len(events)} events.")
            return events

        except Exception as e:
            print(f"Scraping Error: {e}")
            return []

    def update_database(self, new_data):
        """
        [span_4](start_span)Handles Data Storage, Deduplication, and Updates[span_4](end_span)
        """
        if not new_data:
            return

        df_new = pd.DataFrame(new_data)

        if os.path.exists(self.storage_file):
            print("Loading existing database...")
            df_existing = pd.read_excel(self.storage_file)
            
            
            existing_urls = df_existing['URL'].tolist()
            
            
            df_unique_new = df_new[~df_new['URL'].isin(existing_urls)]
            
            
            df_existing.loc[df_existing['URL'].isin(df_new['URL']), 'Last Updated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            
            df_final = pd.concat([df_existing, df_unique_new], ignore_index=True)
            print(f"Added {len(df_unique_new)} new events. Updated existing timestamps.")
        else:
            print("Creating new database...")
            df_final = df_new

        self.process_expiry(df_final)

    def process_expiry(self, df):
        """
        [span_5](start_span)Expiry Handling: Mark past events[span_5](end_span)
        """
        today = datetime.date.today().strftime("%Y-%m-%d")
        
        df.loc[df['Date'] < today, 'Status'] = 'Expired'
        
        
        df.to_excel(self.storage_file, index=False)
        print(f"Database saved to {self.storage_file}")

    def run_job(self):
        print("--- Job Started ---")
        events = self.fetch_events()
        self.update_database(events)
        print("--- Job Finished ---")


if __name__ == "__main__":  
    
    tool = EventDiscoveryTool(city_slug="mumbai")
    
    
    tool.run_job()

    
    schedule.every().day.at("10:00").do(tool.run_job)

    print("Scheduler running... (Press Ctrl+C to stop)")
    while True:
        schedule.run_pending()
        time.sleep(1)
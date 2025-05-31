''' Import Modules '''
import requests
from datetime import datetime

# Replace with your raw GitHub CSV file URL
csv_url = 'https://raw.githubusercontent.com/datasets/nasdaq-listings/refs/heads/main/data/nasdaq-listed.csv'

# Optional: Add timestamp to filename to avoid overwrite
filename = f"nasdaq_listed_symbols_{datetime.now().strftime('%Y%m%d')}.csv"

response = requests.get(csv_url)
if response.status_code == 200:
    with open(filename, 'wb') as f:
        f.write(response.content)
    print(f"CSV downloaded successfully: {filename}")
else:
    print(f"Failed to download. Status code: {response.status_code}")

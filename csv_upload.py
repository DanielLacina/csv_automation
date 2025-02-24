import asyncio
from dotenv import load_dotenv
import json
import os
import httpx

load_dotenv()

cookies = json.loads(os.getenv("APP_INSTITUTE_COOKIES"))
url = "https://cms.appinstitute.com/cms/ajax/get_crm_users.php"
response = httpx.get(
    url,
    cookies=cookies
)

# Print the response
print(response.status_code)
print(response.text)




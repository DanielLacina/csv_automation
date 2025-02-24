import asyncio
import os
from dotenv import load_dotenv
import json
import os
import httpx
import asyncio

load_dotenv()

async def main():
    cookies = json.loads(os.getenv("APP_INSTITUTE_COOKIES"))
    filename = "users.csv"
    with open(filename, "rb") as f:
        async with httpx.AsyncClient(cookies=cookies) as client:
            headers = {"Content-Type": "application/octet-stream"}
            files = {"csv": f}
            params = {"appcode": "undefined", "userfile": filename}
            upload_csv_url = "https://cms.appinstitute.com/cms/ajax/saveCSVFile.php"
            r = await client.post(upload_csv_url, files=files, params=params, headers=headers)
            csv_url = r.json()["csv_url"] 
            import_csv_url = "https://cms.appinstitute.com/cms/ajax/usersCsvImporter.php"
            import_data = {"csvFile": csv_url, "action": "import"}
            r = await client.post(import_csv_url, data=import_data)

asyncio.run(main())

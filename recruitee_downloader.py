import aiohttp
import requests
import asyncio
import json
import os
from datetime import datetime
import sys

args = sys.argv[1:]

proxy = os.environ.get('HTTP_PROXY')

if len(args) == 0:
    print("Usage: python3 recruitee_downloader.py [recruiter-link]")
    exit(0)

recruiter_link = args[0]
container_id = recruiter_link.split("/")[-1]

save_path = f'./resumes_batch_{container_id}_{datetime.now().isoformat().split("T")[0]}'
if not os.path.exists(save_path):
    os.makedirs(save_path)

# Candidates
response = requests.get(f"https://api.recruitee.com/share/containers/{container_id}")
name_resume_urls = []

if (response.ok):
    # Fetch all candidate IDs
    can_list = json.loads(response.content)
    name_id_pairs = [(can['name'], can['id']) for can in can_list['container']['candidates']]
else:
    exit(-1)

async def fetch_candidate_profile(name_id, session):
    try:
        async with session.get(url=f"https://api.recruitee.com/share/containers/{container_id}/candidates/{name_id[1]}", proxy=proxy) as response:
            can_data = await response.json()

            if (response.ok and can_data['candidate'] != None):
                return (name_id[0], can_data['candidate']['cv_url'])
            else:
                return (name_id[0], None)
    except Exception as e:
        print(f"Failed to fetch candidate profile for {name_id[0]} : {e}.")
    return (name_id[0], None)

async def download_resume(name, resumeURL, session):
    try:
        # Download actual resume
        async with session.get(url=resumeURL, proxy=proxy) as response:
            resp = await response.read()
            file_ext = resumeURL.split("?")[0].split(".")[-1]
            file = open(f"{save_path}/{name}.{file_ext}", "wb")
            file.write(resp)
            file.close()
            print(f"Downloaded {file_ext} for {name}")
    except Exception as e:
        print("Failed to save resume for {} : {}.".format(name, e))

async def main():
    async with aiohttp.ClientSession() as session:
        name_resume_urls = await asyncio.gather(*[fetch_candidate_profile(name_id, session) for name_id in name_id_pairs])
        name_resume_urls = [x for x in name_resume_urls if x[1] != None]
        await asyncio.gather(*[download_resume(name_resume_url[0], name_resume_url[1], session) for name_resume_url in name_resume_urls])
        print(f"Done downloading {len(name_resume_urls)} candidates' resumes!")
        
asyncio.run(main())

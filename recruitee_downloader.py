import aiohttp
import requests
import asyncio
import json
import os
import csv
from datetime import datetime
import sys

def flatten_placements(placements):
    """Flatten the placements list to a dict with prefixed keys."""
    flat = {}
    for placement in placements:
        for key, value in placement.items():
            if isinstance(value, list) and key == 'locations' and value:
                # Assuming there's only one location per placement for simplicity
                location = value[0]
                for loc_key, loc_value in location.items():
                    flat[f'location_{loc_key}'] = loc_value
            elif isinstance(value, dict):
                # Flattening nested dictionaries like 'offer'
                for sub_key, sub_value in value.items():
                    flat[f'{key}_{sub_key}'] = sub_value
            else:
                flat[key] = value
    return flat

def process_candidate(candidate):
    # Example of flattening placements, similar to previous examples
    flat_placements = flatten_placements(candidate.pop('placements', []))
    # Directly extract email, phone, and assuming 'socials' is a list or dict
    email = candidate.get('email', '')
    phone = candidate.get('phone', '')
    socials = json.dumps(candidate.get('socials', {}))  # Convert socials to a JSON string if it's a dict/list
    # Merge everything into a single dict
    extended_candidate = {**candidate, **flat_placements, 'email': email, 'phone': phone, 'socials': socials}
    return extended_candidate

def sanitize_data(candidate):
    sanitized_candidate = {}
    for key, value in candidate.items():
        if isinstance(value, str):
            # Replace line breaks with spaces and escape quotes
            sanitized_value = value.replace('\n', ' ').replace('\r', ' ').replace('"', '""')
        else:
            # Convert non-string values to string and escape quotes
            sanitized_value = json.dumps(value).replace('"', '""')
        sanitized_candidate[key] = sanitized_value
    return sanitized_candidate


args = sys.argv[1:]
proxy = os.environ.get('HTTP_PROXY')

if len(args) == 0:
    print("Usage: python3 recruitee_downloader.py [recruiter-link]")
    sys.exit(0)

recruiter_link = args[0]
container_id = recruiter_link.split("/")[-1]

save_path = f'./resumes_batch_{container_id}_{datetime.now().isoformat().split("T")[0]}'
if not os.path.exists(save_path):
    os.makedirs(save_path)

# Fetch basic candidate list for IDs
response = requests.get(f"https://api.recruitee.com/share/containers/{container_id}")
if not response.ok:
    print("Failed to fetch candidates.")
    sys.exit(-1)

can_list = json.loads(response.content)['container']['candidates']
name_id_pairs = [(can['name'], can['id']) for can in can_list]

# Async function to fetch detailed candidate profile including cv_url
async def fetch_candidate_profile(name_id, session):
    try:
        async with session.get(f"https://api.recruitee.com/share/containers/{container_id}/candidates/{name_id[1]}", proxy=proxy) as response:
            if response.status == 200:
                can_data = await response.json()
                candidate = process_candidate(can_data['candidate'])
                return candidate
    except Exception as e:
        print(f"Failed to fetch candidate profile for {name_id[0]}: {e}")
    return None


async def download_image(name, image_url, session):
    if not image_url:  # Skip if no image URL
        return
    try:
        safe_name = "".join(x for x in name if x.isalnum() or x in " ._").rstrip()
        file_path = os.path.join(save_path, f"{safe_name}_photo.jpg")

        async with session.get(url=image_url, proxy=proxy) as response:
            if response.status == 200:
                with open(file_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
                print(f"Downloaded image for {name}")
    except Exception as e:
        print(f"Failed to download image for {name}: {e}")

async def download_resume(name, resume_url, session):
    if not resume_url:
        print(f"No resume URL for {name}")
        return
    try:
        safe_name = "".join(x for x in name if x.isalnum() or x in " ._").rstrip()
        file_path = os.path.join(save_path, f"{safe_name}_resume.pdf")

        async with session.get(url=resume_url, proxy=proxy) as response:
            if response.status == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'application/pdf' in content_type:
                    with open(file_path, 'wb') as f:
                        while True:
                            chunk = await response.content.read(1024)
                            if not chunk:
                                break
                            f.write(chunk)
                    print(f"Downloaded resume for {name}")
                else:
                    print(f"Unexpected content type for {name}: {content_type}")
            else:
                print(f"Failed to download resume for {name}, status: {response.status}")
    except Exception as e:
        print(f"Exception while downloading resume for {name}: {e}")


async def main():
    async with aiohttp.ClientSession() as session:
        # Fetch and process detailed profiles for all candidates
        extended_candidates = await asyncio.gather(*[fetch_candidate_profile(name_id, session) for name_id in name_id_pairs])
        extended_candidates = [c for c in extended_candidates if c]  # Filter out any failed fetches
        extended_candidates = [sanitize_data(candidate) for candidate in extended_candidates]

        # Save processed candidate data to CSV
        csv_file_path = os.path.join(save_path, f'candidates_{container_id}.csv')
        fieldnames = set().union(*(candidate.keys() for candidate in extended_candidates))
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=sorted(fieldnames))
            writer.writeheader()
            for candidate in extended_candidates:
                writer.writerow(candidate)
        print(f"Candidate data saved to {csv_file_path}")

        # Download images and resumes after processing candidate data
        tasks = []
        for candidate in extended_candidates:
            name = candidate.get('name', 'Unknown')
            photo_url = candidate.get('photo_thumb_url')
            resume_url = candidate.get('cv_url')
            if photo_url:
                tasks.append(download_image(name, photo_url, session))
            if resume_url:
                tasks.append(download_resume(name, resume_url, session))
        await asyncio.gather(*tasks)
        print("Done downloading images and resumes.")

# Make sure this is the last line of your script file
if __name__ == "__main__":
    asyncio.run(main())
# recruitee-downloader
Downloads all pdf/ docx resume files, profile images and generates a CSV with candidates in a batch, given a Recruitee URL

## Install

```
pip install -r requirements.txt
```

## Usage

Generate a share URL from recruitee as described here: https://support.recruitee.com/en/articles/1066344-share-candidates-with-team-members-and-guests

Use that URL as a parameter for this script.

```
python3 recruitee_downloader.py <https://[my_company].recruitee.com/v/share/[my_candidate_batch]>
```

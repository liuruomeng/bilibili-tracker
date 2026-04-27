# Bilibili Weekly Must-Watch Collector

Collects metadata, comments (including nested replies), and bullet comments from Bilibili's weekly must-watch videos.

## Installation

```bash
git clone https://github.com/yourusername/bilibili-tracker.git

cd bilibili-tracker

pip install -r requirements.txt

```

## Usage

1. Log in to the Bilibili web version and copy the cookie from your browser's developer tools.

2. Set environment variables:

```bash
export BILI_COOKIE="SESSDATA=xxx; bili_jct=yyy; buvid3=zzz; ..."

```

3. Run:

```bash
python crawler.py init --from 369 --to 370 # Retrieve episode numbers and video list
python crawler.py run # Collect comments and bullet comments
python crawler.py status # Check progress
python crawler.py retry # Reset failed tasks

```

## Optional Environment Variables

- `BILI_DB`: Database path, default `bili.db`

- `BILI_QPS`: Request rate, default `1.5`

- `BILI_WORKERS`: Number of concurrent threads, default `2`

- `BILI_LOG`: Log path, default `bili.log`

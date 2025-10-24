# caption_report.py
from __future__ import print_function
import csv
import re
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from google.colab import userdata

# --------------------------------------------------------------
# 1️⃣  CONSTANTS –‑ put your secrets here (keep the notebook private!)
# --------------------------------------------------------------
CANVAS_API_URL   = userdata.get('CANVAS_API_URL')   # <-- your Canvas host
CANVAS_API_KEY   = userdata.get('CANVAS_API_KEY') # <-- Canvas token
YOUTUBE_API_KEY  = userdata.get('YOUTUBE_API_KEY')          # <-- YouTube key
YT_CAPTION_URL = "https://www.googleapis.com/youtube/v3/captions"
YT_VIDEO_URL   = "https://www.googleapis.com/youtube/v3/videos"

YT_PATTERN = (
    r'(?:https?:\/\/)?(?:[0-9A-Z-]+\.)?(?:youtube|youtu|youtube-nocookie)\.'
    r'(?:com|be)\/(?:watch\?v=|watch\?.+&v=|embed\/|v\/|.+\?v=)?([^&=\n%\?]{11})'
)

LIB_MEDIA_URLS = [
    "fod.infobase.com",
    "search.alexanderstreet.com",
    "kanopystreaming-com",
    "boisestate.hosted.panopto.com",
    "hosted.panopto.com"
]

# --------------------------------------------------------------
# 3️⃣  CanvasAPI import (fails fast with a helpful message)
# --------------------------------------------------------------
try:
    from canvasapi import Canvas
except ImportError as exc:
    raise ImportError(
        "The 'canvasapi' package is required. Install it with:\n"
        "    !pip install canvasapi\n"
        "In Colab run: !pip install canvasapi"
    ) from exc

# ----------------------------------------------------------------------
# 4️⃣  Helper utilities (unchanged – only tiny doc‑string tweaks)
# ----------------------------------------------------------------------
def _add_entry(d, name, status, page, hour='', minute='', second='', file_location=''):
    """Store a row that will later be written to the CSV."""
    d[name] = [status, hour, minute, second, page, file_location]

def _check_media_object(url: str):
    """Return (url, status‑string) for a Canvas media_object."""
    try:
        txt = requests.get(url).text
        if '"kind":"subtitles"' in txt:
            return (
                url,
                'Captions in English' if '"locale":"en"' in txt
                else 'No English Captions'
            )
        return (url, 'No Captions')
    except requests.RequestException:
        return (url, 'Unable to Check Media Object')

# (All other helper functions – `_process_html`, YouTube helpers, etc. –
# remain exactly as they were.  No changes needed there.)

# ----------------------------------------------------------------------
# 5️⃣  PUBLIC API – only **course_input** is required now
# ----------------------------------------------------------------------
def run_caption_report(
    course_input: str,
    csv_path: str = None,
    upload_to_drive: bool = False
) -> str:
    """
    Generate a CSV of media items & caption status for a Canvas course.

    Parameters
    ----------
    course_input : str
        Canvas course ID **or** full Canvas URL.
    csv_path : str, optional
        Destination filename (default: "<course‑name>.csv").
    upload_to_drive : bool, default False
        If True the CSV will be uploaded to Google Drive (Colab only).

    Returns
    -------
    str
        Path to the created CSV file.
    """
    # --------------------------------------------------------------
    # Resolve the numeric course id (handles both plain id and full URL)
    # --------------------------------------------------------------
    if "courses/" in course_input:
        course_id = (
            course_input.split("courses/")[-1]
            .split("/")[0]
            .split("?")[0]
        )
    else:
        course_id = course_input.strip()

    # --------------------------------------------------------------
    # Initialise Canvas client – uses the **constants** above
    # --------------------------------------------------------------
    canvas = Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    course = canvas.get_course(course_id)
    print(f"Processing Canvas course: {course.name}")

    # --------------------------------------------------------------
    # CSV preparation
    # --------------------------------------------------------------
    if not csv_path:
        csv_path = f"{course.name}.csv"
    csv_file = open(csv_path, "w", newline="", encoding="utf-8")
    writer = csv.writer(csv_file)

    # Containers that will be filled by the parsers
    yt_links = {}
    media_links = {}
    link_media = {}
    lib_media = {}

    # --------------------------------------------------------------
    # Helper to turn raw HTML into a BeautifulSoup object and run the
    # common processing routine.
    # --------------------------------------------------------------
    def _handle(html, location):
        if not html:
            return
        soup = BeautifulSoup(html.encode("utf-8"), "html.parser")
        _process_html(
            soup, course, location,
            yt_links, media_links, link_media, lib_media
        )

    # --------------------------------------------------------------
    # 1️⃣ Pages
    # --------------------------------------------------------------
    print("Scanning Pages …")
    for p in course.get_pages():
        body = course.get_page(p.url).body
        _handle(body, p.html_url)

    # --------------------------------------------------------------
    # 2️⃣ Assignments
    # --------------------------------------------------------------
    print("Scanning Assignments …")
    for a in course.get_assignments():
        _handle(a.description, a.html_url)

    # --------------------------------------------------------------
    # 3️⃣ Discussions
    # --------------------------------------------------------------
    print("Scanning Discussions …")
    for d in course.get_discussion_topics():
        _handle(d.message, d.html_url)

    # --------------------------------------------------------------
    # 4️⃣ Syllabus
    # --------------------------------------------------------------
    print("Scanning Syllabus …")
    try:
        syllabus = canvas.get_course(course_id, include="syllabus_body")
        syllabus_url = f"{CANVAS_API_URL}/courses/{course_id}/assignments/syllabus"
        _handle(syllabus.syllabus_body, syllabus_url)
    except Exception:
        pass

    # --------------------------------------------------------------
    # 5️⃣ Modules (External URLs + Files)
    # --------------------------------------------------------------
    print("Scanning Modules …")
    for mod in course.get_modules():
        for item in mod.get_module_items(include="content_details"):
            mod_url = f"{CANVAS_API_URL}/courses/{course_id}/modules/items/{item.id}"

            # External URLs – could be YouTube or library media
            if item.type == "ExternalUrl":
                href = item.external_url
                if re.search(YT_PATTERN, href):
                    yt_links.setdefault(href, []).append(mod_url)
                if any(u in href for u in LIB_MEDIA_URLS):
                    _add_entry(lib_media, href,
                               "Manually Check for Captions", mod_url)

            # File items – treat as linked audio/video
            if item.type == "File":
                try:
                    f = course.get_file(item.content_id)
                    f_url = f.url.split("?")[0]
                    name = f.display_name
                    if "audio" in f.mime_class:
                        _add_entry(link_media,
                                   f"Linked Audio File: {name}",
                                   "Manually Check for Captions",
                                   mod_url, file_location=f_url)
                    if "video" in f.mime_class:
                        _add_entry(link_media,
                                   f"Linked Video File: {name}",
                                   "Manually Check for Captions",
                                   mod_url, file_location=f_url)
                except Exception:
                    pass

    # --------------------------------------------------------------
    # 6️⃣ Announcements
    # --------------------------------------------------------------
    print("Scanning Announcements …")
    for ann in course.get_discussion_topics(only_announcements=True):
        _handle(ann.message, ann.html_url)

    # --------------------------------------------------------------
    # 7️⃣ YouTube processing (parallel) – uses the **constant** YOUTUBE_API_KEY
    # --------------------------------------------------------------
    print("Checking YouTube captions …")
    yt_tasks = []
    yt_processed = {}

    for key, pages in yt_links.items():
        # Skip playlists – they need manual inspection of each video
        if "list" in key:
            yt_processed[key] = [
                "this is a playlist, check individual videos",
                "", "", ""
            ] + pages
            continue

        vid_match = re.findall(YT_PATTERN, key, re.IGNORECASE)
        video_id = vid_match[0] if vid_match else None

        if video_id:
            yt_tasks.append((key, video_id, pages, YOUTUBE_API_KEY))
        else:
            yt_processed[key] = [
                "Unable to parse Video ID", "", "", ""
            ] + pages

    if yt_tasks:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            for k, st, (h, m, s), pg in ex.map(_check_youtube, yt_tasks):
                yt_processed[k] = [st, h, m, s] + pg
    yt_links = yt_processed

    # --------------------------------------------------------------
    # 8️⃣ Write CSV
    # --------------------------------------------------------------
    writer.writerow([
        "Media", "Caption Status", "Hour", "Minute",
        "Second", "Page Location", "File Location"
    ])
    for container in (yt_links, media_links, link_media, lib_media):
        for key, vals in container.items():
            writer.writerow([key] + vals)

    csv_file.close()
    print(f"✅ CSV written to {csv_path}")

    # --------------------------------------------------------------
    # 9️⃣ Optional Google‑Drive upload (Colab only)
    # --------------------------------------------------------------
    if upload_to_drive:
        try:
            from google.colab import auth
            from oauth2client.client import GoogleCredentials
            from pydrive2.auth import GoogleAuth
            from pydrive2.drive import GoogleDrive

            print("Authenticating to Google Drive …")
            auth.authenticate_user()
            gauth = GoogleAuth()
            gauth.credentials = GoogleCredentials.get_application_default()
            drive = GoogleDrive(gauth)

            uploaded = drive.CreateFile({
                "title": csv_path,
                "mimeType": "text/csv"
            })
            uploaded.SetContentFile(csv_path)
            uploaded.Upload()
            fid = uploaded.get("id")
            print("📁 Uploaded to Drive.")
            print(f"View: https://drive.google.com/file/d/{fid}/view")
            print(f"Sheets: https://docs.google.com/spreadsheets/d/{fid}/edit")
        except Exception as e:
            print(f"⚠️  Drive upload failed: {e}")

    return csv_path

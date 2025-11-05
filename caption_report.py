from __future__ import print_function
import re
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from google.colab import userdata
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import math

# --------------------------------------------------------------
# 1️⃣ CONSTANTS – your secrets (keep notebook private)
# --------------------------------------------------------------
CANVAS_API_URL   = userdata.get('CANVAS_API_URL')
CANVAS_API_KEY   = userdata.get('CANVAS_API_KEY')
YOUTUBE_API_KEY  = userdata.get('YOUTUBE_API_KEY')

YT_CAPTION_URL = "https://www.googleapis.com/youtube/v3/captions"
YT_VIDEO_URL   = "https://www.googleapis.com/youtube/v3/videos"

YT_PATTERN = (
    r'(?:https?://)?(?:[0-9A-Z-]+.)?(?:youtube|youtu|youtube-nocookie).'
    r'(?:com|be)/(?:watch\?v=|watch\?.+&v=|embed/|v/|.+\?v=)?([^&=\n%\?]{11})'
)

LIB_MEDIA_URLS = [
    "fod.infobase.com",
    "search.alexanderstreet.com",
    "kanopystreaming-com",
    "hosted.panopto.com"
]

# ----------------------------------------------------------------------
# CanvasAPI
# ----------------------------------------------------------------------
try:
    from canvasapi import Canvas
except ImportError as exc:
    raise ImportError("Please install canvasapi via `!pip install canvasapi`") from exc

# ----------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------
def _auth_header(token: str) -> dict:
    return {"Authorization": f"Bearer {token.strip()}"}

def _add_entry(d, name, status, page, hour="", minute="", second="", file_location=""):
    d[name] = [status, hour, minute, second, page, file_location]

def _check_media_object(url: str):
    try:
        txt = requests.get(url, headers=_auth_header(CANVAS_API_KEY)).text
        if '"kind":"subtitles"' in txt:
            return (url, "Captions in English" if '"locale":"en"' in txt else "No English Captions")
        return (url, "No Captions")
    except requests.RequestException:
        return (url, "Unable to Check Media Object")

def _process_html(soup, course, page, yt_links, media_links, link_media, lib_media):
    media_objs, iframe_objs = [], []

    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue
        try:
            file_id = a.get("data-api-endpoint").split("/")[-1]
            f = course.get_file(file_id)
            f_url = f.url.split("?")[0]
            if "audio" in f.mime_class:
                _add_entry(link_media, f"Linked Audio File: {f.display_name}",
                           "Manually Check for Captions", page, file_location=f_url)
            if "video" in f.mime_class:
                _add_entry(link_media, f"Linked Video File: {f.display_name}",
                           "Manually Check for Captions", page, file_location=f_url)
        except Exception:
            pass

        if re.search(YT_PATTERN, href):
            yt_links.setdefault(href, []).append(page)
        elif any(u in href for u in LIB_MEDIA_URLS):
            _add_entry(lib_media, href, "Manually Check for Captions", page)
        elif "media_objects" in href:
            media_objs.append(href)

    for frm in soup.find_all("iframe"):
        src = frm.get("src")
        if not src:
            continue
        if re.search(YT_PATTERN, src):
            yt_links.setdefault(src, []).append(page)
        elif any(u in src for u in LIB_MEDIA_URLS):
            _add_entry(lib_media, src, "Manually Check for Captions", page)
        elif "media_objects_iframe" in src:
            iframe_objs.append(src)

    all_media = list(set(media_objs + iframe_objs))
    if all_media:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            for url, msg in ex.map(_check_media_object, all_media):
                _add_entry(media_links, url, msg, page)

    for vid in soup.find_all("video"):
        if vid.get("data-media_comment_id"):
            name = f"Video Media Comment {vid['data-media_comment_id']}"
            status = "Captions" if vid.find("track") else "No Captions"
            _add_entry(media_links, name, status, page)

    for src in soup.find_all("source"):
        if src.get("type") == "video/mp4":
            name = f"Embedded Canvas Video {src['src']}"
            _add_entry(media_links, name, "Manually Check for Captions", page)

    for aud in soup.find_all("audio"):
        if aud.get("data-media_comment_id"):
            name = f"Audio Media Comment {aud['data-media_comment_id']}"
            status = "Captions" if aud.find("track") else "No Captions"
            _add_entry(media_links, name, status, page)
        else:
            name = f"Embedded Canvas Audio {aud.get('src', '')}"
            _add_entry(media_links, name, "Manually Check for Captions", page)

# ----------------------------------------------------------------------
# YouTube Helpers
# ----------------------------------------------------------------------
YT_DUR_RE = re.compile(r"[0-9]+[HMS]")

def _parse_iso8601(duration: str):
    h, m, sec = "0", "0", "0"
    for token in YT_DUR_RE.findall(duration):
        unit = token[-1]
        val = token[:-1]
        if unit == "H":
            h = val
        elif unit == "M":
            m = val
        elif unit == "S":
            sec = val
    return h, m, sec

def _check_youtube(task):
    key, vid, pages, api_key = task
    if not vid:
        return key, "this is a playlist, check individual videos", ("", "", ""), pages
    try:
        r1 = requests.get(f"{YT_VIDEO_URL}?part=contentDetails&id={vid}&key={api_key}")
        dur = r1.json()["items"][0]["contentDetails"]["duration"]
        h, m, s = _parse_iso8601(dur)

        r2 = requests.get(f"{YT_CAPTION_URL}?part=snippet&videoId={vid}&key={api_key}")
        caps = r2.json().get("items", [])
        status = "No Captions"
        if caps:
            langs = {c["snippet"]["language"]: c["snippet"]["trackKind"] for c in caps}
            if "en" in langs or "en-US" in langs:
                kind = langs.get("en") or langs.get("en-US")
                if kind == "standard":
                    status = "Captions found in English"
                elif kind == "asr":
                    status = "Automatic Captions in English"
                else:
                    status = "Captions in English (unknown kind)"
            else:
                status = "No Captions in English"
        return key, status, (h, m, s), pages
    except Exception:
        return key, "Unable to Check Youtube Video", ("", "", ""), pages

# ----------------------------------------------------------------------
# NEW FUNCTIONS: Time handling and totaling
# ----------------------------------------------------------------------
def _consolidate_time(hour_str, minute_str, second_str):
    """
    Convert hour, minute, second strings to consolidated "HH:MM" format.
    Rounds up seconds to the next minute if seconds > 0.
    Returns tuple: (formatted_string, total_minutes_for_summing)
    """
    try:
        hours = int(hour_str) if hour_str and hour_str.strip() else 0
        minutes = int(minute_str) if minute_str and minute_str.strip() else 0
        seconds = int(second_str) if second_str and second_str.strip() else 0
        
        # Calculate total minutes for summing (before rounding up seconds)
        total_minutes = hours * 60 + minutes + (1 if seconds > 0 else 0)
        
        # Round up to next minute if there are any seconds
        if seconds > 0:
            minutes += 1
        
        # Handle minute overflow
        if minutes >= 60:
            hours += minutes // 60
            minutes = minutes % 60
        
        return f"{hours:02d}:{minutes:02d}", total_minutes
    except (ValueError, TypeError):
        return "", 0

def _minutes_to_duration(total_minutes):
    """Convert total minutes back to HH:MM format"""
    if total_minutes <= 0:
        return "00:00"
    
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{hours:02d}:{minutes:02d}"

# ----------------------------------------------------------------------
# MAIN FUNCTION
# ----------------------------------------------------------------------
def run_caption_report(course_input: str) -> str:
    """Generate caption report and write directly to a Google Sheet (replace existing content if sheet exists)."""

    # Authenticate Google Sheets for Colab
    print("🔐 Authenticating with Google Sheets …")
    from google.colab import auth
    from google.auth import default
    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)

    # Get Canvas course
    if "courses/" in course_input:
        course_id = course_input.split("courses/")[-1].split("/")[0].split("?")[0]
    else:
        course_id = course_input.strip()

    canvas = Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    course = canvas.get_course(course_id)
    print(f"\n📘 Processing Canvas course: {course.name}\n")

    # Data containers
    yt_links, media_links, link_media, lib_media = {}, {}, {}, {}

    def _handle(html, location):
        if not html:
            return
        soup = BeautifulSoup(html.encode("utf-8"), "html.parser")
        _process_html(soup, course, location, yt_links, media_links, link_media, lib_media)

    # --------------------------------------------------------------
    # Scanning sections with printouts
    # --------------------------------------------------------------
    print("🔎 Scanning Pages …")
    for p in course.get_pages():
        _handle(course.get_page(p.url).body, p.html_url)

    print("🔎 Scanning Assignments …")
    for a in course.get_assignments():
        _handle(a.description, a.html_url)

    print("🔎 Scanning Discussions …")
    for d in course.get_discussion_topics():
        _handle(d.message, d.html_url)

    print("🔎 Scanning Syllabus …")
    try:
        syllabus = canvas.get_course(course_id, include="syllabus_body")
        _handle(syllabus.syllabus_body, f"{CANVAS_API_URL}/courses/{course_id}/assignments/syllabus")
    except Exception:
        print("⚠️  Could not load syllabus.")
        pass

    print("🔎 Scanning Modules …")
    for mod in course.get_modules():
        for item in mod.get_module_items(include="content_details"):
            mod_url = f"{CANVAS_API_URL}/courses/{course_id}/modules/items/{item.id}"
            if item.type == "ExternalUrl":
                href = item.external_url
                if re.search(YT_PATTERN, href):
                    yt_links.setdefault(href, []).append(mod_url)
                if any(u in href for u in LIB_MEDIA_URLS):
                    _add_entry(lib_media, href, "Manually Check for Captions", mod_url)
            if item.type == "File":
                try:
                    f = course.get_file(item.content_id)
                    f_url = f.url.split("?")[0]
                    name = f.display_name
                    if "audio" in f.mime_class:
                        _add_entry(link_media, f"Linked Audio File: {name}", "Manually Check for Captions", mod_url, file_location=f_url)
                    if "video" in f.mime_class:
                        _add_entry(link_media, f"Linked Video File: {name}", "Manually Check for Captions", mod_url, file_location=f_url)
                except Exception:
                    pass

    print("🔎 Scanning Announcements …")
    for ann in course.get_discussion_topics(only_announcements=True):
        _handle(ann.message, ann.html_url)

    # --------------------------------------------------------------
    # YouTube processing
    # --------------------------------------------------------------
    print("\n▶️  Checking YouTube captions …")
    yt_tasks, yt_processed = [], {}
    for key, pages in yt_links.items():
        if "list" in key:
            yt_processed[key] = ["this is a playlist, check individual videos", "", "", ""] + pages
            continue
        vid_match = re.findall(YT_PATTERN, key, re.IGNORECASE)
        video_id = vid_match[0] if vid_match else None
        if video_id:
            yt_tasks.append((key, video_id, pages, YOUTUBE_API_KEY))
        else:
            yt_processed[key] = ["Unable to parse Video ID", "", "", ""] + pages

    if yt_tasks:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            for k, st, (h, m, s), pg in ex.map(_check_youtube, yt_tasks):
                yt_processed[k] = [st, h, m, s] + pg
    yt_links = yt_processed

    # --------------------------------------------------------------
    # Combine results into a DataFrame with consolidated time and totaling
    # --------------------------------------------------------------
    print("\n📊 Compiling results …")

    # Check if there are any linked audio/video files
    has_linked_files = len(link_media) > 0

    rows = []
    total_minutes = 0  # Track total duration
    
    for container in (yt_links, media_links, link_media, lib_media):
        for key, vals in container.items():
            # Extract time components (if they exist)
            if len(vals) >= 4:
                status, hour, minute, second = vals[0], vals[1], vals[2], vals[3]
                location = vals[4] if len(vals) > 4 else ""
                file_location = vals[5] if len(vals) > 5 else ""
                
                # Consolidate time and get minutes for totaling
                duration, minutes_to_add = _consolidate_time(hour, minute, second)
                total_minutes += minutes_to_add
                
                # Build row based on whether we have file locations
                if has_linked_files:
                    rows.append([key, status, duration, location, file_location])
                else:
                    rows.append([key, status, duration, location])
            else:
                # Fallback for entries without time data
                if has_linked_files:
                    rows.append([key] + vals + [""] * (5 - len(vals)))
                else:
                    rows.append([key] + vals + [""] * (4 - len(vals)))

    # Add total row
    total_duration = _minutes_to_duration(total_minutes)
    if has_linked_files:
        total_row = ["Total Duration", "", total_duration, "", ""]
    else:
        total_row = ["Total Duration", "", total_duration, ""]
    
    rows.append(total_row)

    # Define columns based on whether there are linked files
    if has_linked_files:
        columns = [
            "Media", "Caption Status", "Duration (HH:MM)", "Location", "File Location"
        ]
    else:
        columns = [
            "Media", "Caption Status", "Duration (HH:MM)", "Location"
        ]

    df = pd.DataFrame(rows, columns=columns)

    # --------------------------------------------------------------
    # Create or replace Google Sheet
    # --------------------------------------------------------------
    print("\n📄 Creating or updating Google Sheet …")
    sheet_title = f"{course.name} VAST Report"

    try:
        existing_sheets = gc.list_spreadsheet_files()
        sheet = next((s for s in existing_sheets if s["name"] == sheet_title), None)
    except Exception:
        sheet = None

    if sheet:
        print(f"♻️  Found existing sheet: {sheet_title}. Replacing contents …")
        sh = gc.open_by_key(sheet["id"])
        ws = sh.sheet1
        ws.clear()
    else:
        print(f"🆕 No existing sheet found. Creating new sheet: {sheet_title}")
        sh = gc.create(sheet_title)
        ws = sh.sheet1

    set_with_dataframe(ws, df)
    try:
        sh.share('', perm_type='anyone', role='reader')
    except Exception:
        pass

    print(f"\n✅ Report complete for: {course.name}")
    print(f"📎 Google Sheet URL: {sh.url}")
    print(f"⏱️  Total media duration: {total_duration}")

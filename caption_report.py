# caption_report_sheets.py
from __future__ import print_function
import re
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from google.colab import userdata, auth as colab_auth
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd

# --------------------------------------------------------------
# 1️⃣ CONSTANTS – your secrets (keep notebook private)
# --------------------------------------------------------------
CANVAS_API_URL   = userdata.get('CANVAS_API_URL')
CANVAS_API_KEY   = userdata.get('CANVAS_API_KEY')
YOUTUBE_API_KEY  = userdata.get('YOUTUBE_API_KEY')

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
# MAIN FUNCTION
# ----------------------------------------------------------------------
def run_caption_report(course_input: str) -> str:
    """Generate caption report and write directly to a Google Sheet."""
    # Authenticate for Sheets
    print("Authenticating with Google Sheets …")
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
    print(f"Processing Canvas course: {course.name}")

    # Containers
    yt_links, media_links, link_media, lib_media = {}, {}, {}, {}

    def _handle(html, location):
        if not html:
            return
        soup = BeautifulSoup(html.encode("utf-8"), "html.parser")
        _process_html(soup, course, location, yt_links, media_links, link_media, lib_media)

    print("Scanning course content …")
    for p in course.get_pages():
        _handle(course.get_page(p.url).body, p.html_url)
    for a in course.get_assignments():
        _handle(a.description, a.html_url)
    for d in course.get_discussion_topics():
        _handle(d.message, d.html_url)
    try:
        syllabus = canvas.get_course(course_id, include="syllabus_body")
        _handle(syllabus.syllabus_body, f"{CANVAS_API_URL}/courses/{course_id}/assignments/syllabus")
    except Exception:
        pass
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
    for ann in course.get_discussion_topics(only_announcements=True):
        _handle(ann.message, ann.html_url)

    # YouTube processing
    print("Checking YouTube captions …")
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

    # Combine all results into a DataFrame
    rows = []
    for container in (yt_links, media_links, link_media, lib_media):
        for key, vals in container.items():
            rows.append([key] + vals)
    df = pd.DataFrame(rows, columns=[
        "Media", "Caption Status", "Hour", "Minute", "Second", "Page Location", "File Location"
    ])

    # Create a new Google Sheet and write data
    print("Creating Google Sheet …")
    sheet_title = f"{course.name} Caption Report"
    sh = gc.create(sheet_title)
    ws = sh.sheet1
    set_with_dataframe(ws, df)
    sh.share('', perm_type='anyone', role='reader')

    print(f"✅ Report written to Google Sheet: {sh.url}")
    return sh.url


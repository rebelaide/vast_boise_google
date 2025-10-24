# caption_report.py
from __future__ import print_function
import csv
import re
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from canvasapi import Canvas

# ----------------------------------------------------------------------
# CONSTANTS that never change
# ----------------------------------------------------------------------
YT_CAPTION_URL = 'https://www.googleapis.com/youtube/v3/captions'
YT_VIDEO_URL   = 'https://www.googleapis.com/youtube/v3/videos'
CANVAS_API_KEY   = userdata.get('CANVAS_API_KEY')      # e.g. "abcd1234"
YOUTUBE_API_KEY  = userdata.get('YOUTUBE_API_KEY')
CANVAS_API_URL   = userdata.get('CANVAS_API_URL')

# Regex that extracts the 11‑character YouTube video id
YT_PATTERN = (
    r'(?:https?:\/\/)?(?:[0-9A-Z-]+\.)?(?:youtube|youtu|youtube-nocookie)\.'
    r'(?:com|be)\/(?:watch\?v=|watch\?.+&v=|embed\/|v\/|.+\?v=)?([^&=\n%\?]{11})'
)

# Library‑media domains (keep the original domain plus a short form)
LIB_MEDIA_URLS = [
    'fod.infobase.com',
    'search.alexanderstreet.com',
    'kanopystreaming-com',
    'boisestate.hosted.panopto.com',
    'hosted.panopto.com'
]

# ----------------------------------------------------------------------
# Helper utilities
# ----------------------------------------------------------------------
def _add_entry(
    d,
    name,
    status,
    page,
    hour: str = '',
    minute: str = '',
    second: str = '',
    file_location: str = ''
):
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


def _process_html(
    soup,
    course,
    page,
    yt_links,
    media_links,
    link_media,
    lib_media
):
    """Parse a BeautifulSoup page and fill the dict containers."""
    media_objs, iframe_objs = [], []

    # ----- <a> tags -------------------------------------------------
    for a in soup.find_all('a'):
        href = a.get('href')
        if not href:
            continue

        # Canvas file links (audio / video)
        try:
            file_id = a.get('data-api-endpoint').split('/')[-1]
            f = course.get_file(file_id)
            f_url = f.url.split('?')[0]

            if 'audio' in f.mime_class:
                _add_entry(
                    link_media,
                    f'Linked Audio File: {f.display_name}',
                    'Manually Check for Captions',
                    page,
                    file_location=f_url
                )
            if 'video' in f.mime_class:
                _add_entry(
                    link_media,
                    f'Linked Video File: {f.display_name}',
                    'Manually Check for Captions',
                    page,
                    file_location=f_url
                )
        except Exception:
            pass

        # Classify the link
        if re.search(YT_PATTERN, href):
            yt_links.setdefault(href, []).append(page)
        elif any(u in href for u in LIB_MEDIA_URLS):
            _add_entry(lib_media, href,
                       'Manually Check for Captions', page)
        elif 'media_objects' in href:
            media_objs.append(href)

    # ----- <iframe> tags -------------------------------------------
    for frm in soup.find_all('iframe'):
        src = frm.get('src')
        if not src:
            continue
        if re.search(YT_PATTERN, src):
            yt_links.setdefault(src, []).append(page)
        elif any(u in src for u in LIB_MEDIA_URLS):
            _add_entry(lib_media, src,
                       'Manually Check for Captions', page)
        elif 'media_objects_iframe' in src:
            iframe_objs.append(src)

    # ----- Canvas media objects (parallel) -------------------------
    all_media = list(set(media_objs + iframe_objs))
    if all_media:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            for url, msg in ex.map(_check_media_object, all_media):
                _add_entry(media_links, url, msg, page)

    # ----- <video> tags --------------------------------------------
    for vid in soup.find_all('video'):
        if vid.get('data-media_comment_id'):
            name = f'Video Media Comment {vid["data-media_comment_id"]}'
            status = 'Captions' if vid.find('track') else 'No Captions'
            _add_entry(media_links, name, status, page)

    # ----- <source> tags (embedded Canvas video) -------------------
    for src in soup.find_all('source'):
        if src.get('type') == 'video/mp4':
            name = f'Embedded Canvas Video {src["src"]}'
            _add_entry(media_links, name,
                       'Manually Check for Captions', page)

    # ----- <audio> tags --------------------------------------------
    for aud in soup.find_all('audio'):
        if aud.get('data-media_comment_id'):
            name = f'Audio Media Comment {aud["data-media_comment_id"]}'
            status = 'Captions' if aud.find('track') else 'No Captions'
            _add_entry(media_links, name, status, page)
        else:
            name = f'Embedded Canvas Audio {aud.get("src", "")}'
            _add_entry(media_links, name,
                       'Manually Check for Captions', page)


# ----------------------------------------------------------------------
# YouTube helpers
# ----------------------------------------------------------------------
YT_DUR_RE = re.compile(r'[0-9]+[HMS]')


def _parse_iso8601(duration: str):
    """Convert an ISO‑8601 duration (e.g. PT1H5M10S) → (h,m,s)."""
    h, m, sec = '0', '0', '0'
    for token in YT_DUR_RE.findall(duration):
        unit = token[-1]
        val = token[:-1]
        if unit == 'H':
            h = val
        elif unit == 'M':
            m = val
        elif unit == 'S':
            sec = val
    return h, m, sec


def _check_youtube(task):
    """
    Worker for a single YouTube video.

    task = (key, video_id, pages, youtube_api_key)
    Returns (key, status, (h,m,s), pages)
    """
    key, vid, pages, api_key = task
    if not vid:   # playlist or malformed URL
        return key, 'this is a playlist, check individual videos', ('', '', ''), pages

    try:
        # ---- duration -------------------------------------------------
        r1 = requests.get(
            f'{YT_VIDEO_URL}?part=contentDetails&id={vid}&key={api_key}'
        )
        dur = r1.json()['items'][0]['contentDetails']['duration']
        h, m, s = _parse_iso8601(dur)

        # ---- captions -------------------------------------------------
        r2 = requests.get(
            f'{YT_CAPTION_URL}?part=snippet&videoId={vid}&key={api_key}'
        )
        caps = r2.json().get('items', [])
        status = 'No Captions'

        if caps:
            langs = {
                c['snippet']['language']: c['snippet']['trackKind']
                for c in caps
            }
            if 'en' in langs or 'en-US' in langs:
                kind = langs.get('en') or langs.get('en-US')
                if kind == 'standard':
                    status = 'Captions found in English'
                elif kind == 'asr':
                    status = 'Automatic Captions in English'
                else:
                    status = 'Captions in English (unknown kind)'
            else:
                status = 'No Captions in English'

        return key, status, (h, m, s), pages
    except Exception:
        return key, 'Unable to Check Youtube Video', ('', '', ''), pages


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------
def run_caption_report(
    course_input: str,
    canvas_api_key: str,
    youtube_api_key: str,
    csv_path: str = None,
    upload_to_drive: bool = False,
    canvas_api_url: str = None
) -> str:
    """
    Generate a CSV of media items & caption status for a Canvas course.

    Parameters
    ----------
    course_input : str
        Canvas course ID **or** full Canvas URL.
    canvas_api_key : str
        Canvas personal access token.
    youtube_api_key : str
        YouTube Data API v3 key.
    csv_path : str, optional
        Destination filename (default: "<course‑name>.csv").
    upload_to_drive : bool, default False
        If True the CSV is uploaded to Google Drive (Colab auth required).
    canvas_api_url : str
        Base Canvas URL (e.g. ``https://mycanvas.instructure.com``).  
        **This argument is required** – the caller must supply it.

    Returns
    -------
    str
        Path to the created CSV file.
    """
    if not canvas_api_url:
        raise ValueError('canvas_api_url must be supplied by the caller.')

    # --------------------------------------------------------------
    # Resolve the numeric course id (handles both plain id and full URL)
    # --------------------------------------------------------------
    if 'courses/' in course_input:
        course_id = (
            course_input.split('courses/')[-1]
            .split('/')[0]
            .split('?')[0]
        )
    else:
        course_id = course_input.strip()

    # --------------------------------------------------------------
    # Initialise the Canvas client
    # --------------------------------------------------------------
    canvas = Canvas(canvas_api_url, canvas_api_key)
    course = canvas.get_course(course_id)
    print(f'Processing Canvas course: {course.name}')

    # --------------------------------------------------------------
    # CSV preparation
    # --------------------------------------------------------------
    if not csv_path:
        csv_path = f'{course.name}.csv'
    csv_file = open(csv_path, 'w', newline='', encoding='utf-8')
    writer = csv.writer(csv_file)

    # Containers that will be filled by the parsers
    yt_links = {}
    media_links = {}
    link_media = {}
    lib_media = {}

    # --------------------------------------------------------------
    # Helper that turns raw HTML into a BeautifulSoup object and runs
    # the common processing routine.
    # --------------------------------------------------------------
    def _handle(html, location):
        if not html:
            return
        soup = BeautifulSoup(html.encode('utf-8'), 'html.parser')
        _process_html(
            soup, course, location,
            yt_links, media_links, link_media, lib_media
        )

    # --------------------------------------------------------------
    # 1️⃣ Pages
    # --------------------------------------------------------------
    print('Scanning Pages …')
    for p in course.get_pages():
        body = course.get_page(p.url).body
        _handle(body, p.html_url)

    # --------------------------------------------------------------
    # 2️⃣ Assignments
    # --------------------------------------------------------------
    print('Scanning Assignments …')
    for a in course.get_assignments():
        _handle(a.description, a.html_url)

    # --------------------------------------------------------------
    # 3️⃣ Discussions
    # --------------------------------------------------------------
    print('Scanning Discussions …')
    for d in course.get_discussion_topics():
        _handle(d.message, d.html_url)

    # --------------------------------------------------------------
    # 4️⃣ Syllabus
    # --------------------------------------------------------------
    print('Scanning Syllabus …')
    try:
        syllabus = canvas.get_course(course_id, include='syllabus_body')
        syllabus_url = f'{canvas_api_url}/courses/{course_id}/assignments/syllabus'
        _handle(syllabus.syllabus_body, syllabus_url)
    except Exception:
        pass

    # --------------------------------------------------------------
    # 5️⃣ Modules (External URLs + Files)
    # --------------------------------------------------------------
    print('Scanning Modules …')
    for mod in course.get_modules():
        for item in mod.get_module_items(include='content_details'):
            mod_url = f'{canvas_api_url}/courses/{course_id}/modules/items/{item.id}'

            # External URLs – could be YouTube or library media
            if item.type == 'ExternalUrl':
                href = item.external_url
                if re.search(YT_PATTERN, href):
                    yt_links.setdefault(href, []).append(mod_url)
                if any(u in href for u in LIB_MEDIA_URLS):
                    _add_entry(lib_media, href,
                               'Manually Check for Captions', mod_url)

            # File items – treat as linked audio/video
            if item.type == 'File':
                try:
                    f = course.get_file(item.content_id)
                    f_url = f.url.split('?')[0]
                    name = f.display_name
                    if 'audio' in f.mime_class:
                        _add_entry(link_media,
                                   f'Linked Audio File: {name}',
                                   'Manually Check for Captions',
                                   mod_url, file_location=f_url)
                    if 'video' in f.mime_class:
                        _add_entry(link_media,
                                   f'Linked Video File: {name}',
                                   'Manually Check for Captions',
                                   mod_url, file_location=f_url)
                except Exception:
                    pass

    # --------------------------------------------------------------
    # 6️⃣ Announcements
    # --------------------------------------------------------------
    print('Scanning Announcements …')
    for ann in course.get_discussion_topics(only_announcements=True):
        _handle(ann.message, ann.html_url)

    # --------------------------------------------------------------
    # 7️⃣ YouTube processing (parallel)
    # --------------------------------------------------------------
    print('Checking YouTube captions …')
    yt_tasks = []
    yt_processed = {}

    for key, pages in yt_links.items():
        # Skip playlists – they need manual inspection of each video
        if 'list' in key:
            yt_processed[key] = [
                'this is a playlist, check individual videos',
                '', '', ''
            ] + pages
            continue

        vid_match = re.findall(YT_PATTERN, key, re.IGNORECASE)
        video_id = vid_match[0] if vid_match else None

        if video_id:
            yt_tasks.append((key, video_id, pages, youtube_api_key))
        else:
            yt_processed[key] = [
                'Unable to parse Video ID', '', '', ''
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
        'Media', 'Caption Status', 'Hour', 'Minute',
        'Second', 'Page Location', 'File Location'
    ])
    for container in (yt_links, media_links, link_media, lib_media):
        for key, vals in container.items():
            writer.writerow([key] + vals)

    csv_file.close()
    print(f'✅ CSV written to {csv_path}')

    # --------------------------------------------------------------
    # 9️⃣ Optional Google‑Drive upload (Colab only)
    # --------------------------------------------------------------
    if upload_to_drive:
        try:
            from google.colab import auth
            from oauth2client.client import GoogleCredentials
            from pydrive2.auth import GoogleAuth
            from pydrive2.drive import GoogleDrive

            print('Authenticating to Google Drive …')
            auth.authenticate_user()
            gauth = GoogleAuth()
            gauth.credentials = GoogleCredentials.get_application_default()
            drive = GoogleDrive(gauth)

            uploaded = drive.CreateFile({
                'title': csv_path,
                'mimeType': 'text/csv'
            })
            uploaded.SetContentFile(csv_path)
            uploaded.Upload()
            fid = uploaded.get('id')
            print('📁 Uploaded to Drive.')
            print(f'View: https://drive.google.com/file/d/{fid}/view')
            print(f'Sheets: https://docs.google.com/spreadsheets/d/{fid}/edit')
        except Exception as e:
            print(f'⚠️  Drive upload failed: {e}')

    return csv_path

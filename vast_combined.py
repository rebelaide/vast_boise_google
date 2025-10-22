from __future__ import print_function
import csv
import re
import requests
import concurrent.futures

from bs4 import BeautifulSoup
from canvasapi import Canvas
from six.moves import input


# ==================== CONFIGURATION ====================

# Canvas API Key
api_key = '15177~H8PVFuWL7KkxvzNDc7h2fvLA3rwUAXTaaWYFJHuUa7D2tJJ3rHGwy38hmPRwAxaf'
# Canvas URL
api_url = 'https://boisestatecanvas.instructure.com'

# YouTube API
youtube_key = 'AIzaSyDT9XbAhNzfMv0-cLFH3USN6yjurx1kDkU'
google_url = 'https://www.googleapis.com/youtube/v3/captions'
google_video = 'https://www.googleapis.com/youtube/v3/videos'

# YouTube URL pattern
youtube_pattern = r'(?:https?:\/\/)?(?:[0-9A-Z-]+\.)?(?:youtube|youtu|youtube-nocookie)\.(?:com|be)\/(?:watch\?v=|watch\?.+&v=|embed\/|v\/|.+\?v=)?([^&=\n%\?]{11})'

# Canvas courses URL
courses_url = 'https://boisestatecanvas.instructure.com/courses'

# Library media URLs to check
lib_media_urls = [
    'fod.infobase.com',
    'search.alexanderstreet.com',
    'kanopystreaming-com',
    'boisestate.hosted.panopto.com'
]


# ==================== UTILITY FUNCTIONS ====================

def add_entry(
    media_dict, link_name, caption_status, page_location,
    hour='', minute='', second='', file_location=''
):
    """
    Adds an entry to the provided dictionary at the appropriate key.
    :param media_dict: The dictionary to add the entry to.
    :type media_dict: dict
    :param link_name: The name to identify a link by - usually the URL.
    :type link_name: str
    :param caption_status: A short description of whether or not
        captions were found, what language, or if the user must
        manually check.
    :type caption_status: str
    :param page_location: The full URL to the resource in Canvas.
    :type page_location: str
    :param hour: The hours place of the duration of a video
    :type: str
    :param minute: The minutes place of the duration of a video
    :type: str
    :param second: The seconds place of the duration of a video
    :type: str
    :param file_location: The full URL to the file in Canvas.
    :type: str
    :returns: None
    """
    media_dict.setdefault(link_name, [])
    media_dict[link_name] = [caption_status, hour, minute, second, page_location, file_location]


def check_media_object(link):
    """
    Worker function to check a Canvas media_object link for captions.
    
    :param link: The URL of the media_object.
    :type link: str
    :return: A tuple of (link, message)
    :rtype: tuple
    """
    try:
        media_object_video = requests.get(link)
        page_html = media_object_video.text
        if '"kind":"subtitles"' in page_html:
            if '"locale":"en"' in page_html:
                message = 'Captions in English'
            else:
                message = 'No English Captions'
        else:
            message = 'No Captions'
    except requests.RequestException:
        message = 'Unable to Check Media Object'
    
    return (link, message)


def process_contents(
    soup, course, page_location,
    youtube_link, vimeo_link, media_link, link_media, library_media
):
    """
    Process the provided contents
    :param soup:
    :type soup: :class:`bs4.BeautifulSoup`
    :param course:
    :type course: :class:`canvasapi.course.Course`
    :param page_location: The full URL to the resource in Canvas.
    :type page_location: str or unicode
    """

    # --- Lists to hold links for concurrent processing ---
    media_object_links = []
    iframe_media_links = []

    # Process Anchor Tags
    href_href_list = []
    for link in soup.find_all('a'):
        href_href_list.append(link.get('href'))

        location = link.get('data-api-endpoint')
        try:
            file_id = location.split('/')[-1:]
            file_id_string = ', '.join(file_id)
            get_file = course.get_file(file_id_string)
            file_location = get_file.url.split('?')[0]
            if 'audio' in get_file.mime_class:
                add_entry(
                    link_media,
                    'Linked Audio File: {}'.format(get_file.display_name),
                    'Manually Check for Captions',
                    page_location,
                    file_location=file_location
                )
            if 'video' in get_file.mime_class:
                add_entry(
                    link_media,
                    'Linked Video File: {}'.format(get_file.display_name),
                    'Manually Check for Captions',
                    page_location,
                    file_location=file_location
                )
        except Exception:
            pass

    href_list_filter = filter(None, href_href_list)

    for link in href_list_filter:
        # Matches YouTube
        if re.search(youtube_pattern, link):
            youtube_link.setdefault(link, [])
            youtube_link[link].append(page_location)
        # Matches library media from lib_media_urls
        elif any(match_str in link for match_str in lib_media_urls):
            add_entry(library_media, link, 'Manually Check for Captions', page_location)
        # Matches New RCE Canvas Linked Videos - Add to list for processing
        elif 'media_objects' in link:
            media_object_links.append(link)

    # Process IFrames
    iframe_list = []
    for link in soup.find_all('iframe'):
        iframe_list.append(link.get('src'))
    iframe_list_filter = filter(None, iframe_list)

    for link in iframe_list_filter:
        # Matches YouTube
        if re.search(youtube_pattern, link):
            youtube_link.setdefault(link, [])
            youtube_link[link].append(page_location)
        # Matches library media from lib_media_urls
        elif any(match_str in link for match_str in lib_media_urls):
            add_entry(library_media, link, 'Manually Check for Captions', page_location)
        # Matches New RCE Canvas Embedded Videos - Add to list for processing
        elif 'media_objects_iframe' in link:
            iframe_media_links.append(link)

    # --- Process Canvas Media Links Concurrently ---
    all_media_links = list(set(media_object_links + iframe_media_links))
    
    if all_media_links:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(check_media_object, all_media_links)
        
        for link, message in results:
            add_entry(media_link, link, message, page_location)

    # Process Videos
    for video in soup.find_all('video'):
        if video.get('data-media_comment_id'):
            m_link = 'Video Media Comment {}'.format(video.get('data-media_comment_id'))
            for media_comment in video.get('class'):
                track = soup.find_all('track')
                if track:
                    add_entry(media_link, m_link, 'Captions', page_location)
                else:
                    add_entry(media_link, m_link, 'No Captions', page_location)

    # Process Canvas Embedded Video
    for embed in soup.find_all('source'):
        media_type = embed.get('type')
        if media_type == 'video/mp4':
            media_url = embed.get('src')
            m_link = 'Embedded Canvas Video {}'.format(media_url)
            add_entry(media_link, m_link, 'Manually Check for Captions', page_location)

    # Process Audio
    for audio in soup.find_all('audio'):
        if audio.get('data-media_comment_id'):
            m_link = 'Audio Media Comment {}'.format(audio.get('data-media_comment_id'))
            for media_comment in audio.get('class'):
                track = soup.find_all('track')
                if track:
                    add_entry(media_link, m_link, 'Captions', page_location)
                else:
                    add_entry(media_link, m_link, 'No Captions', page_location)
        else:
            m_link = 'Embedded Canvas Audio {}'.format(audio.get('src'))
            add_entry(media_link, m_link, 'Manually Check for Captions', page_location)


# ==================== YOUTUBE FUNCTIONS ====================

YOUTUBE_VIDEO_LENGTH_REGEX = re.compile(r"[0-9]+[HMS]{1}")
YOUTUBE_TIMESTAMP_POSITION_MAP = {
    "H": 0,  # Index 0 for Hour
    "M": 1,  # Index 1 for Minute
    "S": 2,  # Index 2 for Second
}

def get_youtube_video_length(duration_str):
    """
    Parses an ISO 8601 duration string and returns (hour, minute, second).
    
    :param duration_str: The ISO 8601 duration string (e.g., "PT1H5M10S").
    :type duration_str: str
    :return: A tuple of strings (hour, minute, second).
    :rtype: tuple
    """
    duration = ['0', '0', '0']  # H, M, S
    timestamps = YOUTUBE_VIDEO_LENGTH_REGEX.findall(duration_str)
    for timestamp in timestamps:
        time_val = timestamp[:-1]
        time_unit = timestamp[-1]
        if time_unit in YOUTUBE_TIMESTAMP_POSITION_MAP:
            duration[YOUTUBE_TIMESTAMP_POSITION_MAP[time_unit]] = time_val
    return tuple(duration)


def check_youtube_video(task_data):
    """
    Worker function to check a single YouTube video for captions and duration.
    
    :param task_data: A tuple containing (key, video_id, page_locations)
    :type task_data: tuple
    :return: A tuple containing (key, status, (h, m, s), page_locations)
    :rtype: tuple
    """
    key, video_id, page_locations = task_data
    
    # Handle Playlists separately
    if not video_id:
        return (key, 'this is a playlist, check individual videos', ('', '', ''), page_locations)
        
    try:
        # Call 1: Get Duration
        r_duration = requests.get('{}?part=contentDetails&id={}&key={}'.format(
            google_video, video_id, youtube_key
        ))
        duration_data = r_duration.json()
        duration_str = duration_data['items'][0]['contentDetails']['duration']
        h, m, s = get_youtube_video_length(duration_str)

        # Call 2: Get Caption Tracks
        r_captions = requests.get('{}?part=snippet&videoId={}&key={}'.format(
            google_url, video_id, youtube_key
        ))
        caption_data = r_captions.json()

        status = 'No Captions'  # Default
        
        if caption_data['items']:
            is_ASR = False
            is_standard = False
            is_english = False
            
            for e in caption_data['items']:
                lang = e['snippet']['language']
                if lang == 'en' or lang == 'en-US':
                    is_english = True
                    if e['snippet']['trackKind'] == 'standard':
                        is_standard = True
                    elif e['snippet']['trackKind'] == 'asr' or e['snippet']['trackKind'] == 'ASR':
                        is_ASR = True

            if is_standard:
                status = 'Captions found in English'
            elif is_ASR:
                status = 'Automatic Captions in English'
            elif not is_english:
                status = 'No Captions in English'
        
        return (key, status, (h, m, s), page_locations)

    except Exception:
        return (key, 'Unable to Check Youtube Video', ('', '', ''), page_locations)


# ==================== MAIN SCRIPT ====================

if __name__ == '__main__':
    course_id = input('Enter Canvas ID: ')
    canvas = Canvas(api_url, api_key)
    course = canvas.get_course(course_id)
    print('Checking ' + course.name)
    writer = csv.writer(open('{}.csv'.format(course.name), 'w'))

    youtube_link = {}
    vimeo_link = {}
    media_link = {}
    link_media = {}
    library_media = {}

    # Check Pages
    print('Checking Pages')
    pages = course.get_pages()
    for page in pages:
        page_body = course.get_page(page.url)
        page_location = page.html_url
        if not page_body.body:
            continue
        contents = page_body.body.encode('utf-8')
        soup = BeautifulSoup(contents, 'html.parser')
        process_contents(
            soup, course, page_location,
            youtube_link, vimeo_link, media_link, link_media, library_media
        )

    # Check Assignments
    print('Checking Assignments')
    assign = course.get_assignments()
    for item in assign:
        if not item.description:
            continue
        assign_location = item.html_url
        contents = item.description.encode('utf-8')
        soup = BeautifulSoup(contents, 'html.parser')
        process_contents(
            soup, course, assign_location,
            youtube_link, vimeo_link, media_link, link_media, library_media
        )

    # Check Discussions
    print('Checking Discussions')
    discuss = course.get_discussion_topics()
    for item in discuss:
        if not item.message:
            continue
        discuss_location = item.html_url
        contents = item.message.encode('utf-8')
        soup = BeautifulSoup(contents, 'html.parser')
        process_contents(
            soup, course, discuss_location,
            youtube_link, vimeo_link, media_link, link_media, library_media
        )

    # Check Syllabus
    print('Checking Syllabus')
    syllabus = canvas.get_course(course_id, include='syllabus_body')
    syllabus_location = '{}/{}/assignments/syllabus'.format(courses_url, course_id)
    try:
        contents = syllabus.syllabus_body
        soup = BeautifulSoup(contents, 'html.parser')
        process_contents(
            soup, course, syllabus_location,
            youtube_link, vimeo_link, media_link, link_media, library_media
        )
    except Exception:
        pass

    # Check Modules
    print('Checking Modules')
    modules = course.get_modules()
    for module in modules:
        items = module.get_module_items(include='content_details')
        for item in items:
            youtube_embed = []
            library_embed = []
            if item.type == 'ExternalUrl':
                module_url = '{}/{}/modules/items/{}'.format(courses_url, course_id, item.id)
                href = item.external_url
                if re.search(youtube_pattern, href):
                    youtube_embed.append(href)
                if any(match_str in href for match_str in lib_media_urls):
                    library_embed.append(href)

            for y_link in youtube_embed:
                youtube_link.setdefault(y_link, [])
                youtube_link[y_link].append(module_url)
            for link in library_embed:
                add_entry(library_media, link, 'Manually Check for Captions', module_url)
            
            if item.type == 'File':
                try:
                    module_location = item.html_url
                    file_id = item.content_id
                    get_file = course.get_file(file_id)
                    file_location = get_file.url.split('?')[0]

                    if 'audio' in get_file.mime_class:
                        link_name = 'Linked Audio File: {}'.format(get_file.display_name)
                        add_entry(
                            link_media, link_name, 'Manually Check for Captions',
                            module_location, file_location=file_location
                        )
                    if 'video' in get_file.mime_class:
                        link_name = 'Linked Video File: {}'.format(get_file.display_name)
                        add_entry(
                            link_media, link_name, 'Manually Check for Captions',
                            module_location, file_location=file_location
                        )
                except Exception:
                    pass

    # Check Announcements
    print('Checking Announcements')
    announce = course.get_discussion_topics(only_announcements=True)
    for item in announce:
        announce_location = item.html_url
        if not item.message:
            continue
        contents = item.message.encode('utf-8')
        soup = BeautifulSoup(contents, 'html.parser')
        process_contents(
            soup, course, announce_location,
            youtube_link, vimeo_link, media_link, link_media, library_media
        )

    # Check YouTube Captions
    print('Checking YouTube Captions')
    tasks = []
    processed_youtube_links = {}

    for key, page_locations in youtube_link.items():
        if 'list' in key:
            processed_youtube_links[key] = ['this is a playlist, check individual videos', '', '', ''] + page_locations
            continue
        
        video_id_match = re.findall(youtube_pattern, key, re.MULTILINE | re.IGNORECASE)
        video_id = video_id_match[0] if video_id_match else None
        
        if video_id:
            tasks.append((key, video_id, page_locations))
        else:
            processed_youtube_links[key] = ['Unable to parse Video ID', '', '', ''] + page_locations

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(check_youtube_video, tasks)

    for result in results:
        key, status, (h, m, s), page_locations = result
        processed_youtube_links[key] = [status, h, m, s] + page_locations

    youtube_link = processed_youtube_links

    # Write CSV
    writer.writerow([
        'Media', 'Caption Status', 'Hour', 'Minute', 'Second', 'Page Location',
        'File Location'
    ])
    for key, value in youtube_link.items():
        writer.writerow([key] + value)
    for key, value in media_link.items():
        writer.writerow([key] + value)
    for key, value in link_media.items():
        writer.writerow([key] + value)
    for key, value in library_media.items():
        writer.writerow([key] + value)

    print('Complete! CSV file generated: {}.csv'.format(course.name))

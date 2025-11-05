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
import subprocess
import json
import tempfile
import os

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

def _add_accessibility_entry(d, page, issue_type, message, code="", selector=""):
    """Add accessibility issue entry following the same format as media entries"""
    name = f"A11Y: {issue_type} - {page}"
    status = f"{message} (Code: {code})" if code else message
    d[name] = [status, "", "", "", page, selector]

def _check_pa11y_installed():
    """Check if pa11y is installed and install if necessary"""
    try:
        subprocess.run(['pa11y', '--version'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Pa11y not found. Installing...")
        try:
            subprocess.run(['npm', 'install', '-g', 'pa11y'], check=True)
            return True
        except subprocess.CalledProcessError:
            print("Failed to install pa11y. Please install Node.js and npm first.")
            return False

def _run_pa11y_on_html(html_content, page_name):
    """Run pa11y accessibility testing on HTML content"""
    if not _check_pa11y_installed():
        return []
    
    # Create temporary HTML file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as temp_file:
        temp_file.write(html_content)
        temp_file_path = temp_file.name
    
    try:
        # Run pa11y with WCAG 2.1 AA standard and JSON reporter
        result = subprocess.run([
            'pa11y',
            '--standard', 'WCAG2AA',
            '--reporter', 'json',
            '--timeout', '30000',
            '--wait', '2000',
            f'file://{temp_file_path}'
        ], capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            # Parse JSON output
            try:
                issues = json.loads(result.stdout)
                return issues
            except json.JSONDecodeError:
                print(f"Failed to parse pa11y JSON output for {page_name}")
                return []
        else:
            print(f"Pa11y failed for {page_name}: {result.stderr}")
            return []
    
    except subprocess.TimeoutExpired:
        print(f"Pa11y timeout for {page_name}")
        return []
    except Exception as e:
        print(f"Error running pa11y for {page_name}: {str(e)}")
        return []
    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_file_path)
        except OSError:
            pass

def _process_accessibility_issues(issues, page_name, accessibility_dict):
    """Process pa11y issues and add them to the accessibility dictionary"""
    if not issues:
        _add_accessibility_entry(accessibility_dict, page_name, "PASS", "No accessibility issues found")
        return
    
    # Group issues by type for better reporting
    issue_counts = {}
    for issue in issues:
        issue_type = issue.get('type', 'unknown')
        issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1
        
        # Add individual issue entry
        message = issue.get('message', 'Unknown accessibility issue')
        code = issue.get('code', '')
        selector = issue.get('selector', '')
        
        _add_accessibility_entry(
            accessibility_dict, 
            page_name, 
            issue_type.upper(), 
            message, 
            code, 
            selector
        )
    
    # Add summary entry
    summary = ", ".join([f"{count} {type_name}" for type_name, count in issue_counts.items()])
    _add_accessibility_entry(accessibility_dict, page_name, "SUMMARY", f"Total issues: {summary}")

def _check_media_object(url: str):
    try:
        txt = requests.get(url, headers=_auth_header(CANVAS_API_KEY)).text
        if '"kind":"subtitles"' in txt:
            return (url, "Captions in English" if '"locale":"en"' in txt else "No English Captions")
        return (url, "No Captions")
    except requests.RequestException:
        return (url, "Unable to Check Media Object")

def _process_html(soup, course, page, yt_links, media_links, link_media, lib_media, accessibility_dict, html_content):
    """Enhanced version that includes accessibility testing"""
    media_objs, iframe_objs = [], []

    # Run accessibility testing on the HTML content
    issues = _run_pa11y_on_html(html_content, page)
    _process_accessibility_issues(issues, page, accessibility_dict)

    # Original media processing code continues...
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

def _check_yt_captions(video_id: str):
    try:
        params = {"part": "snippet", "videoId": video_id, "key": YOUTUBE_API_KEY}
        response = requests.get(YT_CAPTION_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data["items"]:
            for item in data["items"]:
                if item["snippet"]["language"] == "en":
                    return "Captions in English"
            return "No English Captions"
        return "No Captions"
    except Exception:
        return "Unable to Check YouTube Captions"

def _get_yt_duration(video_id: str):
    try:
        params = {"part": "contentDetails", "id": video_id, "key": YOUTUBE_API_KEY}
        response = requests.get(YT_VIDEO_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data["items"]:
            duration = data["items"][0]["contentDetails"]["duration"]
            match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
            if match:
                hours = int(match.group(1) or 0)
                minutes = int(match.group(2) or 0)
                seconds = int(match.group(3) or 0)
                return hours, minutes, seconds
    except Exception:
        pass
    return "", "", ""

def _process_yt_links(yt_links, yt_media):
    for link, pages in yt_links.items():
        match = re.search(YT_PATTERN, link)
        if match:
            video_id = match.group(1)
            status = _check_yt_captions(video_id)
            hours, minutes, seconds = _get_yt_duration(video_id)
            page_list = ", ".join(pages)
            _add_entry(yt_media, link, status, page_list, hours, minutes, seconds)

def analyze_course(course_id: int):
    """Main function to analyze a course for media and accessibility"""
    canvas = Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    course = canvas.get_course(course_id)
    
    # Initialize dictionaries for different types of content
    yt_links = {}
    media_links = {}
    link_media = {}
    lib_media = {}
    accessibility_dict = {}  # New dictionary for accessibility issues
    
    print(f"Analyzing course: {course.name}")
    
    # Get all pages
    pages = list(course.get_pages())
    
    for page in pages:
        try:
            page_content = page.show_latest_revision()
            if hasattr(page_content, 'body') and page_content.body:
                soup = BeautifulSoup(page_content.body, 'html.parser')
                _process_html(soup, course, page.title, yt_links, media_links, 
                            link_media, lib_media, accessibility_dict, page_content.body)
        except Exception as e:
            print(f"Error processing page {page.title}: {str(e)}")
    
    # Process modules and their items
    modules = list(course.get_modules())
    for module in modules:
        try:
            items = list(module.get_module_items())
            for item in items:
                if item.type == 'Page':
                    try:
                        page = course.get_page(item.page_url)
                        page_content = page.show_latest_revision()
                        if hasattr(page_content, 'body') and page_content.body:
                            soup = BeautifulSoup(page_content.body, 'html.parser')
                            _process_html(soup, course, f"{module.name} - {item.title}", 
                                        yt_links, media_links, link_media, lib_media, 
                                        accessibility_dict, page_content.body)
                    except Exception as e:
                        print(f"Error processing module item {item.title}: {str(e)}")
        except Exception as e:
            print(f"Error processing module {module.name}: {str(e)}")
    
    # Process YouTube links
    yt_media = {}
    _process_yt_links(yt_links, yt_media)
    
    # Create comprehensive report
    return {
        'youtube_media': yt_media,
        'canvas_media': media_links,
        'linked_media': link_media,
        'library_media': lib_media,
        'accessibility_issues': accessibility_dict  # New accessibility section
    }

def create_vast_report(course_analysis, course_name):
    """Create VAST report including accessibility data"""
    all_data = []
    
    # Add YouTube media
    for name, data in course_analysis['youtube_media'].items():
        all_data.append({
            'Type': 'YouTube Video',
            'Name/URL': name,
            'Status': data[0],
            'Hours': data[1],
            'Minutes': data[2],
            'Seconds': data[3],
            'Page': data[4],
            'File Location': data[5] if len(data) > 5 else ""
        })
    
    # Add Canvas media
    for name, data in course_analysis['canvas_media'].items():
        all_data.append({
            'Type': 'Canvas Media',
            'Name/URL': name,
            'Status': data[0],
            'Hours': data[1],
            'Minutes': data[2],
            'Seconds': data[3],
            'Page': data[4],
            'File Location': data[5] if len(data) > 5 else ""
        })
    
    # Add linked media
    for name, data in course_analysis['linked_media'].items():
        all_data.append({
            'Type': 'Linked Media',
            'Name/URL': name,
            'Status': data[0],
            'Hours': data[1],
            'Minutes': data[2],
            'Seconds': data[3],
            'Page': data[4],
            'File Location': data[5] if len(data) > 5 else ""
        })
    
    # Add library media
    for name, data in course_analysis['library_media'].items():
        all_data.append({
            'Type': 'Library Media',
            'Name/URL': name,
            'Status': data[0],
            'Hours': data[1],
            'Minutes': data[2],
            'Seconds': data[3],
            'Page': data[4],
            'File Location': data[5] if len(data) > 5 else ""
        })
    
    # Add accessibility issues
    for name, data in course_analysis['accessibility_issues'].items():
        all_data.append({
            'Type': 'Accessibility (WCAG 2.1 AA)',
            'Name/URL': name,
            'Status': data[0],
            'Hours': data[1],
            'Minutes': data[2],
            'Seconds': data[3],
            'Page': data[4],
            'File Location': data[5] if len(data) > 5 else ""
        })
    
    # Create DataFrame
    df = pd.DataFrame(all_data)
    
    # Add summary statistics
    summary_stats = {
        'Total YouTube Videos': len(course_analysis['youtube_media']),
        'Total Canvas Media': len(course_analysis['canvas_media']),
        'Total Linked Media': len(course_analysis['linked_media']),
        'Total Library Media': len(course_analysis['library_media']),
        'Total Accessibility Issues': len(course_analysis['accessibility_issues'])
    }
    
    return df, summary_stats

def upload_to_google_sheets(df, sheet_name, worksheet_name="VAST Report"):
    """Upload the report to Google Sheets"""
    try:
        gc = gspread.service_account()
        sh = gc.open(sheet_name)
        
        try:
            worksheet = sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=worksheet_name, rows="1000", cols="20")
        
        set_with_dataframe(worksheet, df)
        print(f"Report uploaded to Google Sheets: {sheet_name}")
        
    except Exception as e:
        print(f"Error uploading to Google Sheets: {str(e)}")

def run_caption_report(course_input):
    """
    Main function that runs the complete VAST report with accessibility testing
    Args:
        course_input: Canvas course URL or course ID
    Returns:
        Dictionary containing the analysis results
    """
    try:
        # Extract course ID from URL if needed
        if isinstance(course_input, str) and "courses/" in course_input:
            course_id = int(course_input.split("courses/")[-1].split("/")[0])
        else:
            course_id = int(course_input)
        
        print(f"🎯 Analyzing Course ID: {course_id}")
        
        # Run the analysis
        analysis = analyze_course(course_id)
        
        # Create the report
        course_name = f"Course_{course_id}"
        df, stats = create_vast_report(analysis, course_name)
        
        # Print summary
        print("\n📊 === VAST Report Summary ===")
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        # Save to CSV
        csv_filename = f"{course_name}_vast_report.csv"
        df.to_csv(csv_filename, index=False)
        print(f"\n💾 Report saved as {csv_filename}")
        
        # Display first few rows
        print(f"\n📋 Preview of report (first 5 rows):")
        print(df.head().to_string())
        
        return {
            'analysis': analysis,
            'dataframe': df,
            'summary': stats,
            'csv_file': csv_filename
        }
        
    except Exception as e:
        print(f"❌ Error in run_caption_report: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
# Example usage
if __name__ == "__main__":
    # Replace with your course ID
    COURSE_ID = 12345
    
    # Analyze the course
    print("Starting course analysis...")
    analysis = analyze_course(COURSE_ID)
    
    # Create report
    course_name = f"Course_{COURSE_ID}"
    df, stats = create_vast_report(analysis, course_name)
    
    # Print summary
    print("\n=== VAST Report Summary ===")
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    # Save to CSV
    df.to_csv(f"{course_name}_vast_report.csv", index=False)
    print(f"\nReport saved as {course_name}_vast_report.csv")
    
    # Optionally upload to Google Sheets
    # upload_to_google_sheets(df, "VAST Reports", f"{course_name} Report")


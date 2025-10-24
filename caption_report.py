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
    # Combine results into a DataFrame
    # --------------------------------------------------------------
    print("\n📊 Compiling results …")
    rows = []
    for container in (yt_links, media_links, link_media, lib_media):
        for key, vals in container.items():
            rows.append([key] + vals)

    df = pd.DataFrame(rows, columns=[
        "Media", "Caption Status", "Hour", "Minute", "Second", "Page Location", "File Location"
    ])

    # --------------------------------------------------------------
    # Create or replace Google Sheet
    # --------------------------------------------------------------
    print("\n📄 Creating or updating Google Sheet …")
    sheet_title = f"{course.name} Caption Report"

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

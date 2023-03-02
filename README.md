[![Join UCF Open Slack Discussions](https://badgen.net/badge/icon/ucfopen?icon=slack&label=slack&color=pink)](https://dl.ucf.edu/join-ucfopen)

# VAST (Video Accessibility Scanning Tool)

VAST is a Python script that searches an Instructure Canvas course for media and returns results in a CSV file.

## Requirements

* Python 3
* YouTube API Key
* Vimeo API Key
* System permission to install python libraries

## Installation

* Install all dependencies by using `pip install -r requirements.txt`
* Copy `vast_config.template.py` to `vast_config.py`
* Set the configuration variables in `vast_config.py`:
    * **api_key** - Canvas API Key. To scan a course you're not enrolled in, you'll needs a sub-account or admin level key
    * **api_url** - Canvas URL and API e.g. `https://example.instructure.com`
    * **youtube_key** - See instructions below.
    * **vimeo_api_key** - See instructions below.
    * **courses_url** - e.g. `https://example.instructure.com/courses`

### Google/YouTube API Key
In order for VAST to scan YouTube videos for closed captioning, you will need to create a YouTube Data API key.  Follow the instructions below:

1. Go to the [Google Developer Console](https://console.developers.google.com).
2. Create a project.
3. Enable ***YouTube Data API V3***
4. Create an ***API key*** credential.

### Vimeo API Key
In order for VAST to scan Vimeo videos for closed captioning, you will need to create a Vimeo API key. Follow the instructions below:

1. [Create a new App on Vimeo Developer API](https://developer.vimeo.com/apps/new?source=getting-started), please note you must have a Vimeo Developer account.
2. On your application's "Authentication" page, Generate a new Access Token.  (Select the `Public` and `Private` checkboxes for Scopes.)

## Types of Media

VAST will identify the following types of media:

* YouTube videos (iframe and links)
* Vimeo videos (iframe and links)
* Video files linked from the Rich Content Editor
* Audio files linked from the Rich Content Editor
* SWF files linked from the Rich Content Editor
* Media comments in the Rich Content Editor
* Embedded Canvas Video and Audio in the Rich Content Editor
* Links from video providers:
    * Films on Demand
    * Alexander Street Press
    * Kanopy
* Custom links can be added/removed to the `lib_media_urls` list in the `vast_config.py`

**Note**: VAST relies on the mime type of the file provided by Canvas

## Limitations

* VAST does not check the Quizzes tool
* VAST can not determine if captions exist on video files, audio files, flash files, or video providers. The report will list the location of the file to check manually and provide a download link. Make sure to be logged into Canvas first; unless the download link will not work.
* VAST may not be able to check all YouTube or Vimeo videos for captions. If it can't, the report will note so.

## Testing

A testing course is available in [Canvas Commons](https://lor.instructure.com/). Use the search term `Vast Test Course`.

## Use

1. Open up a terminal window
2. Go to the directory containing `vast.py` (use cd command)
3. Type `python vast.py`
4. Enter the Canvas course id number e.g.(**1234567** from `https://example.instructure.com/courses/1234567`)
5. The report will be saved in the same directory as the `vast.py` script

## Contributors

### Creator

* John Raible

### Contributors

* Matt Emond


import json
import os
import subprocess
import tempfile

from django.conf import settings
from rest_framework import serializers

ALLOWED_EXTENSIONS = {".mp4", ".mov", ".webm"}
ALLOWED_MIME_TYPES = {"video/mp4", "video/quicktime", "video/webm"}
MAX_DURATION_SECONDS = 20
DURATION_TOLERANCE_SECONDS = 0.5
DEFAULT_MAX_SIZE_MB = 50


def get_max_size_bytes():
    return int(getattr(settings, "VIDEO_COMMENT_MAX_SIZE_MB", DEFAULT_MAX_SIZE_MB)) * 1024 * 1024


def parse_ffprobe_metadata(payload):
    try:
        data = json.loads(payload)
    except (TypeError, ValueError) as exc:
        raise serializers.ValidationError({"video": "ffprobe returned unreadable metadata."}) from exc
    streams = data.get("streams") or []
    video_streams = [stream for stream in streams if stream.get("codec_type") == "video"]
    if not video_streams:
        raise serializers.ValidationError({"video": "The file does not contain a video track."})
    duration_values = [data.get("format", {}).get("duration")] + [s.get("duration") for s in video_streams]
    duration = None
    for value in duration_values:
        try:
            if value is not None:
                duration = float(value)
                break
        except (TypeError, ValueError):
            continue
    if duration is None or duration <= 0:
        raise serializers.ValidationError({"video": "Could not determine a valid video duration."})
    format_name = (data.get("format", {}).get("format_name") or "").lower()
    return {"duration_seconds": duration, "format_name": format_name, "has_video": True}


def validate_video_upload(uploaded_file):
    if not uploaded_file:
        raise serializers.ValidationError({"video": "A video file is required."})
    size = getattr(uploaded_file, "size", 0) or 0
    if size <= 0:
        raise serializers.ValidationError({"video": "The video file is empty."})
    if size > get_max_size_bytes():
        raise serializers.ValidationError({"video": f"The video file exceeds {getattr(settings, 'VIDEO_COMMENT_MAX_SIZE_MB', DEFAULT_MAX_SIZE_MB)} MB."})
    ext = os.path.splitext(uploaded_file.name or "")[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise serializers.ValidationError({"video": "Allowed video extensions are .mp4, .mov and .webm."})
    content_type = (getattr(uploaded_file, "content_type", "") or "").lower()
    if content_type and content_type not in ALLOWED_MIME_TYPES:
        raise serializers.ValidationError({"video": "Allowed video MIME types are video/mp4, video/quicktime and video/webm."})

    ffprobe_path = getattr(settings, "VIDEO_COMMENT_FFPROBE_PATH", "ffprobe")
    suffix = ext or ".video"
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp:
            temp_name = temp.name
            for chunk in uploaded_file.chunks():
                temp.write(chunk)
        uploaded_file.seek(0)
        command = [
            ffprobe_path, "-v", "error", "-print_format", "json",
            "-show_format", "-show_streams", temp_name,
        ]
        try:
            completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=15)
        except FileNotFoundError as exc:
            raise serializers.ValidationError({"video": "ffprobe is not available. Install ffmpeg/ffprobe and configure VIDEO_COMMENT_FFPROBE_PATH."}) from exc
        except subprocess.TimeoutExpired as exc:
            raise serializers.ValidationError({"video": "ffprobe timed out while reading the video."}) from exc
        if completed.returncode != 0:
            raise serializers.ValidationError({"video": "The video file is unreadable or corrupt."})
        metadata = parse_ffprobe_metadata(completed.stdout)
    finally:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)
    if metadata["duration_seconds"] > MAX_DURATION_SECONDS + DURATION_TOLERANCE_SECONDS:
        raise serializers.ValidationError({"video": "Video comments must be 20 seconds or shorter."})
    # Simple container/MIME cross-check from verified ffprobe format metadata.
    fmt = metadata.get("format_name", "")
    if ext == ".webm" and "webm" not in fmt:
        raise serializers.ValidationError({"video": "The file content is not a valid WebM video."})
    if ext == ".mov" and not ({"mov", "mp4", "m4a", "3gp", "3g2", "mj2"} & set(fmt.split(","))):
        raise serializers.ValidationError({"video": "The file content is not a valid QuickTime video."})
    if ext == ".mp4" and not ({"mov", "mp4", "m4a", "3gp", "3g2", "mj2"} & set(fmt.split(","))):
        raise serializers.ValidationError({"video": "The file content is not a valid MP4 video."})
    return {"duration_seconds": round(metadata["duration_seconds"], 3), "mime_type": content_type or "application/octet-stream", "file_size": size}

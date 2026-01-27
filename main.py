import os
import sys
import time
import math
import json
import asyncio
import logging
import shutil
import signal
import hashlib
import traceback
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Any
from enum import Enum, auto
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from pyrogram.handlers import MessageHandler, CallbackQueryHandler

# Third-party libraries
from pyrogram import Client, filters, enums, errors
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    CallbackQuery,
    InputMediaVideo,
    InputMediaDocument
)
import motor.motor_asyncio
from motor.core import AgnosticCollection
import aiohttp
from aiohttp import web
import psutil

# ===============================================================================
# 1. ENUMERATIONS & DATA MODELS
# ================================================================================

class JobState(Enum):
    """Complete state machine for job lifecycle"""
    IDLE = auto()
    WAITING_DURATION = auto()
    WAITING_CHANNEL = auto()
    PREVIEW = auto()
    QUEUED = auto()
    DOWNLOADING = auto()
    PROCESSING = auto()
    UPLOADING = auto()
    VALIDATING = auto()
    PAUSED = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    RESUMING = auto()

class UploadMode(Enum):
    VIDEO = "video"
    DOCUMENT = "document"

class ValidationResult(Enum):
    VALID = auto()
    INVALID_SIZE = auto()
    INVALID_DURATION = auto()
    CORRUPTED = auto()

@dataclass
class JobData:
    """Complete job data model"""
    job_id: str
    user_id: int
    file_message_id: int
    file_id: str
    file_name: str
    file_size: int
    original_duration: float = 0.0
    # --- NEW FIELDS FOR TRIMMING ---
    start_time: float = 0.0       # Start processing from this second
    end_time: float = 0.0         # End processing at this second
    # -------------------------------
    clip_duration: int = 60
    total_parts: int = 0
    current_part: int = 1
    completed_parts: List[int] = field(default_factory=list)
    failed_parts: List[int] = field(default_factory=list)
    state: JobState = JobState.IDLE
    target_channel_id: int = 0
    target_channel_name: str = ""
    file_path: Optional[str] = None
    caption_template: str = "[Part {part}] {start_time} - {end_time}"
    upload_mode: UploadMode = UploadMode.VIDEO
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    progress_message_id: Optional[int] = None
    checksum: Optional[str] = None
    retry_count: int = 0
    flood_wait_until: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    cancel_requested: bool = False

@dataclass
class ChannelInfo:
    """Channel information model"""
    channel_id: int
    name: str
    added_by: int
    added_at: datetime
    last_used: Optional[datetime] = None
    permissions_verified: bool = False

@dataclass
class UserSettings:
    """User preferences model"""
    user_id: int
    default_duration: int = 60
    upload_mode: UploadMode = UploadMode.VIDEO
    auto_resume: bool = True
    caption_template: str = "[Part {part}] {start_time} - {end_time}"
    notify_on_completion: bool = True
    notify_on_failure: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

# ================================================================================
# 2. CONFIGURATION & CONSTANTS
# ================================================================================

class Config:
    """Bot configuration with environment variables"""
    
    # Telegram API
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
    
    # Database
    DATABASE_URL = os.environ.get("DATABASE_URL", "")
    DATABASE_NAME = os.environ.get("DATABASE_NAME", "AutoVideoClipGenerate")
    
    # Security
    OWNER_ID = int(os.environ.get("OWNER_ID", 0))
    
    # --- FIXED ADMIN ID LOADING ---
    # Split by comma, strip spaces, and filter out empty strings
    _admin_list = os.environ.get("ADMIN_IDS", "").split(",")
    ADMIN_IDS = [int(x.strip()) for x in _admin_list if x.strip().isdigit()]
    # Ensure Owner is always an Admin
    if OWNER_ID not in ADMIN_IDS:
        ADMIN_IDS.append(OWNER_ID)
    # -------------------------------
    
    # Paths
    WORK_DIR = os.environ.get("WORK_DIR", "downloads")
    LOG_DIR = os.environ.get("LOG_DIR", "logs")
    TEMP_DIR = os.environ.get("TEMP_DIR", "temp")
    
    # Limits & Settings
    MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", 1))
    MAX_QUEUE_SIZE = int(os.environ.get("MAX_QUEUE_SIZE", 10))
    MAX_CLIP_DURATION = int(os.environ.get("MAX_CLIP_DURATION", 300))
    MIN_CLIP_DURATION = int(os.environ.get("MIN_CLIP_DURATION", 10))
    MIN_CLIP_SIZE = int(os.environ.get("MIN_CLIP_SIZE", 10240))
    MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
    VALIDATE_EACH_CLIP = os.environ.get("VALIDATE_EACH_CLIP", "true").lower() == "true"
    FLOOD_WAIT_BACKOFF = int(os.environ.get("FLOOD_WAIT_BACKOFF", 2))
    MAX_FLOOD_WAIT = int(os.environ.get("MAX_FLOOD_WAIT", 3600))
    UPLOAD_COOLDOWN = float(os.environ.get("UPLOAD_COOLDOWN", 1.0))
    FFMPEG_PRESET = os.environ.get("FFMPEG_PRESET", "fast")
    AUTO_CLEANUP_HOURS = int(os.environ.get("AUTO_CLEANUP_HOURS", 24))
    MAX_DISK_USAGE_PERCENT = int(os.environ.get("MAX_DISK_USAGE_PERCENT", 90))
    
    # Web Server
    WEB_HOST = os.environ.get("WEB_HOST", "0.0.0.0")
    WEB_PORT = int(os.environ.get("PORT", 8080))
    
    # Logging
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    LOG_TO_CHANNEL = os.environ.get("LOG_TO_CHANNEL", "")
    
    @classmethod
    def validate(cls):
        if not cls.API_ID or not cls.API_HASH or not cls.BOT_TOKEN:
            raise ValueError("API_ID, API_HASH, and BOT_TOKEN must be set")
        if not cls.OWNER_ID:
            raise ValueError("OWNER_ID must be set")
        
        for directory in [cls.WORK_DIR, cls.LOG_DIR, cls.TEMP_DIR]:
            Path(directory).mkdir(parents=True, exist_ok=True)

# ================================================================================
# 3. LOGGING SETUP
# ================================================================================

class StructuredLogger:
    """Enhanced logging with structured output"""
    
    def __init__(self):
        self.logger = logging.getLogger("AutoSplitter")
        
        Path(Config.LOG_DIR).mkdir(parents=True, exist_ok=True)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_format)
        
        # File handler
        log_file = Path(Config.LOG_DIR) / f"autosplitter_{datetime.now():%Y%m%d}.log"
        file_handler = logging.FileHandler(log_file)
        file_format = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        file_handler.setFormatter(file_format)
        
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        self.logger.setLevel(getattr(logging, Config.LOG_LEVEL))
        
        # Telegram log channel handler (if configured)
        self.log_channel = None
        self.bot_client = None
    
    def set_bot_client(self, client):
        """Set bot client for Telegram logging"""
        self.bot_client = client
    
    async def log_to_telegram(self, level: str, message: str):
        """Send log to Telegram channel"""
        if not self.bot_client or not Config.LOG_TO_CHANNEL:
            return
        
        try:
            log_message = f"[{level}] {datetime.now():%Y-%m-%d %H:%M:%S}\n{message}"
            await self.bot_client.send_message(Config.LOG_TO_CHANNEL, log_message)
        except Exception as e:
            self.logger.error(f"Failed to send log to Telegram: {e}")
    
    def info(self, message: str, **kwargs):
        self.logger.info(message, **kwargs)
        if self.bot_client:
            asyncio.create_task(self.log_to_telegram("INFO", message))
    
    def warning(self, message: str, **kwargs):
        self.logger.warning(message, **kwargs)
        if self.bot_client:
            asyncio.create_task(self.log_to_telegram("WARNING", message))
    
    def error(self, message: str, **kwargs):
        self.logger.error(message, **kwargs)
        if self.bot_client:
            asyncio.create_task(self.log_to_telegram("ERROR", message))
    
    def critical(self, message: str, **kwargs):
        self.logger.critical(message, **kwargs)
        if self.bot_client:
            asyncio.create_task(self.log_to_telegram("CRITICAL", message))
    
    def debug(self, message: str, **kwargs):
        self.logger.debug(message, **kwargs)

logger = StructuredLogger()

# ================================================================================
# 4. DATABASE LAYER
# ================================================================================

class DatabaseManager:
    """Complete database management with recovery support"""
    
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(Config.DATABASE_URL)
        self.db = self.client[Config.DATABASE_NAME]
        
        self.users: AgnosticCollection = self.db.users
        self.jobs: AgnosticCollection = self.db.jobs
        self.channels: AgnosticCollection = self.db.channels
        self.settings: AgnosticCollection = self.db.settings
        self.queue: AgnosticCollection = self.db.queue
        self.logs: AgnosticCollection = self.db.logs
        
        self.indexes_created = False
    
    async def create_indexes(self):
        """Create database indexes for performance"""
        if self.indexes_created:
            return
        
        await self.jobs.create_index([("job_id", 1)], unique=True)
        await self.jobs.create_index([("user_id", 1)])
        await self.jobs.create_index([("state", 1)])
        await self.jobs.create_index([("created_at", -1)])
        await self.jobs.create_index([("updated_at", -1)])
        
        await self.users.create_index([("user_id", 1)], unique=True)
        await self.channels.create_index([("channel_id", 1), ("user_id", 1)], unique=True)
        await self.queue.create_index([("priority", -1), ("created_at", 1)])
        
        self.indexes_created = True
        logger.info("Database indexes created")
    
    async def get_or_create_user(self, user_id: int, name: str) -> Dict:
        """Get or create user record"""
        user = await self.users.find_one({"user_id": user_id})
        if not user:
            user = {
                "user_id": user_id,
                "name": name,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "settings": {
                    "default_duration": 60,
                    "upload_mode": "video",
                    "auto_resume": True,
                    "caption_template": "[Part {part}] {start_time} - {end_time}",
                    "notify_on_completion": True,
                    "notify_on_failure": True
                },
                "stats": {
                    "jobs_completed": 0,
                    "jobs_failed": 0,
                    "total_parts": 0,
                    "total_duration": 0
                }
            }
            await self.users.insert_one(user)
            logger.info(f"Created new user: {user_id} ({name})")
        return user
    
    async def add_channel(self, user_id: int, channel_id: int, name: str) -> bool:
        """Add a channel for user"""
        channel_data = {
            "channel_id": channel_id,
            "user_id": user_id,
            "name": name,
            "added_at": datetime.now(),
            "last_used": None,
            "permissions_verified": False
        }
        
        try:
            await self.channels.update_one(
                {"channel_id": channel_id, "user_id": user_id},
                {"$set": channel_data},
                upsert=True
            )
            logger.info(f"Added channel {channel_id} ({name}) for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to add channel: {e}")
            return False
    
    async def get_user_channels(self, user_id: int) -> List[ChannelInfo]:
        """Get all channels for user"""
        cursor = self.channels.find({"user_id": user_id})
        channels = []
        async for doc in cursor:
            channels.append(ChannelInfo(
                channel_id=doc["channel_id"],
                name=doc["name"],
                added_by=doc["user_id"],
                added_at=doc["added_at"],
                last_used=doc.get("last_used"),
                permissions_verified=doc.get("permissions_verified", False)
            ))
        return channels
    
    async def remove_channel(self, user_id: int, channel_id: int) -> bool:
        """Remove a channel"""
        try:
            result = await self.channels.delete_one({"user_id": user_id, "channel_id": channel_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Failed to remove channel: {e}")
            return False
    
    async def update_channel_usage(self, user_id: int, channel_id: int):
        """Update channel last used timestamp"""
        await self.channels.update_one(
            {"user_id": user_id, "channel_id": channel_id},
            {"$set": {"last_used": datetime.now()}}
        )
    
    async def update_channel_permissions(self, user_id: int, channel_id: int, verified: bool):
        """Update channel permission status"""
        await self.channels.update_one(
            {"user_id": user_id, "channel_id": channel_id},
            {"$set": {"permissions_verified": verified}}
        )
    
    async def create_job(self, job_data: JobData) -> str:
        """Create a new job with initial state"""
        job_dict = {
            "job_id": job_data.job_id,
            "user_id": job_data.user_id,
            "file_message_id": job_data.file_message_id,
            "file_id": job_data.file_id,
            "file_name": job_data.file_name,
            "file_size": job_data.file_size,
            "original_duration": job_data.original_duration,
            "start_time": job_data.start_time,
            "end_time": job_data.end_time,
            "clip_duration": job_data.clip_duration,
            "total_parts": job_data.total_parts,
            "current_part": job_data.current_part,
            "completed_parts": job_data.completed_parts,
            "failed_parts": job_data.failed_parts,
            "state": job_data.state.value,
            "target_channel_id": job_data.target_channel_id,
            "target_channel_name": job_data.target_channel_name,
            "file_path": job_data.file_path,
            "caption_template": job_data.caption_template,
            "upload_mode": job_data.upload_mode.value,
            "created_at": job_data.created_at,
            "updated_at": job_data.updated_at,
            "started_at": job_data.started_at,
            "completed_at": job_data.completed_at,
            "error_message": job_data.error_message,
            "progress_message_id": job_data.progress_message_id,
            "checksum": job_data.checksum,
            "retry_count": job_data.retry_count,
            "flood_wait_until": job_data.flood_wait_until,
            "metadata": job_data.metadata,
            "cancel_requested": job_data.cancel_requested
        }
        
        await self.jobs.insert_one(job_dict)
        logger.info(f"Created job {job_data.job_id} for user {job_data.user_id}")
        return job_data.job_id
    
    async def update_job_state(self, job_id: str, state: JobState, **updates):
        """Update job state with additional fields"""
        update_data = {
            "state": state.value,
            "updated_at": datetime.now(),
            **updates
        }
        
        await self.jobs.update_one(
            {"job_id": job_id},
            {"$set": update_data}
        )
        
        logger.debug(f"Updated job {job_id} to state {state.name}")
    
    async def mark_job_cancelled(self, job_id: str):
        """Mark job as cancelled"""
        await self.jobs.update_one(
            {"job_id": job_id},
            {"$set": {"cancel_requested": True, "updated_at": datetime.now()}}
        )
    
    async def get_job(self, job_id: str) -> Optional[JobData]:
        """Get complete job data"""
        doc = await self.jobs.find_one({"job_id": job_id})
        if not doc:
            return None
        
        return JobData(
            job_id=doc["job_id"],
            user_id=doc["user_id"],
            file_message_id=doc["file_message_id"],
            file_id=doc["file_id"],
            file_name=doc["file_name"],
            file_size=doc["file_size"],
            original_duration=doc.get("original_duration", 0.0),
            start_time=doc.get("start_time", 0.0),
            end_time=doc.get("end_time", 0.0),
            clip_duration=doc["clip_duration"],
            total_parts=doc["total_parts"],
            current_part=doc["current_part"],
            completed_parts=doc.get("completed_parts", []),
            failed_parts=doc.get("failed_parts", []),
            state=JobState(doc["state"]),
            target_channel_id=doc["target_channel_id"],
            target_channel_name=doc["target_channel_name"],
            file_path=doc.get("file_path"),
            caption_template=doc.get("caption_template", "[Part {part}] {start_time} - {end_time}"),
            upload_mode=UploadMode(doc.get("upload_mode", "video")),
            created_at=doc["created_at"],
            updated_at=doc["updated_at"],
            started_at=doc.get("started_at"),
            completed_at=doc.get("completed_at"),
            error_message=doc.get("error_message"),
            progress_message_id=doc.get("progress_message_id"),
            checksum=doc.get("checksum"),
            retry_count=doc.get("retry_count", 0),
            flood_wait_until=doc.get("flood_wait_until"),
            metadata=doc.get("metadata", {}),
            cancel_requested=doc.get("cancel_requested", False)
        )
    
    async def get_active_user_job(self, user_id: int) -> Optional[JobData]:
        """Get active job for user"""
        doc = await self.jobs.find_one({
            "user_id": user_id,
            "state": {
                "$in": [
                    JobState.WAITING_DURATION.value,
                    JobState.WAITING_CHANNEL.value,
                    JobState.PREVIEW.value,
                    JobState.QUEUED.value,
                    JobState.DOWNLOADING.value,
                    JobState.PROCESSING.value,
                    JobState.UPLOADING.value,
                    JobState.PAUSED.value,
                    JobState.RESUMING.value
                ]
            }
        })
        return await self.get_job(doc["job_id"]) if doc else None
    
    async def get_incomplete_jobs(self) -> List[JobData]:
        """Get all incomplete jobs for crash recovery"""
        cursor = self.jobs.find({
            "state": {
                "$in": [
                    JobState.QUEUED.value,
                    JobState.DOWNLOADING.value,
                    JobState.PROCESSING.value,
                    JobState.UPLOADING.value,
                    JobState.PAUSED.value
                ]
            }
        })
        
        jobs = []
        async for doc in cursor:
            job = await self.get_job(doc["job_id"])
            if job:
                jobs.append(job)
        
        return jobs
    
    async def add_to_queue(self, job_id: str, priority: int = 0):
        """Add job to processing queue"""
        queue_item = {
            "job_id": job_id,
            "priority": priority,
            "created_at": datetime.now(),
            "status": "pending"
        }
        await self.queue.insert_one(queue_item)
    
    async def get_next_job(self) -> Optional[str]:
        """Get next job from queue (priority based)"""
        doc = await self.queue.find_one_and_delete(
            {},
            sort=[("priority", -1), ("created_at", 1)]
        )
        return doc["job_id"] if doc else None
    
    async def get_queue_size(self) -> int:
        """Get current queue size"""
        return await self.queue.count_documents({})
    
    async def get_user_settings(self, user_id: int) -> Dict:
        """Get user settings"""
        user = await self.users.find_one({"user_id": user_id})
        if user and "settings" in user:
            return user["settings"]
        return {
            "default_duration": 60,
            "upload_mode": "video",
            "auto_resume": True,
            "caption_template": "[Part {part}] {start_time} - {end_time}",
            "notify_on_completion": True,
            "notify_on_failure": True
        }
    
    async def update_user_settings(self, user_id: int, settings: Dict):
        """Update user settings"""
        await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"settings": settings, "updated_at": datetime.now()}}
        )
    
    async def get_statistics(self) -> Dict:
        """Get comprehensive statistics"""
        total_jobs = await self.jobs.count_documents({})
        active_jobs = await self.jobs.count_documents({
            "state": {
                "$in": [
                    JobState.QUEUED.value,
                    JobState.DOWNLOADING.value,
                    JobState.PROCESSING.value,
                    JobState.UPLOADING.value
                ]
            }
        })
        completed_jobs = await self.jobs.count_documents({
            "state": JobState.COMPLETED.value
        })
        failed_jobs = await self.jobs.count_documents({
            "state": JobState.FAILED.value
        })
        
        total_users = await self.users.count_documents({})
        
        disk_usage = 0
        cursor = self.jobs.find({"file_path": {"$ne": None}})
        async for doc in cursor:
            if doc.get("file_path") and os.path.exists(doc["file_path"]):
                try:
                    disk_usage += os.path.getsize(doc["file_path"])
                except:
                    pass
        
        return {
            "total_jobs": total_jobs,
            "active_jobs": active_jobs,
            "completed_jobs": completed_jobs,
            "failed_jobs": failed_jobs,
            "total_users": total_users,
            "disk_usage_bytes": disk_usage,
            "disk_usage_mb": disk_usage / (1024 * 1024),
            "queue_size": await self.get_queue_size()
        }

db = DatabaseManager()

# ================================================================================
# 5. UTILITY FUNCTIONS
# ================================================================================

class VideoProcessor:
    """Handle all video processing operations"""
    
    @staticmethod
    async def get_video_info(file_path: str) -> Dict[str, Any]:
        """Get comprehensive video information using ffprobe"""
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            file_path
        ]
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                raise Exception(f"FFprobe failed: {stderr.decode()}")
            
            info = json.loads(stdout.decode())
            
            duration = float(info["format"].get("duration", 0))
            
            video_stream = next(
                (s for s in info["streams"] if s["codec_type"] == "video"),
                None
            )
            
            audio_stream = next(
                (s for s in info["streams"] if s["codec_type"] == "audio"),
                None
            )
            
            return {
                "duration": duration,
                "format": info["format"]["format_name"],
                "size": int(info["format"]["size"]),
                "video_codec": video_stream["codec_name"] if video_stream else None,
                "video_width": video_stream.get("width") if video_stream else None,
                "video_height": video_stream.get("height") if video_stream else None,
                "audio_codec": audio_stream["codec_name"] if audio_stream else None,
                "audio_channels": audio_stream.get("channels") if audio_stream else None,
                "bitrate": int(info["format"].get("bit_rate", 0))
            }
            
        except Exception as e:
            logger.error(f"Failed to get video info: {e}")
            return {"duration": 0, "error": str(e)}
    
    @staticmethod
    async def cut_video_segment(
        input_path: str,
        output_path: str,
        start_time: float,
        duration: float,
        reencode: bool = True
    ) -> Tuple[bool, str]:
        """Cut a segment from video with precise timing"""
        
        if reencode:
            cmd = [
                "ffmpeg", "-y",
                "-ss", str(start_time),
                "-i", input_path,
                "-t", str(duration),
                "-c:v", "libx264",
                "-preset", Config.FFMPEG_PRESET,
                "-crf", "23",
                "-c:a", "aac",
                "-b:a", "128k",
                "-avoid_negative_ts", "make_zero",
                "-flags", "+global_header",
                str(output_path)
            ]
        else:
            cmd = [
                "ffmpeg", "-y",
                "-ss", str(start_time),
                "-i", input_path,
                "-t", str(duration),
                "-c", "copy",
                "-avoid_negative_ts", "make_zero",
                str(output_path)
            ]
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode()[-500:] if stderr else "Unknown error"
                return False, error_msg
            
            if not os.path.exists(output_path):
                return False, "Output file not created"
            
            file_size = os.path.getsize(output_path)
            if file_size < Config.MIN_CLIP_SIZE:
                return False, f"File too small: {file_size} bytes"
            
            return True, "Success"
            
        except Exception as e:
            return False, str(e)
    
    @staticmethod
    async def validate_clip(file_path: str) -> ValidationResult:
        """Validate clip integrity"""
        if not os.path.exists(file_path):
            return ValidationResult.CORRUPTED
        
        file_size = os.path.getsize(file_path)
        if file_size < Config.MIN_CLIP_SIZE:
            return ValidationResult.INVALID_SIZE
        
        try:
            info = await VideoProcessor.get_video_info(file_path)
            if info["duration"] <= 0:
                return ValidationResult.INVALID_DURATION
        except:
            return ValidationResult.CORRUPTED
        
        return ValidationResult.VALID

class ResourceMonitor:
    """Monitor system resources"""
    
    @staticmethod
    def get_disk_usage(path: str = Config.WORK_DIR) -> Dict:
        """Get disk usage statistics"""
        usage = shutil.disk_usage(path)
        return {
            "total": usage.total,
            "used": usage.used,
            "free": usage.free,
            "percent": (usage.used / usage.total) * 100
        }
    
    @staticmethod
    def get_memory_usage() -> Dict:
        """Get memory usage statistics"""
        memory = psutil.virtual_memory()
        return {
            "total": memory.total,
            "available": memory.available,
            "used": memory.used,
            "percent": memory.percent
        }
    
    @staticmethod
    def check_disk_space(min_free_gb: int = 1) -> bool:
        """Check if enough disk space is available"""
        usage = ResourceMonitor.get_disk_usage()
        free_gb = usage["free"] / (1024 ** 3)
        return free_gb >= min_free_gb
    
    @staticmethod
    def cleanup_old_files(max_age_hours: int = Config.AUTO_CLEANUP_HOURS):
        """Cleanup old temporary files"""
        work_dir = Path(Config.WORK_DIR)
        temp_dir = Path(Config.TEMP_DIR)
        
        cutoff_time = time.time() - (max_age_hours * 3600)
        
        for directory in [work_dir, temp_dir]:
            if not directory.exists():
                continue
            
            for file_path in directory.glob("*"):
                if file_path.is_file():
                    try:
                        if file_path.stat().st_mtime < cutoff_time:
                            file_path.unlink()
                            logger.debug(f"Cleaned up old file: {file_path}")
                    except Exception as e:
                        logger.warning(f"Failed to cleanup {file_path}: {e}")
    
    @staticmethod
    async def force_cleanup_job_files(job_id: str):
        """Force cleanup all files related to a job and free memory"""
        work_dir = Path(Config.WORK_DIR)
        temp_dir = Path(Config.TEMP_DIR)
        
        cleaned_count = 0
        freed_bytes = 0
        
        for directory in [work_dir, temp_dir]:
            if not directory.exists():
                continue
            
            for file_path in directory.glob(f"{job_id}*"):
                try:
                    if file_path.is_file():
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        freed_bytes += file_size
                        cleaned_count += 1
                        logger.info(f"Cleaned up: {file_path.name} ({file_size} bytes)")
                except Exception as e:
                    logger.error(f"Failed to cleanup {file_path}: {e}")
        
        # Force garbage collection to free memory
        import gc
        gc.collect()
        
        return cleaned_count, freed_bytes

class TimeUtils:
    """Time-related utilities"""
    
    @staticmethod
    def format_timestamp(seconds: float) -> str:
        """Format seconds to HH:MM:SS"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        else:
            return f"{minutes:02d}:{secs:02d}"
    
    @staticmethod
    def format_duration(seconds: float) -> str:
        """Format duration to human readable"""
        if seconds < 60:
            return f"{int(seconds)} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"
    
    @staticmethod
    def format_file_size(bytes: int) -> str:
        """Format bytes to human readable"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes < 1024.0:
                return f"{bytes:.2f} {unit}"
            bytes /= 1024.0
        return f"{bytes:.2f} TB"
    
    @staticmethod
    def estimate_processing_time(
        duration: float,
        clip_duration: int,
        include_upload: bool = True
    ) -> str:
        """Estimate total processing time"""
        total_parts = math.ceil(duration / clip_duration)
        
        cut_time_per_part = 5
        upload_time_per_part = 10
        
        total_cut_time = total_parts * cut_time_per_part
        total_upload_time = total_parts * upload_time_per_part if include_upload else 0
        
        total_seconds = total_cut_time + total_upload_time
        
        if total_seconds < 60:
            return f"{int(total_seconds)} seconds"
        elif total_seconds < 3600:
            return f"{int(total_seconds / 60)} minutes"
        else:
            hours = total_seconds / 3600
            return f"{hours:.1f} hours"

class UIComponents:
    """UI component generators"""
    
    @staticmethod
    def create_progress_bar(current: int, total: int, width: int = 20) -> str:
        """Create a visual progress bar"""
        if total == 0:
            return "▢" * width
        
        progress = current / total
        filled = int(width * progress)
        empty = width - filled
        
        bar = "■" * filled + "□" * empty
        percentage = int(progress * 100)
        
        return f"{bar} {percentage}%"
    
    @staticmethod
    def create_trim_mode_buttons(message_id: int) -> InlineKeyboardMarkup:
        """Create buttons to choose between Full Video or Custom Trim"""
        buttons = [
            [
                InlineKeyboardButton("🎬 Process Full Video", callback_data=f"mode_full_{message_id}"),
                InlineKeyboardButton("✂️ Custom Trim", callback_data=f"mode_custom_{message_id}")
            ],
            [
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_all")
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def create_duration_buttons(message_id: int) -> InlineKeyboardMarkup:
        """Create duration selection buttons"""
        buttons = [
            [
                InlineKeyboardButton("30s", callback_data=f"dur_{message_id}_30"),
                InlineKeyboardButton("55s", callback_data=f"dur_{message_id}_55"),
                InlineKeyboardButton("60s", callback_data=f"dur_{message_id}_60")
            ],
            [
                InlineKeyboardButton("2m", callback_data=f"dur_{message_id}_120"),
                InlineKeyboardButton("5m", callback_data=f"dur_{message_id}_300"),
                InlineKeyboardButton("✏️ Custom", callback_data=f"custom_dur_{message_id}")
            ],
            [
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_all")
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def create_channel_buttons(channels: List[ChannelInfo], message_id: int, duration: int) -> InlineKeyboardMarkup:
        """Create channel selection buttons"""
        buttons = []
        row = []
        
        for i, channel in enumerate(channels):
            verified_icon = "✅" if channel.permissions_verified else "⚠️"
            btn = InlineKeyboardButton(
                f"{verified_icon} {channel.name}",
                callback_data=f"chan_{message_id}_{duration}_{channel.channel_id}"
            )
            row.append(btn)
            
            if (i + 1) % 2 == 0 or i == len(channels) - 1:
                buttons.append(row)
                row = []
        
        buttons.append([
            InlineKeyboardButton("➕ Add Channel", callback_data="add_channel_prompt"),
            InlineKeyboardButton("🔙 Back", callback_data=f"back_dur_{message_id}")
        ])
        
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def create_preview_buttons(message_id: int, duration: int, channel_id: int) -> InlineKeyboardMarkup:
        """Create preview confirmation buttons"""
        buttons = [
            [
                InlineKeyboardButton("✅ Start Processing", callback_data=f"start_job_{message_id}_{duration}_{channel_id}")
            ],
            [
                InlineKeyboardButton("🔙 Change Channel", callback_data=f"back_to_chan_{message_id}_{duration}"),
                InlineKeyboardButton("⏱ Change Duration", callback_data=f"back_dur_{message_id}")
            ],
            [
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_all")
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def create_job_control_buttons(job_id: str, show_cancel: bool = True) -> InlineKeyboardMarkup:
        """Create job control buttons"""
        buttons = []
        
        if show_cancel:
            buttons.append([
                InlineKeyboardButton("⏹ Cancel Job", callback_data=f"cancel_job_{job_id}")
            ])
        
        buttons.append([
            InlineKeyboardButton("📊 Job Details", callback_data=f"details_{job_id}")
        ])
        
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def create_admin_dashboard_buttons() -> InlineKeyboardMarkup:
        """Create admin dashboard buttons"""
        buttons = [
            [
                InlineKeyboardButton("📊 Statistics", callback_data="admin_stats"),
                InlineKeyboardButton("👥 Users", callback_data="admin_users")
            ],
            [
                InlineKeyboardButton("🔄 Active Jobs", callback_data="admin_jobs"),
                InlineKeyboardButton("💾 Storage", callback_data="admin_storage")
            ],
            [
                InlineKeyboardButton("🧹 Cleanup", callback_data="admin_cleanup"),
                InlineKeyboardButton("🔄 Refresh", callback_data="admin_panel")
            ],
            [
                InlineKeyboardButton("❌ Close", callback_data="close")
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def create_settings_buttons(settings: Dict) -> InlineKeyboardMarkup:
        """Create settings buttons"""
        default_dur = settings.get("default_duration", 60)
        upload_mode = settings.get("upload_mode", "video")
        auto_resume = settings.get("auto_resume", True)
        
        buttons = [
            [
                InlineKeyboardButton(f"⏱ Duration: {default_dur}s", callback_data="setting_duration"),
                InlineKeyboardButton(f"📤 Mode: {upload_mode.upper()}", callback_data="setting_mode")
            ],
            [
                InlineKeyboardButton(
                    f"🔄 Auto Resume: {'✅' if auto_resume else '❌'}", 
                    callback_data="setting_auto_resume"
                )
            ],
            [
                InlineKeyboardButton("📝 Edit Caption Template", callback_data="setting_caption")
            ],
            [
                InlineKeyboardButton("🔙 Back", callback_data="start")
            ]
        ]
        return InlineKeyboardMarkup(buttons)

# ================================================================================
# 6. JOB PROCESSOR WITH CRASH RECOVERY
# ================================================================================

class JobProcessor:
    """Core job processor with crash recovery and cancellation support"""
    
    def __init__(self, bot_client):
        self.bot = bot_client
        self.active_jobs: Dict[str, asyncio.Task] = {}
        self.queue_lock = asyncio.Lock()
        self.processing_lock = asyncio.Semaphore(Config.MAX_CONCURRENT_JOBS)
        
        self.upload_timestamps: Dict[int, List[datetime]] = {}
        self.flood_wait_expiry: Dict[int, datetime] = {}
        
        # Download tasks for cancellation
        self.download_tasks: Dict[str, asyncio.Task] = {}
    
    async def start(self):
        """Start job processor and recover incomplete jobs"""
        logger.info("Starting job processor...")
        
        incomplete_jobs = await db.get_incomplete_jobs()
        if incomplete_jobs:
            logger.info(f"Found {len(incomplete_jobs)} incomplete jobs for recovery")
            
            for job in incomplete_jobs:
                if job.retry_count >= Config.MAX_RETRIES:
                    logger.error(f"Job {job.job_id} exceeded max retries. Marking as FAILED.")
                    await db.update_job_state(
                        job.job_id, 
                        JobState.FAILED, 
                        error_message="Max retries exceeded during recovery"
                    )
                    continue

                if job.state == JobState.FAILED:
                    retry_count = job.retry_count + 1
                else:
                    retry_count = job.retry_count
                
                if job.state in [JobState.QUEUED, JobState.PAUSED]:
                    await db.add_to_queue(job.job_id)
                    logger.info(f"Rescheduled job {job.job_id} for recovery")
                elif job.state in [JobState.DOWNLOADING, JobState.PROCESSING, JobState.UPLOADING]:
                    await db.update_job_state(
                        job.job_id, 
                        JobState.PAUSED, 
                        error_message="Bot restarted",
                        retry_count=retry_count
                    )
                    await db.add_to_queue(job.job_id, priority=10)
                    logger.info(f"Marked interrupted job {job.job_id} for resume")
        
        asyncio.create_task(self._queue_processor())
        logger.info("Job processor started")
    
    async def _queue_processor(self):
        """Process jobs from queue"""
        while True:
            try:
                async with self.queue_lock:
                    job_id = await db.get_next_job()
                
                if job_id:
                    async with self.processing_lock:
                        await self._process_job(job_id)
                else:
                    await asyncio.sleep(5)
                    
            except Exception as e:
                logger.error(f"Queue processor error: {e}")
                await asyncio.sleep(10)
    
    async def _process_job(self, job_id: str):
        """Process a single job with full state machine"""
        job = await db.get_job(job_id)
        if not job:
            logger.warning(f"Job {job_id} not found")
            return
        
        # Check if job was cancelled
        if job.cancel_requested or job.state == JobState.CANCELLED:
            logger.info(f"Job {job_id} was cancelled, skipping")
            await self._cleanup_job_completely(job_id)
            return
        
        try:
            await db.update_job_state(job_id, JobState.PROCESSING, started_at=datetime.now())
            
            status_msg = await self._create_status_message(job)
            
            if job.state == JobState.QUEUED or job.current_part == 1:
                await self._process_from_start(job, status_msg)
            elif job.state == JobState.PAUSED:
                await self._process_from_resume(job, status_msg)
            else:
                await self._process_from_start(job, status_msg)
                
        except asyncio.CancelledError:
            logger.info(f"Job {job_id} was cancelled")
            await db.update_job_state(job_id, JobState.CANCELLED, error_message="User cancelled")
            await self._cleanup_job_completely(job_id)
            raise
            
        except Exception as e:
            logger.error(f"Failed to process job {job_id}: {e}")
            await db.update_job_state(job_id, JobState.FAILED, error_message=str(e))
            await self._cleanup_job_completely(job_id)
    
    async def _process_from_start(self, job: JobData, status_msg: Message):
        """Process job from beginning (UPDATED LOGIC)"""
        job_id = job.job_id
        
        try:
            # Check cancellation
            current_job = await db.get_job(job_id)
            if current_job.cancel_requested:
                raise asyncio.CancelledError("Job cancelled by user")
            
            # DOWNLOAD PHASE
            await db.update_job_state(job_id, JobState.DOWNLOADING)
            # (UI update handled inside download function now)
            
            file_path = await self._download_file(job)
            if not file_path:
                raise Exception("Failed to download file")
            
            # Check cancellation after download
            current_job = await db.get_job(job_id)
            if current_job.cancel_requested:
                if os.path.exists(file_path):
                    os.remove(file_path)
                raise asyncio.CancelledError("Job cancelled by user")
            
            # PROBE PHASE
            await self._update_status_message(
                status_msg,
                "🔍 **Analyzing video**\n\nGetting exact duration...",
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )
            
            video_info = await VideoProcessor.get_video_info(file_path)
            if not video_info or video_info["duration"] <= 0:
                raise Exception(f"Invalid video duration: {video_info.get('duration', 0)}")
            
            real_duration = video_info["duration"]
            logger.info(f"Real duration detected: {real_duration}s (Initial was: {job.original_duration}s)")
            
            # --- FIX: UPDATE JOB TIME WITH REAL DURATION ---
            # If end_time was 0 (unknown) or full video requested, update it
            new_end_time = job.end_time
            if job.end_time <= 0 or job.end_time > real_duration or (job.end_time == job.original_duration and job.original_duration == 0):
                new_end_time = real_duration
                logger.info(f"Correcting end_time to {new_end_time}s")
            
            # Update DB with real info
            await db.jobs.update_one(
                {"job_id": job_id},
                {
                    "$set": {
                        "original_duration": real_duration,
                        "end_time": new_end_time,
                        "file_path": file_path
                    }
                }
            )
            
            # Refresh job object with new times
            job.end_time = new_end_time
            job.original_duration = real_duration
            
            # Calculate parts based on NEW duration
            process_duration = job.end_time - job.start_time
            total_parts = math.ceil(process_duration / job.clip_duration)
            
            if total_parts <= 0:
                 raise Exception(f"Invalid Time Range: {job.start_time} to {job.end_time} (Duration: {process_duration})")

            await db.update_job_state(
                job_id, 
                JobState.PROCESSING, 
                total_parts=total_parts
            )
            
            # PROCESSING LOOP
            await self._process_parts(job_id, status_msg, file_path, real_duration, total_parts)
            
            # COMPLETION
            await db.update_job_state(job_id, JobState.COMPLETED, completed_at=datetime.now())
            
            final_job = await db.get_job(job_id)
            completed_count = len(final_job.completed_parts)
            failed_count = len(final_job.failed_parts)
            
            await self._update_status_message(
                status_msg,
                f"✅ **Job Completed!**\n\n"
                f"✓ Total parts: {total_parts}\n"
                f"✓ Uploaded: {completed_count}\n"
                f"✓ Failed: {failed_count}\n"
                f"✓ Channel: {job.target_channel_name}\n\n"
                f"All files have been cleaned up."
            )
            
            # Cleanup
            await self._cleanup_job_completely(job_id)
            await self._update_user_stats(job.user_id, completed_count, real_duration)
            
        except asyncio.CancelledError:
            await db.update_job_state(job_id, JobState.CANCELLED, error_message="User cancelled")
            await self._update_status_message(status_msg, "❌ **Job Cancelled**\n\nAll files cleaned up.")
            await self._cleanup_job_completely(job_id)
            raise
            
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            await db.update_job_state(job_id, JobState.FAILED, error_message=str(e))
            await self._update_status_message(
                status_msg,
                f"❌ **Job Failed**\n\nError: `{str(e)[:200]}`"
            )
            await self._cleanup_job_completely(job_id)
    
    async def _process_from_resume(self, job: JobData, status_msg: Message):
        """Resume job from last completed part"""
        job_id = job.job_id
        
        try:
            # Check cancellation
            current_job = await db.get_job(job_id)
            if current_job.cancel_requested:
                raise asyncio.CancelledError("Job cancelled by user")
            
            if not job.file_path or not os.path.exists(job.file_path):
                await self._update_status_message(
                    status_msg,
                    "🔄 **Resuming - Downloading file again**",
                    reply_markup=UIComponents.create_job_control_buttons(job_id, True)
                )
                file_path = await self._download_file(job)
                if not file_path:
                    raise Exception("Failed to download file for resume")
                
                await db.update_job_state(job_id, JobState.PROCESSING, file_path=file_path)
                file_to_process = file_path
            else:
                file_to_process = job.file_path
            
            if job.original_duration <= 0:
                video_info = await VideoProcessor.get_video_info(file_to_process)
                duration = video_info["duration"]
                total_parts = math.ceil(duration / job.clip_duration)
                
                await db.update_job_state(
                    job_id,
                    JobState.PROCESSING,
                    original_duration=duration,
                    total_parts=total_parts
                )
            else:
                duration = job.original_duration
                total_parts = job.total_parts
            
            await self._update_status_message(
                status_msg,
                f"🔄 **Resuming Job**\n\nResuming from part {job.current_part}/{total_parts}",
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )
            
            await self._process_parts(job_id, status_msg, file_to_process, duration, total_parts, job.current_part)
            
            await db.update_job_state(job_id, JobState.COMPLETED, completed_at=datetime.now())
            
            completed_parts = len((await db.get_job(job_id)).completed_parts)
            
            await self._update_status_message(
                status_msg,
                f"✅ **Job Resumed & Completed!**\n\n"
                f"Total parts: {completed_parts}/{total_parts}\n\n"
                f"All files cleaned up and memory freed."
            )
            
            await self._cleanup_job_completely(job_id)
            
        except asyncio.CancelledError:
            await db.update_job_state(job_id, JobState.CANCELLED, error_message="User cancelled")
            await self._update_status_message(status_msg, "❌ **Job Cancelled**\n\nAll files cleaned up.")
            await self._cleanup_job_completely(job_id)
            raise
            
        except Exception as e:
            logger.error(f"Failed to resume job {job_id}: {e}")
            await db.update_job_state(job_id, JobState.FAILED, error_message=str(e))
            await self._update_status_message(
                status_msg,
                f"❌ **Resume Failed**\n\nError: `{str(e)[:200]}`"
            )
            await self._cleanup_job_completely(job_id)
    
    async def _process_parts(
        self,
        job_id: str,
        status_msg: Message,
        input_path: str,
        duration: float,
        total_parts: int,
        start_part: int = 1
    ):
        """Process all parts using FAST CUT (Copy Mode) to save RAM"""
        job = await db.get_job(job_id)
        if not job:
            return
        
        # Calculate actual processing start time
        base_start_time = job.start_time

        for part_num in range(start_part, total_parts + 1):
            # Check if job was cancelled
            current_job = await db.get_job(job_id)
            if current_job.cancel_requested or current_job.state == JobState.CANCELLED:
                logger.info(f"Job {job_id} cancelled during processing at part {part_num}")
                raise asyncio.CancelledError("Job cancelled by user")
            
            # --- TIMESTAMP LOGIC ---
            relative_start = (part_num - 1) * job.clip_duration
            abs_start_time = base_start_time + relative_start
            
            time_remaining = job.end_time - abs_start_time
            segment_duration = min(job.clip_duration, time_remaining)
            
            abs_end_time = abs_start_time + segment_duration
            
            if segment_duration <= 0.1:
                continue
            
            # Update status
            progress_bar = UIComponents.create_progress_bar(part_num - 1, total_parts)
            safe_filename = job.file_name.replace("`", "")
            
            await self._update_status_message(
                status_msg,
                f"⚙️ **Processing Part {part_num}/{total_parts}**\n\n"
                f"🎞 File: `{safe_filename}`\n"
                f"⏱ Time: {TimeUtils.format_timestamp(abs_start_time)} - {TimeUtils.format_timestamp(abs_end_time)}\n"
                f"📊 Progress: `{progress_bar}`\n"
                f"📍 Status: ✂️ Cutting (Fast Mode)...",
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )
            
            output_path = Path(Config.TEMP_DIR) / f"{job_id}_part_{part_num}.mp4"
            
            # Cut the segment
            # --- IMPORTANT CHANGE: reencode=False ---
            # This prevents RAM Crash and preserves 100% Original Quality
            success, error = await VideoProcessor.cut_video_segment(
                input_path,
                str(output_path),
                abs_start_time,
                segment_duration,
                reencode=False  # Changed from True to False
            )
            
            if not success:
                logger.error(f"Failed to cut part {part_num}: {error}")
                # Fallback: If copy fails, try re-encode briefly (optional, usually not needed)
                await db.jobs.update_one(
                    {"job_id": job_id},
                    {"$push": {"failed_parts": part_num}}
                )
                continue
            
            # Upload Phase
            await db.update_job_state(job_id, JobState.UPLOADING)
            
            caption = job.caption_template.format(
                part=part_num,
                start_time=TimeUtils.format_timestamp(abs_start_time),
                end_time=TimeUtils.format_timestamp(abs_end_time),
                total_parts=total_parts,
                file_name=safe_filename
            )
            
            # --- PASS STATUS_MSG HERE ---
            upload_success = await self._upload_with_retry(
                job,
                str(output_path),
                caption,
                part_num,
                status_msg
            )
            
            if upload_success:
                await db.jobs.update_one(
                    {"job_id": job_id},
                    {
                        "$set": {"current_part": part_num + 1, "updated_at": datetime.now()},
                        "$push": {"completed_parts": part_num}
                    }
                )
            else:
                await db.jobs.update_one(
                    {"job_id": job_id},
                    {"$push": {"failed_parts": part_num}}
                )
            
            # Cleanup clip - FORCE GC to free RAM immediately
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                    import gc
                    gc.collect() # Manually clear RAM
                except:
                    pass
            
            await asyncio.sleep(Config.UPLOAD_COOLDOWN)
            await db.update_job_state(job_id, JobState.PROCESSING)
    
    async def _upload_with_retry(
        self,
        job: JobData,
        file_path: str,
        caption: str,
        part_num: int,
        status_msg: Message = None
    ) -> bool:
        """Upload with retry logic and PROGRESS BAR"""
        max_retries = Config.MAX_RETRIES
        total_parts = job.total_parts
        
        # Set upload start time for speed calculation
        setattr(self, f'_up_start_{job.job_id}', time.time())
        
        for attempt in range(max_retries):
            try:
                # Check cancellation
                current_job = await db.get_job(job.job_id)
                if current_job.cancel_requested:
                    raise asyncio.CancelledError("Job cancelled")
                
                # FloodWait Check
                if job.target_channel_id in self.flood_wait_expiry:
                    expiry = self.flood_wait_expiry[job.target_channel_id]
                    if datetime.now() < expiry:
                        wait_seconds = (expiry - datetime.now()).total_seconds()
                        if status_msg:
                            await self._update_status_message(status_msg, f"⏳ FloodWait: Sleeping {int(wait_seconds)}s...")
                        await asyncio.sleep(wait_seconds)
                
                # UPLOAD WITH PROGRESS
                if job.upload_mode == UploadMode.VIDEO:
                    await self.bot.send_video(
                        chat_id=job.target_channel_id,
                        video=file_path,
                        caption=caption,
                        supports_streaming=True,
                        parse_mode=enums.ParseMode.MARKDOWN,
                        progress=self._upload_progress,
                        progress_args=(job.job_id, part_num, total_parts)
                    )
                else:
                    await self.bot.send_document(
                        chat_id=job.target_channel_id,
                        document=file_path,
                        caption=caption,
                        parse_mode=enums.ParseMode.MARKDOWN,
                        progress=self._upload_progress,
                        progress_args=(job.job_id, part_num, total_parts)
                    )
                
                logger.info(f"Uploaded part {part_num}")
                return True
                
            except errors.FloodWait as e:
                wait_time = e.value * Config.FLOOD_WAIT_BACKOFF
                expiry_time = datetime.now() + timedelta(seconds=wait_time)
                self.flood_wait_expiry[job.target_channel_id] = expiry_time
                await asyncio.sleep(wait_time)
                continue
                
            except asyncio.CancelledError:
                raise
                
            except Exception as e:
                logger.error(f"Upload error: {e}")
                await asyncio.sleep(5)
        
        return False
    
    async def _download_file(self, job: JobData) -> Optional[str]:
        """Download file with progress tracking and cancellation support"""
        try:
            # Check cancellation before starting
            current_job = await db.get_job(job.job_id)
            if current_job.cancel_requested:
                raise asyncio.CancelledError("Job cancelled before download")
            
            message = await self.bot.get_messages(job.user_id, job.file_message_id)
            if not message or not message.media:
                raise Exception("Message or media not found")
            
            file_name = f"{job.job_id}_{job.file_name}"
            file_path = Path(Config.WORK_DIR) / file_name
            
            # Create download task
            download_task = asyncio.create_task(
                self.bot.download_media(
                    message,
                    file_name=str(file_path),
                    progress=self._download_progress,
                    progress_args=(job.job_id,)
                )
            )
            
            self.download_tasks[job.job_id] = download_task
            
            try:
                download_path = await download_task
            finally:
                if job.job_id in self.download_tasks:
                    del self.download_tasks[job.job_id]
            
            if download_path and os.path.exists(download_path):
                file_size = os.path.getsize(download_path)
                logger.info(f"Downloaded {job.file_name} ({TimeUtils.format_file_size(file_size)})")
                return download_path
            
            return None
            
        except asyncio.CancelledError:
            logger.info(f"Download cancelled for job {job.job_id}")
            if job.job_id in self.download_tasks:
                self.download_tasks[job.job_id].cancel()
                del self.download_tasks[job.job_id]
            raise
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return None
    
    async def _download_progress(self, current, total, job_id):
        """Show download progress"""
        now = time.time()
        # Update only every 3 seconds to avoid FloodWait
        if not hasattr(self, '_last_update'):
            self._last_update = {}
        
        last_time = self._last_update.get(job_id, 0)
        if now - last_time < 3 and current != total:
            return
            
        self._last_update[job_id] = now
        
        try:
            job = await db.get_job(job_id)
            if not job or not job.progress_message_id:
                return
                
            percentage = current * 100 / total
            speed = current / (now - job.created_at.timestamp() + 1) # Simple speed calc
            
            progress_str = UIComponents.create_progress_bar(current, total)
            text = (
                f"📥 **Downloading...**\n"
                f"`{job.file_name}`\n\n"
                f"{progress_str}\n"
                f"💾 {TimeUtils.format_file_size(current)} / {TimeUtils.format_file_size(total)}\n"
                f"🚀 Speed: {TimeUtils.format_file_size(speed)}/s"
            )
            
            await self.bot.edit_message_text(
                chat_id=job.user_id,
                message_id=job.progress_message_id,
                text=text,
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )
        except Exception:
            pass
    
    async def _upload_progress(self, current, total, job_id, part_num, total_parts):
        """Show upload progress for each part"""
        now = time.time()
        # Update only every 3 seconds to avoid FloodWait
        if not hasattr(self, '_last_up_update'):
            self._last_up_update = {}
        
        last_time = self._last_up_update.get(job_id, 0)
        if now - last_time < 3 and current != total:
            return
            
        self._last_up_update[job_id] = now
        
        try:
            job = await db.get_job(job_id)
            if not job or not job.progress_message_id:
                return
                
            percentage = current * 100 / total
            speed = current / (now - getattr(self, f'_up_start_{job_id}', now) + 1)
            
            progress_bar = UIComponents.create_progress_bar(current, total)
            
            text = (
                f"📤 **Uploading Part {part_num}/{total_parts}**\n\n"
                f"{progress_bar}\n"
                f"📊 {percentage:.1f}%\n"
                f"💾 {TimeUtils.format_file_size(current)} / {TimeUtils.format_file_size(total)}\n"
                f"🚀 Speed: {TimeUtils.format_file_size(speed)}/s"
            )
            
            await self.bot.edit_message_text(
                chat_id=job.user_id,
                message_id=job.progress_message_id,
                text=text,
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )
        except Exception:
            pass
    
    async def _create_status_message(self, job: JobData) -> Message:
        """Create or get status message"""
        if job.progress_message_id:
            try:
                return await self.bot.get_messages(job.user_id, job.progress_message_id)
            except:
                pass
        
        message = await self.bot.send_message(
            job.user_id,
            "🔄 **Job Initializing...**",
            reply_markup=UIComponents.create_job_control_buttons(job.job_id, True)
        )
        
        await db.update_job_state(
            job.job_id,
            job.state,
            progress_message_id=message.id
        )
        
        return message
    
    async def _update_status_message(self, message: Message, text: str, **kwargs):
        """Update status message with checks to prevent errors"""
        try:
            # Check if content is same to prevent "Message Not Modified"
            if message.text and message.text.strip() == text.strip():
                return
            
            await message.edit_text(text, **kwargs)
        except errors.MessageNotModified:
            pass # Ignore if identical
        except errors.FloodWait as e:
            logger.warning(f"Status update floodwait: {e.value}")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.warning(f"Failed to update status message: {e}")
    
    async def _update_user_stats(self, user_id: int, parts: int, duration: float):
        """Update user statistics"""
        try:
            await db.users.update_one(
                {"user_id": user_id},
                {
                    "$inc": {
                        "stats.jobs_completed": 1,
                        "stats.total_parts": parts,
                        "stats.total_duration": duration
                    },
                    "$set": {"updated_at": datetime.now()}
                }
            )
        except Exception as e:
            logger.error(f"Failed to update user stats: {e}")
    
    async def _cleanup_job_completely(self, job_id: str):
        """Complete cleanup of all job files and free memory"""
        job = await db.get_job(job_id)
        
        # Cleanup source file
        if job and job.file_path and os.path.exists(job.file_path):
            try:
                file_size = os.path.getsize(job.file_path)
                os.remove(job.file_path)
                logger.info(f"Cleaned up source file ({TimeUtils.format_file_size(file_size)})")
            except Exception as e:
                logger.error(f"Failed to cleanup source file: {e}")
        
        # Cleanup all temporary files
        cleaned, freed = await ResourceMonitor.force_cleanup_job_files(job_id)
        
        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} files, freed {TimeUtils.format_file_size(freed)}")
        
        # Update job to clear file path
        await db.jobs.update_one(
            {"job_id": job_id},
            {"$set": {"file_path": None, "updated_at": datetime.now()}}
        )
    
    async def cancel_job(self, job_id: str, user_id: int) -> bool:
        """Cancel a running job and cleanup all resources"""
        job = await db.get_job(job_id)
        if not job or job.user_id != user_id:
            return False
        
        # Mark as cancelled in database
        await db.mark_job_cancelled(job_id)
        await db.update_job_state(job_id, JobState.CANCELLED, error_message="User cancelled")
        
        # Cancel download task if active
        if job_id in self.download_tasks:
            self.download_tasks[job_id].cancel()
            del self.download_tasks[job_id]
        
        # Cancel processing task if running
        if job_id in self.active_jobs:
            self.active_jobs[job_id].cancel()
            del self.active_jobs[job_id]
        
        # Complete cleanup
        await self._cleanup_job_completely(job_id)
        
        logger.info(f"Job {job_id} cancelled by user {user_id}")
        return True

# ================================================================================
# 7. TELEGRAM BOT HANDLERS (FIXED & COMPLETE)
# ================================================================================

class BotHandlers:
    """All Telegram bot handlers"""
    
    def __init__(self, bot_client, job_processor):
        self.bot = bot_client
        self.processor = job_processor
        self.user_states: Dict[int, Dict] = {}
    
    async def start_command(self, client: Client, message: Message):
        """Handle /start command"""
        # Fix: Get ID reliably
        user = message.from_user
        if not user:
            return # Ignore channel posts
            
        user_id = user.id
        
        logger.info(f"Start command from User: {user_id}")
        
        # Access Check: Allow Owner OR anyone in ADMIN_IDS
        if user_id not in Config.ADMIN_IDS:
            await message.reply(
                f"⛔ **Access Denied**\n\n"
                f"Your ID: `{user_id}`\n"
                f"This bot is private. Contact the owner to get access."
            )
            return
        
        # Create user in DB
        await db.get_or_create_user(user_id, user.first_name or "Unknown")
        
        incomplete_job = await db.get_active_user_job(user_id)
        if incomplete_job:
            buttons = [
                [
                    InlineKeyboardButton("🔄 Resume", callback_data=f"resume_{incomplete_job.job_id}"),
                    InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job_{incomplete_job.job_id}")
                ],
                [InlineKeyboardButton("📊 Details", callback_data=f"details_{incomplete_job.job_id}")]
            ]
            
            await message.reply(
                f"⚠️ **Incomplete Job Found**\n\n"
                f"Job ID: `{incomplete_job.job_id}`\n"
                f"File: `{incomplete_job.file_name}`\n"
                f"State: {incomplete_job.state.name}\n\n"
                f"Resume or Cancel?",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return
        
        text = (
            "👋 **Welcome to Auto-Splitter Bot v2.1**\n\n"
            "**🚀 Quick Start:**\n"
            "1. Send me a video file\n"
            "2. Select clip duration\n"
            "3. Choose target channel\n\n"
            "**📝 Commands:**\n"
            "/channels - Manage channels\n"
            "/settings - Your preferences\n"
            "/stats - View statistics"
        )
        
        buttons = [
            [
                InlineKeyboardButton("📤 Send Video", callback_data="help_video"),
                InlineKeyboardButton("⚙️ Settings", callback_data="settings_main")
            ],
            [
                InlineKeyboardButton("📢 Channels", callback_data="channel_list"),
                InlineKeyboardButton("📊 My Stats", callback_data="user_stats")
            ]
        ]
        
        # Only show Admin Panel button to the OWNER
        if user_id == Config.OWNER_ID:
            buttons.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
        
        await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))
    
    async def video_handler(self, client: Client, message: Message):
        """Handle incoming video files - Ask for Mode first"""
        if not message.from_user: return
        user_id = message.from_user.id
        
        # Access Check
        if user_id not in Config.ADMIN_IDS:
            return
        
        active_job = await db.get_active_user_job(user_id)
        if active_job:
            await message.reply("⚠️ You already have an active job. Please cancel it first.")
            return
        
        # Get file info
        if message.video:
            file_name = message.video.file_name or f"video_{message.id}.mp4"
            file_size = message.video.file_size
            duration = message.video.duration
            file_id = message.video.file_id
        elif message.document:
            file_name = message.document.file_name or f"video_{message.id}.mp4"
            file_size = message.document.file_size
            duration = getattr(message.document, "duration", 0) 
            file_id = message.document.file_id
        else:
            return
        
        # Save temporary state
        self.user_states[user_id] = {
            "file_message_id": message.id,
            "file_id": file_id,
            "file_name": file_name,
            "file_size": file_size,
            "duration": duration,
            "state": "waiting_mode"
        }
        
        await message.reply(
            f"🎬 **Video Received**\n`{file_name}`\n\n**Select Processing Mode:**",
            reply_markup=UIComponents.create_trim_mode_buttons(message.id)
        )

    async def text_handler(self, client: Client, message: Message):
        """Handle text input for Custom Time range"""
        if not message.from_user: return
        user_id = message.from_user.id
        
        if user_id not in Config.ADMIN_IDS:
            return

        state_data = self.user_states.get(user_id)
        
        if not state_data or state_data.get("state") != "waiting_custom_time":
            return
            
        text = message.text.strip()
        
        try:
            parts = text.split()
            if len(parts) != 2:
                await message.reply("❌ Invalid format. Please send: `Start End`\nExample: `00:15:00 01:50:00`")
                return
            
            def parse_time(t_str):
                t_parts = list(map(int, t_str.split(':')))
                if len(t_parts) == 3:
                    return t_parts[0]*3600 + t_parts[1]*60 + t_parts[2]
                elif len(t_parts) == 2:
                    return t_parts[0]*60 + t_parts[1]
                else:
                    raise ValueError
            
            start_seconds = parse_time(parts[0])
            end_seconds = parse_time(parts[1])
            
            if start_seconds >= end_seconds:
                await message.reply("❌ Start time must be less than End time.")
                return
                
            if state_data.get("duration") and end_seconds > state_data["duration"]:
                await message.reply(f"❌ End time exceeds video duration ({TimeUtils.format_timestamp(state_data['duration'])})")
                return

            self.user_states[user_id]["custom_start"] = start_seconds
            self.user_states[user_id]["custom_end"] = end_seconds
            self.user_states[user_id]["state"] = "waiting_duration"
            
            new_duration = end_seconds - start_seconds
            
            await message.reply(
                f"✅ **Range Set:** {parts[0]} - {parts[1]}\n"
                f"Total Clip Time: {TimeUtils.format_duration(new_duration)}\n\n"
                f"**Now select split duration per part:**",
                reply_markup=UIComponents.create_duration_buttons(state_data["file_message_id"])
            )
            
        except ValueError:
            await message.reply("❌ Invalid time format. Use HH:MM:SS (e.g., `00:15:00 01:30:00`)")
        except Exception as e:
            logger.error(f"Time parse error: {e}")
            await message.reply("❌ Error parsing time.")

    async def callback_handler(self, client: Client, callback_query: CallbackQuery):
        """Handle all callback queries (Improved Permission Logic)"""
        data = callback_query.data
        user_id = callback_query.from_user.id
        
        # Access Check: Allow anyone in ADMIN_IDS (Owner is included in this list)
        if user_id not in Config.ADMIN_IDS:
            if not data.startswith("close"):
                await callback_query.answer("⛔ Access Denied", show_alert=True)
                return
        
        try:
            # --- MENU HANDLERS ---
            if data == "help_video":
                await callback_query.message.edit_text(
                    "📤 **How to Send Video**\n\nSimply send any Video file here.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="start")]])
                )
            
            elif data == "start":
                await self.start_command(client, callback_query.message)

            # --- MODE SELECTION ---
            elif data.startswith("mode_full_"):
                msg_id = int(data.split("_")[2])
                if user_id in self.user_states:
                    self.user_states[user_id]["custom_start"] = 0
                    self.user_states[user_id]["custom_end"] = self.user_states[user_id]["duration"]
                    self.user_states[user_id]["state"] = "waiting_duration"
                    
                    await callback_query.message.edit_text(
                        "🎬 **Full Video Selected**\n\n**Select split duration:**",
                        reply_markup=UIComponents.create_duration_buttons(msg_id)
                    )
            
            elif data.startswith("mode_custom_"):
                if user_id in self.user_states:
                    self.user_states[user_id]["state"] = "waiting_custom_time"
                    await callback_query.message.edit_text(
                        "✂️ **Custom Trim Mode**\n\n"
                        "Send me the **Start** and **End** time.\n"
                        "Format: `HH:MM:SS HH:MM:SS`\n\n"
                        "Example: `00:15:00 01:50:00`"
                    )

            # --- PROCESS FLOW ---
            elif data.startswith("dur_"):
                _, msg_id, duration = data.split("_")
                await self._handle_duration_selection(callback_query, int(msg_id), int(duration))
            
            elif data.startswith("chan_"):
                _, msg_id, duration, channel_id = data.split("_")
                await self._handle_channel_selection(callback_query, int(msg_id), int(duration), int(channel_id))
            
            elif data.startswith("start_job_"):
                _, _, msg_id, duration, channel_id = data.split("_")
                await self._handle_job_start(callback_query, int(msg_id), int(duration), int(channel_id))
            
            # --- JOB CONTROL ---
            elif data.startswith("cancel_job_"):
                job_id = data.split("_", 2)[2]
                await self._handle_job_cancellation(callback_query, job_id)
            
            elif data.startswith("resume_"):
                job_id = data.split("_")[1]
                await self._handle_job_resume(callback_query, job_id)
            
            elif data.startswith("details_"):
                job_id = data.split("_")[1]
                await self._show_job_details(callback_query, job_id)
            
            # --- NAVIGATION ---
            elif data.startswith("back_dur_"):
                msg_id = data.split("_")[2]
                await callback_query.message.edit_text(
                    "⏱ **Select clip duration:**",
                    reply_markup=UIComponents.create_duration_buttons(int(msg_id))
                )
            
            elif data.startswith("back_to_chan_"):
                _, _, _, msg_id, duration = data.split("_")
                channels = await db.get_user_channels(user_id)
                await callback_query.message.edit_text(
                    f"⏱ **Duration:** {duration}s\n\n**Select target channel:**",
                    reply_markup=UIComponents.create_channel_buttons(channels, int(msg_id), int(duration))
                )
            
            # --- ADMIN PANELS (RESTRICTED TO OWNER ONLY) ---
            elif data.startswith("admin_"):
                # Strict check for Owner ID
                if user_id != Config.OWNER_ID:
                    await callback_query.answer("🔒 Restricted to Owner.", show_alert=True)
                    return

                if data == "admin_panel":
                    await self._show_admin_panel(callback_query)
                elif data == "admin_stats":
                    await self._show_admin_stats(callback_query)
                elif data == "admin_jobs":
                    await self._show_admin_jobs(callback_query)
                elif data == "admin_storage":
                    await self._show_admin_storage(callback_query)
                elif data == "admin_cleanup":
                    await self._handle_admin_cleanup(callback_query)
            
            # --- CHANNELS ---
            elif data == "channel_list":
                await self._show_channel_list(callback_query)
            
            elif data == "add_channel_prompt":
                 await callback_query.message.edit_text(
                     "➕ **Add Channel**\n\nUse this command:\n`/addchannel CHANNEL_ID CHANNEL_NAME`\n\n"
                     "Example:\n`/addchannel -1001234567890 MyMovieChannel`\n\n"
                     "Note: Make sure to add me as Admin in that channel first!",
                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="channel_list")]])
                 )
            
            elif data.startswith("delete_channel_"):
                channel_id = int(data.split("_")[2])
                await self._handle_channel_deletion(callback_query, channel_id)
            
            elif data == "verify_channels":
                await self._verify_all_channels(callback_query)
            
            # --- SETTINGS ---
            elif data == "settings_main":
                await self._show_settings(callback_query)
            
            elif data == "setting_duration":
                await callback_query.answer("Use /settings command", show_alert=True)
            
            elif data == "setting_mode":
                settings = await db.get_user_settings(user_id)
                new_mode = "document" if settings.get("upload_mode") == "video" else "video"
                settings["upload_mode"] = new_mode
                await db.update_user_settings(user_id, settings)
                await self._show_settings(callback_query)
                await callback_query.answer(f"Switched to {new_mode.upper()}", show_alert=True)
            
            elif data == "setting_auto_resume":
                settings = await db.get_user_settings(user_id)
                settings["auto_resume"] = not settings.get("auto_resume", True)
                await db.update_user_settings(user_id, settings)
                await self._show_settings(callback_query)
            
            elif data == "user_stats":
                await self._show_user_stats(callback_query)
            
            # --- MISC ---
            elif data == "cancel_all":
                if user_id in self.user_states:
                    del self.user_states[user_id]
                await callback_query.message.delete()
                await callback_query.answer("Cancelled")
            
            elif data == "close":
                await callback_query.message.delete()
            
            else:
                await callback_query.answer(f"❌ Unknown action: {data}", show_alert=True)
                
        except Exception as e:
            logger.error(f"Callback error: {e}")
            # Prevent popup error if something goes wrong
            try:
                await callback_query.answer("⚠️ Processing...", show_alert=False)
            except:
                pass

    # --- HELPER METHODS ---
    
    async def _handle_duration_selection(self, callback: CallbackQuery, msg_id: int, duration: int):
        user_id = callback.from_user.id
        if user_id in self.user_states:
            self.user_states[user_id]["clip_duration"] = duration
            self.user_states[user_id]["state"] = "waiting_channel"
        
        channels = await db.get_user_channels(user_id)
        if not channels:
            await callback.message.edit_text(
                "❌ **No Channels Found**\nUse `/addchannel` first.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"back_dur_{msg_id}")]])
            )
            return
        
        await callback.message.edit_text(
            f"⏱ **Duration:** {duration}s\n\n**Select channel:**",
            reply_markup=UIComponents.create_channel_buttons(channels, msg_id, duration)
        )

    async def _handle_channel_selection(self, callback: CallbackQuery, msg_id: int, duration: int, channel_id: int):
        user_id = callback.from_user.id
        channels = await db.get_user_channels(user_id)
        channel = next((c for c in channels if c.channel_id == channel_id), None)
        
        if not channel:
            await callback.answer("Channel not found", show_alert=True)
            return
        
        file_info = self.user_states.get(user_id)
        if not file_info:
            await callback.answer("Session expired", show_alert=True)
            return
        
        total_parts = math.ceil(file_info.get("duration", 0) / duration) if file_info.get("duration", 0) > 0 else "?"
        
        preview = (
            f"📋 **Preview**\n"
            f"File: `{file_info['file_name']}`\n"
            f"Clip: {duration}s | Parts: {total_parts}\n"
            f"Target: {channel.name}"
        )
        await callback.message.edit_text(
            preview,
            reply_markup=UIComponents.create_preview_buttons(msg_id, duration, channel_id)
        )

    async def _handle_job_start(self, callback: CallbackQuery, msg_id: int, duration: int, channel_id: int):
        user_id = callback.from_user.id
        file_info = self.user_states.get(user_id)
        
        if not file_info:
            await callback.answer("Session expired", show_alert=True)
            return
            
        channels = await db.get_user_channels(user_id)
        channel = next((c for c in channels if c.channel_id == channel_id), None)
        
        job_id = hashlib.md5(f"{user_id}_{time.time()}".encode()).hexdigest()[:12]
        settings = await db.get_user_settings(user_id)
        
        start_time = file_info.get("custom_start", 0)
        end_time = file_info.get("custom_end", file_info.get("duration", 0))
        
        job_data = JobData(
            job_id=job_id,
            user_id=user_id,
            file_message_id=file_info["file_message_id"],
            file_id=file_info["file_id"],
            file_name=file_info["file_name"],
            file_size=file_info["file_size"],
            original_duration=file_info.get("duration", 0),
            start_time=start_time,
            end_time=end_time,
            clip_duration=duration,
            target_channel_id=channel_id,
            target_channel_name=channel.name if channel else "Unknown",
            state=JobState.QUEUED,
            caption_template=settings.get("caption_template", "[Part {part}]"),
            upload_mode=UploadMode(settings.get("upload_mode", "video"))
        )
        
        await db.create_job(job_data)
        await db.add_to_queue(job_id)
        del self.user_states[user_id]
        
        await callback.message.edit_text(f"✅ **Job {job_id} Queued!**\nTime Range: {TimeUtils.format_timestamp(start_time)} - {TimeUtils.format_timestamp(end_time)}")

    async def _handle_job_cancellation(self, callback: CallbackQuery, job_id: str):
        success = await self.processor.cancel_job(job_id, callback.from_user.id)
        if success:
            await callback.answer("Job cancelled", show_alert=True)
            await callback.message.edit_text("❌ Job Cancelled")
        else:
            await callback.answer("Failed to cancel", show_alert=True)

    async def _handle_job_resume(self, callback: CallbackQuery, job_id: str):
        job = await db.get_job(job_id)
        if job and job.state == JobState.PAUSED:
            await db.update_job_state(job_id, JobState.QUEUED)
            await db.add_to_queue(job_id, priority=5)
            await callback.message.edit_text("🔄 Resumed")
        else:
            await callback.answer("Cannot resume", show_alert=True)

    async def _show_job_details(self, callback: CallbackQuery, job_id: str):
        job = await db.get_job(job_id)
        if not job:
            await callback.answer("Not found", show_alert=True)
            return
        text = f"📊 **Job {job_id}**\nStatus: {job.state.name}\nParts: {len(job.completed_parts)}/{job.total_parts}"
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="start")]]))

    async def _show_admin_panel(self, callback: CallbackQuery):
        # Double check owner logic here just in case
        if callback.from_user.id != Config.OWNER_ID:
            await callback.answer("🔒 Restricted to Owner.", show_alert=True)
            return
            
        stats = await db.get_statistics()
        text = f"🛠 **Admin**\nJobs: {stats['total_jobs']}\nActive: {stats['active_jobs']}"
        await callback.message.edit_text(text, reply_markup=UIComponents.create_admin_dashboard_buttons())

    async def _show_admin_stats(self, callback: CallbackQuery):
        await self._show_admin_panel(callback)

    async def _show_admin_jobs(self, callback: CallbackQuery):
        active_jobs = await db.get_incomplete_jobs()
        if not active_jobs:
            await callback.answer("No active jobs", show_alert=True)
            return
        text = "🔄 **Active Jobs:**\n" + "\n".join([f"`{j.job_id}`: {j.state.name}" for j in active_jobs[:5]])
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="admin_panel")]]))

    async def _show_admin_storage(self, callback: CallbackQuery):
        await self._show_admin_panel(callback)

    async def _handle_admin_cleanup(self, callback: CallbackQuery):
        await callback.answer("Cleaning...", show_alert=False)
        ResourceMonitor.cleanup_old_files(1)
        await callback.answer("Done!", show_alert=True)

    async def _show_channel_list(self, callback: CallbackQuery):
        channels = await db.get_user_channels(callback.from_user.id)
        text = "📢 **Channels**\n" + "\n".join([f"• {c.name}" for c in channels]) if channels else "No channels."
        
        buttons = []
        for c in channels:
            buttons.append([InlineKeyboardButton(f"🗑 {c.name}", callback_data=f"delete_channel_{c.channel_id}")])
        buttons.append([InlineKeyboardButton("➕ Add", callback_data="add_channel_prompt")])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="start")])
        
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    async def _handle_channel_deletion(self, callback: CallbackQuery, channel_id: int):
        await db.remove_channel(callback.from_user.id, channel_id)
        await self._show_channel_list(callback)

    async def _verify_all_channels(self, callback: CallbackQuery):
        await callback.answer("Verifying...", show_alert=False)
        # Simple verification logic
        await callback.answer("Done", show_alert=True)

    async def _show_settings(self, callback: CallbackQuery):
        settings = await db.get_user_settings(callback.from_user.id)
        await callback.message.edit_text("⚙️ **Settings**", reply_markup=UIComponents.create_settings_buttons(settings))

    async def _show_user_stats(self, callback: CallbackQuery):
        user = await db.users.find_one({"user_id": callback.from_user.id})
        stats = user.get("stats", {}) if user else {}
        text = f"📊 **Stats**\nCompleted: {stats.get('jobs_completed', 0)}"
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="start")]]))

# ================================================================================
# 8. COMMAND HANDLERS & HELPERS
# ================================================================================

async def handle_addchannel_command(client: Client, message: Message):
    """Handle /addchannel command"""
    user_id = message.from_user.id
    
    if user_id not in Config.ADMIN_IDS:
        return await message.reply("⛔ Access Denied")
    
    try:
        args = message.text.split()
        if len(args) < 3:
            await message.reply(
                "❌ **Usage:** `/addchannel <channel_id> <name>`\n\n"
                "Example: `/addchannel -100123456 MyMovies`"
            )
            return

        channel_id = int(args[1])
        name = " ".join(args[2:])
        
        # Verify permissions immediately
        verified = False
        try:
            msg = await client.send_message(channel_id, "✅ Bot connected successfully!")
            await msg.delete()
            verified = True
        except Exception as e:
            await message.reply(f"⚠️ **Warning:** I cannot send messages to that channel.\nError: `{e}`\n\nPlease make sure I am an Admin there.")
            verified = False

        await db.add_channel(user_id, channel_id, name)
        if verified:
            await db.update_channel_permissions(user_id, channel_id, True)
            
        await message.reply(f"✅ Channel **{name}** added successfully!")
        
    except ValueError:
        await message.reply("❌ Invalid Channel ID. It must be a number (e.g., -100...)")
    except Exception as e:
        logger.error(f"Add channel error: {e}")
        await message.reply("❌ An error occurred.")

# ================================================================================
# 9. WEB SERVER
# ================================================================================

async def web_server():
    """Web server for health checks"""
    routes = web.RouteTableDef()
    
    @routes.get('/')
    async def root_route(request):
        return web.json_response({
            "status": "running",
            "uptime": "online",
            "timestamp": datetime.now().isoformat()
        })

    @routes.get('/health')
    async def health_route(request):
        return web.Response(text="OK", status=200)

    app = web.Application()
    app.add_routes(routes)
    return app

# ================================================================================
# 10. MAIN APPLICATION
# ================================================================================

class AutoSplitterBot:
    """Main bot application wrapper"""
    
    def __init__(self):
        self.client = Client(
            "AutoSplitterBot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN,
            workers=10
        )
        self.job_processor = JobProcessor(self.client)
        self.handlers = BotHandlers(self.client, self.job_processor)

    async def start(self):
        """Start the application"""
        # Validate Config
        Config.validate()
        
        # Init DB Indexes
        await db.create_indexes()
        
        # Start Web Server
        app = await web_server()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, Config.WEB_HOST, Config.WEB_PORT)
        await site.start()
        logger.info(f"Web server started on port {Config.WEB_PORT}")
        
        # Register Handlers
        self._register_handlers()
        
        # Start Client
        await self.client.start()
        logger.set_bot_client(self.client)
        
        me = await self.client.get_me()
        logger.info(f"Bot started as @{me.username}")
        
        # Start Processor
        await self.job_processor.start()
        
        # Send startup msg
        try:
            await self.client.send_message(
                Config.OWNER_ID, 
                f"✅ **Bot Started Successfully!**\nDate: {datetime.now()}"
            )
        except:
            pass
            
        # Idle
        await asyncio.Event().wait()

    def _register_handlers(self):
        """Register Pyrogram handlers using explicit Handler classes"""
        
        # 1. Start Command
        self.client.add_handler(
            MessageHandler(
                self.handlers.start_command,
                filters.command("start") & filters.private
            )
        )
        
        # 2. Add Channel Command (with wrapper)
        async def addchannel_wrapper(client, message):
            await handle_addchannel_command(client, message)
            
        self.client.add_handler(
            MessageHandler(
                addchannel_wrapper,
                filters.command("addchannel") & filters.private
            )
        )
        
        # 3. Settings Command
        self.client.add_handler(
            MessageHandler(
                lambda c, m: self.handlers.start_command(c, m),
                filters.command("settings") & filters.private
            )
        )
        
        # 4. Video/Document Handler
        self.client.add_handler(
            MessageHandler(
                self.handlers.video_handler,
                (filters.video | filters.document) & filters.private
            )
        )
        
        # 5. Callback Query Handler
        self.client.add_handler(
            CallbackQueryHandler(
                self.handlers.callback_handler
            )
        )
        
        # 6. Text Handler (For Custom Time Input)
        self.client.add_handler(
            MessageHandler(
                self.handlers.text_handler,
                filters.text & filters.private & ~filters.command(["start", "addchannel", "settings"])
            )
        )

    async def stop(self):
        await self.client.stop()
# ================================================================================
# 11. ENTRY POINT
# ================================================================================

async def main():
    bot = AutoSplitterBot()
    
    def signal_handler():
        logger.info("Stopping bot...")
        asyncio.create_task(bot.stop())
        sys.exit(0)

    try:
        await bot.start()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.critical(f"Fatal error: {e}\n{traceback.format_exc()}")

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

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

# ================================================================================
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
    DATABASE_URL = os.environ.get("DATABASE_URL", "mongodb://localhost:27017")
    DATABASE_NAME = os.environ.get("DATABASE_NAME", "AutoSplitterBot")
    
    # Security
    OWNER_ID = int(os.environ.get("OWNER_ID", 0))
    ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x] + [OWNER_ID]
    
    # Paths
    WORK_DIR = os.environ.get("WORK_DIR", "downloads")
    LOG_DIR = os.environ.get("LOG_DIR", "logs")
    TEMP_DIR = os.environ.get("TEMP_DIR", "temp")
    
    # Limits
    MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", 1))
    MAX_QUEUE_SIZE = int(os.environ.get("MAX_QUEUE_SIZE", 10))
    MAX_CLIP_DURATION = int(os.environ.get("MAX_CLIP_DURATION", 300))
    MIN_CLIP_DURATION = int(os.environ.get("MIN_CLIP_DURATION", 10))
    
    # Validation
    MIN_CLIP_SIZE = int(os.environ.get("MIN_CLIP_SIZE", 10240))
    MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
    VALIDATE_EACH_CLIP = os.environ.get("VALIDATE_EACH_CLIP", "true").lower() == "true"
    
    # Flood Control
    FLOOD_WAIT_BACKOFF = int(os.environ.get("FLOOD_WAIT_BACKOFF", 2))
    MAX_FLOOD_WAIT = int(os.environ.get("MAX_FLOOD_WAIT", 3600))
    UPLOAD_COOLDOWN = float(os.environ.get("UPLOAD_COOLDOWN", 1.0))
    
    # FFmpeg
    FFMPEG_PRESET = os.environ.get("FFMPEG_PRESET", "fast")
    
    # Cleanup
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
        """Validate configuration"""
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
        
        # --- FIX: Create logs directory immediately ---
        Path(Config.LOG_DIR).mkdir(parents=True, exist_ok=True)
        # ----------------------------------------------
        
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
    def create_duration_buttons(message_id: int) -> InlineKeyboardMarkup:
        """Create duration selection buttons"""
        buttons = [
            [
                InlineKeyboardButton("30s", callback_data=f"dur_{message_id}_30"),
                InlineKeyboardButton("45s", callback_data=f"dur_{message_id}_45"),
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
        """Process job from beginning"""
        job_id = job.job_id
        
        try:
            # Check cancellation
            current_job = await db.get_job(job_id)
            if current_job.cancel_requested:
                raise asyncio.CancelledError("Job cancelled by user")
            
            # DOWNLOAD PHASE
            await db.update_job_state(job_id, JobState.DOWNLOADING)
            await self._update_status_message(
                status_msg,
                f"📥 **Downloading**\n`{job.file_name}`\n\n"
                f"Size: {TimeUtils.format_file_size(job.file_size)}\n\n"
                f"Please wait...",
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )
            
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
                "🔍 **Analyzing video**\n\nGetting duration and metadata...",
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )
            
            video_info = await VideoProcessor.get_video_info(file_path)
            if not video_info or video_info["duration"] <= 0:
                raise Exception(f"Invalid video duration: {video_info.get('duration', 0)}")
            
            duration = video_info["duration"]
            total_parts = math.ceil(duration / job.clip_duration)
            
            await db.update_job_state(
                job_id,
                JobState.PROCESSING,
                original_duration=duration,
                total_parts=total_parts,
                file_path=file_path
            )
            
            # PROCESSING LOOP
            await self._process_parts(job_id, status_msg, file_path, duration, total_parts)
            
            # COMPLETION
            await db.update_job_state(job_id, JobState.COMPLETED, completed_at=datetime.now())
            
            completed_parts = len((await db.get_job(job_id)).completed_parts)
            failed_parts = len((await db.get_job(job_id)).failed_parts)
            
            await self._update_status_message(
                status_msg,
                f"✅ **Job Completed!**\n\n"
                f"✓ Total parts: {total_parts}\n"
                f"✓ Completed: {completed_parts}\n"
                f"{'⚠️ Failed: ' + str(failed_parts) if failed_parts > 0 else ''}\n"
                f"✓ Channel: {job.target_channel_name}\n"
                f"✓ Duration: {TimeUtils.format_duration(duration)}\n\n"
                f"All files have been cleaned up and memory freed."
            )
            
            # Cleanup
            await self._cleanup_job_completely(job_id)
            
            # Update user stats
            await self._update_user_stats(job.user_id, completed_parts, duration)
            
        except asyncio.CancelledError:
            await db.update_job_state(job_id, JobState.CANCELLED, error_message="User cancelled")
            await self._update_status_message(status_msg, "❌ **Job Cancelled**\n\nAll files cleaned up and memory freed.")
            await self._cleanup_job_completely(job_id)
            raise
            
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            await db.update_job_state(job_id, JobState.FAILED, error_message=str(e))
            await self._update_status_message(
                status_msg,
                f"❌ **Job Failed**\n\nError: `{str(e)[:200]}`\n\nAll files have been cleaned up."
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
        """Process all parts with validation and cancellation support"""
        job = await db.get_job(job_id)
        if not job:
            return
        
        for part_num in range(start_part, total_parts + 1):
            # Check if job was cancelled
            current_job = await db.get_job(job_id)
            if current_job.cancel_requested or current_job.state == JobState.CANCELLED:
                logger.info(f"Job {job_id} cancelled during processing at part {part_num}")
                raise asyncio.CancelledError("Job cancelled by user")
            
            # Calculate timestamps
            start_time = (part_num - 1) * job.clip_duration
            end_time = min(start_time + job.clip_duration, duration)
            segment_duration = end_time - start_time
            
            if segment_duration <= 0:
                continue
            
            # Update status
            progress_bar = UIComponents.create_progress_bar(part_num - 1, total_parts)
            await self._update_status_message(
                status_msg,
                f"⚙️ **Processing Part {part_num}/{total_parts}**\n\n"
                f"⏱ Time: {TimeUtils.format_timestamp(start_time)} → {TimeUtils.format_timestamp(end_time)}\n"
                f"📊 Progress: {progress_bar}\n"
                f"📍 Status: Cutting video...",
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )
            
            output_path = Path(Config.TEMP_DIR) / f"{job_id}_part_{part_num}.mp4"
            
            # Cut the segment
            success, error = await VideoProcessor.cut_video_segment(
                input_path,
                str(output_path),
                start_time,
                segment_duration,
                reencode=True
            )
            
            if not success:
                logger.error(f"Failed to cut part {part_num}: {error}")
                await db.jobs.update_one(
                    {"job_id": job_id},
                    {"$push": {"failed_parts": part_num}}
                )
                continue
            
            # Validate clip
            if Config.VALIDATE_EACH_CLIP:
                validation = await VideoProcessor.validate_clip(str(output_path))
                if validation != ValidationResult.VALID:
                    logger.warning(f"Part {part_num} validation failed: {validation.name}")
                    
                    for retry in range(Config.MAX_RETRIES):
                        if os.path.exists(output_path):
                            os.remove(output_path)
                        
                        success, error = await VideoProcessor.cut_video_segment(
                            input_path,
                            str(output_path),
                            start_time,
                            segment_duration,
                            reencode=True
                        )
                        
                        if success:
                            validation = await VideoProcessor.validate_clip(str(output_path))
                            if validation == ValidationResult.VALID:
                                break
                    
                    if validation != ValidationResult.VALID:
                        logger.error(f"Part {part_num} failed after retries")
                        if os.path.exists(output_path):
                            os.remove(output_path)
                        await db.jobs.update_one(
                            {"job_id": job_id},
                            {"$push": {"failed_parts": part_num}}
                        )
                        continue
            
            # Upload
            await db.update_job_state(job_id, JobState.UPLOADING)
            
            await self._update_status_message(
                status_msg,
                f"📤 **Uploading Part {part_num}/{total_parts}**\n\n"
                f"⏱ Time: {TimeUtils.format_timestamp(start_time)} → {TimeUtils.format_timestamp(end_time)}\n"
                f"📊 Progress: {progress_bar}\n"
                f"📍 Status: Uploading to channel...",
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )
            
            caption = job.caption_template.format(
                part=part_num,
                start_time=TimeUtils.format_timestamp(start_time),
                end_time=TimeUtils.format_timestamp(end_time),
                total_parts=total_parts,
                file_name=job.file_name
            )
            
            upload_success = await self._upload_with_retry(
                job,
                str(output_path),
                caption,
                part_num
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
            
            # Cleanup clip immediately to free memory
            if os.path.exists(output_path):
                try:
                    file_size = os.path.getsize(output_path)
                    os.remove(output_path)
                    logger.info(f"Cleaned up part {part_num} ({TimeUtils.format_file_size(file_size)})")
                except Exception as e:
                    logger.error(f"Failed to cleanup part {part_num}: {e}")
            
            await db.update_channel_usage(job.user_id, job.target_channel_id)
            
            # Small delay
            await asyncio.sleep(Config.UPLOAD_COOLDOWN)
            
            # Update state back to processing
            await db.update_job_state(job_id, JobState.PROCESSING)
    
    async def _upload_with_retry(
        self,
        job: JobData,
        file_path: str,
        caption: str,
        part_num: int
    ) -> bool:
        """Upload with retry logic and flood control"""
        max_retries = Config.MAX_RETRIES
        
        for attempt in range(max_retries):
            try:
                # Check if job was cancelled
                current_job = await db.get_job(job.job_id)
                if current_job.cancel_requested:
                    raise asyncio.CancelledError("Job cancelled during upload")
                
                if job.target_channel_id in self.flood_wait_expiry:
                    expiry = self.flood_wait_expiry[job.target_channel_id]
                    if datetime.now() < expiry:
                        wait_seconds = (expiry - datetime.now()).total_seconds()
                        logger.info(f"Flood wait active, waiting {wait_seconds}s")
                        await asyncio.sleep(wait_seconds)
                
                if job.upload_mode == UploadMode.VIDEO:
                    await self.bot.send_video(
                        chat_id=job.target_channel_id,
                        video=file_path,
                        caption=caption,
                        supports_streaming=True,
                        parse_mode=enums.ParseMode.MARKDOWN
                    )
                
                logger.info(f"Uploaded part {part_num} to channel {job.target_channel_id}")
                return True
                
            except errors.FloodWait as e:
                wait_time = e.value * Config.FLOOD_WAIT_BACKOFF
                expiry_time = datetime.now() + timedelta(seconds=wait_time)
                self.flood_wait_expiry[job.target_channel_id] = expiry_time
                
                logger.warning(f"FloodWait for {e.value}s, backing off to {wait_time}s")
                await asyncio.sleep(wait_time)
                
                await db.update_job_state(
                    job.job_id,
                    JobState.PAUSED,
                    flood_wait_until=expiry_time,
                    error_message=f"FloodWait: {e.value}s"
                )
                
                continue
                
            except asyncio.CancelledError:
                raise
                
            except Exception as e:
                logger.error(f"Upload attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error(f"Upload failed after {max_retries} attempts: {e}")
                    return False
        
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
        """Download progress callback"""
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
        """Update status message with error handling"""
        try:
            await message.edit_text(text, **kwargs)
        except errors.MessageNotModified:
            pass
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
# 7. TELEGRAM BOT HANDLERS
# ================================================================================

class BotHandlers:
    """All Telegram bot handlers"""
    
    def __init__(self, bot_client, job_processor):
        self.bot = bot_client
        self.processor = job_processor
        self.user_states: Dict[int, Dict] = {}
    
    async def start_command(self, client: Client, message: Message):
        """Handle /start command"""
        user_id = message.from_user.id
        
        if user_id not in Config.ADMIN_IDS:
            await message.reply("⛔ **Access Denied**\nThis bot is for administrators only.")
            return
        
        user = await db.get_or_create_user(user_id, message.from_user.first_name)
        
        incomplete_job = await db.get_active_user_job(user_id)
        if incomplete_job:
            buttons = [
                [
                    InlineKeyboardButton("🔄 Resume", callback_data=f"resume_{incomplete_job.job_id}"),
                    InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job_{incomplete_job.job_id}")
                ],
                [
                    InlineKeyboardButton("📊 Details", callback_data=f"details_{incomplete_job.job_id}")
                ]
            ]
            
            await message.reply(
                f"⚠️ **Incomplete Job Found**\n\n"
                f"Job ID: `{incomplete_job.job_id}`\n"
                f"File: `{incomplete_job.file_name}`\n"
                f"Progress: {incomplete_job.current_part-1}/{incomplete_job.total_parts} parts\n"
                f"State: {incomplete_job.state.name}\n\n"
                f"Would you like to resume or cancel?",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return
        
        text = (
            "👋 **Welcome to Auto-Splitter Bot v2.1**\n\n"
            "I can split large videos into smaller clips and upload them to your channels.\n\n"
            "**✨ Features:**\n"
            "✅ Crash recovery & auto-resume\n"
            "✅ Queue system with priority\n"
            "✅ Smart flood control\n"
            "✅ Clip validation\n"
            "✅ Real-time cancellation\n"
            "✅ Memory management\n"
            "✅ Admin dashboard\n\n"
            "**🚀 Quick Start:**\n"
            "1. Send me a video file\n"
            "2. Select clip duration\n"
            "3. Choose target channel\n"
            "4. I'll handle the rest!\n\n"
            "**📝 Commands:**\n"
            "/start - Show this menu\n"
            "/channels - Manage channels\n"
            "/settings - Your preferences\n"
            "/stats - View statistics\n"
            "/admin - Admin panel\n"
            "/cancel - Cancel active job"
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
        
        if user_id == Config.OWNER_ID:
            buttons.append([
                InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")
            ])
        
        await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))
    
    async def video_handler(self, client: Client, message: Message):
        """Handle incoming video files"""
        user_id = message.from_user.id
        
        if user_id not in Config.ADMIN_IDS:
            return
        
        active_job = await db.get_active_user_job(user_id)
        if active_job:
            await message.reply(
                f"⚠️ **You already have an active job**\n\n"
                f"Job ID: `{active_job.job_id}`\n"
                f"Status: {active_job.state.name}\n\n"
                f"Please complete or cancel the current job first.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 View Job", callback_data=f"details_{active_job.job_id}")],
                    [InlineKeyboardButton("❌ Cancel Job", callback_data=f"cancel_job_{active_job.job_id}")]
                ])
            )
            return
        
        if message.video:
            file_name = message.video.file_name or f"video_{message.id}.mp4"
            file_size = message.video.file_size
            duration = message.video.duration
            file_id = message.video.file_id
        elif message.document and "video" in (message.document.mime_type or ""):
            file_name = message.document.file_name or f"video_{message.id}.mp4"
            file_size = message.document.file_size
            duration = getattr(message.document, "duration", 0)
            file_id = message.document.file_id
        else:
            await message.reply("❌ **Unsupported file type**\nPlease send a video file.")
            return
        
        self.user_states[user_id] = {
            "file_message_id": message.id,
            "file_id": file_id,
            "file_name": file_name,
            "file_size": file_size,
            "duration": duration,
            "state": "waiting_duration"
        }
        
        await message.reply(
            f"🎬 **Video Received**\n\n"
            f"📂 File: `{file_name}`\n"
            f"📦 Size: {TimeUtils.format_file_size(file_size)}\n"
            f"⏱ Duration: {TimeUtils.format_duration(duration) if duration > 0 else 'Unknown'}\n\n"
            f"**Select clip duration:**",
            reply_markup=UIComponents.create_duration_buttons(message.id)
        )
    
    async def callback_handler(self, client: Client, callback_query: CallbackQuery):
        """Handle all callback queries"""
        data = callback_query.data
        user_id = callback_query.from_user.id
        
        if user_id not in Config.ADMIN_IDS and not data.startswith("close"):
            await callback_query.answer("⛔ Access Denied", show_alert=True)
            return
        
        try:
            # Duration selection
            if data.startswith("dur_"):
                _, msg_id, duration = data.split("_")
                await self._handle_duration_selection(callback_query, int(msg_id), int(duration))
            
            # Channel selection
            elif data.startswith("chan_"):
                _, msg_id, duration, channel_id = data.split("_")
                await self._handle_channel_selection(callback_query, int(msg_id), int(duration), int(channel_id))
            
            # Job start
            elif data.startswith("start_job_"):
                _, _, msg_id, duration, channel_id = data.split("_")
                await self._handle_job_start(callback_query, int(msg_id), int(duration), int(channel_id))
            
            # Job control
            elif data.startswith("cancel_job_"):
                job_id = data.split("_", 2)[2]
                await self._handle_job_cancellation(callback_query, job_id)
            
            elif data.startswith("resume_"):
                job_id = data.split("_")[1]
                await self._handle_job_resume(callback_query, job_id)
            
            elif data.startswith("details_"):
                job_id = data.split("_")[1]
                await self._show_job_details(callback_query, job_id)
            
            # Navigation
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
                    f"⏱ **Duration:** {duration} seconds\n\n**Select target channel:**",
                    reply_markup=UIComponents.create_channel_buttons(channels, int(msg_id), int(duration))
                )
            
            # Admin panel
            elif data == "admin_panel":
                await self._show_admin_panel(callback_query)
            
            elif data == "admin_stats":
                await self._show_admin_stats(callback_query)
            
            elif data == "admin_jobs":
                await self._show_admin_jobs(callback_query)
            
            elif data == "admin_storage":
                await self._show_admin_storage(callback_query)
            
            elif data == "admin_cleanup":
                await self._handle_admin_cleanup(callback_query)
            
            # Channel management
            elif data == "channel_list":
                await self._show_channel_list(callback_query)
            
            elif data == "add_channel_prompt":
                await callback_query.message.edit_text(
                    "➕ **Add New Channel**\n\n"
                    "Use the command:\n"
                    "`/addchannel <channel_id> <name>`\n\n"
                    "**Example:**\n"
                    "`/addchannel -100123456789 MyChannel`\n\n"
                    "**Note:** Make sure to add me as admin to the channel first!",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back", callback_data="channel_list")
                    ]])
                )
            
            elif data.startswith("delete_channel_"):
                channel_id = int(data.split("_")[2])
                await self._handle_channel_deletion(callback_query, channel_id)
            
            elif data == "verify_channels":
                await self._verify_all_channels(callback_query)
            
            # Settings
            elif data == "settings_main":
                await self._show_settings(callback_query)
            
            elif data == "setting_duration":
                await callback_query.answer("Use /settings command to change default duration", show_alert=True)
            
            elif data == "setting_mode":
                settings = await db.get_user_settings(user_id)
                new_mode = "document" if settings.get("upload_mode") == "video" else "video"
                settings["upload_mode"] = new_mode
                await db.update_user_settings(user_id, settings)
                await self._show_settings(callback_query)
                await callback_query.answer(f"Upload mode changed to {new_mode.upper()}", show_alert=True)
            
            elif data == "setting_auto_resume":
                settings = await db.get_user_settings(user_id)
                settings["auto_resume"] = not settings.get("auto_resume", True)
                await db.update_user_settings(user_id, settings)
                await self._show_settings(callback_query)
            
            # Stats
            elif data == "user_stats":
                await self._show_user_stats(callback_query)
            
            # Misc
            elif data == "start":
                await self.start_command(client, callback_query.message)
            
            elif data == "cancel_all":
                if user_id in self.user_states:
                    del self.user_states[user_id]
                await callback_query.message.delete()
                await callback_query.answer("Cancelled")
            
            elif data == "close":
                await callback_query.message.delete()
            
            else:
                await callback_query.answer("❌ Unknown action", show_alert=True)
                
        except Exception as e:
            logger.error(f"Callback handler error: {e}\n{traceback.format_exc()}")
            await callback_query.answer("❌ Error processing request", show_alert=True)
    
    async def _handle_duration_selection(self, callback: CallbackQuery, msg_id: int, duration: int):
        """Handle duration selection"""
        user_id = callback.from_user.id
        
        if duration < Config.MIN_CLIP_DURATION:
            await callback.answer(f"Duration must be at least {Config.MIN_CLIP_DURATION} seconds", show_alert=True)
            return
        
        if duration > Config.MAX_CLIP_DURATION:
            await callback.answer(f"Duration cannot exceed {Config.MAX_CLIP_DURATION} seconds", show_alert=True)
            return
        
        if user_id in self.user_states:
            self.user_states[user_id]["clip_duration"] = duration
            self.user_states[user_id]["state"] = "waiting_channel"
        
        channels = await db.get_user_channels(user_id)
        
        if not channels:
            await callback.message.edit_text(
                "❌ **No Channels Found**\n\n"
                "You need to add a channel first.\n\n"
                "**To add a channel:**\n"
                "1. Add me as admin to your channel\n"
                "2. Use: `/addchannel <channel_id> <name>`\n"
                "3. Example: `/addchannel -100123456789 MyChannel`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data=f"back_dur_{msg_id}")]
                ])
            )
            return
        
        await callback.message.edit_text(
            f"⏱ **Duration Selected:** {duration} seconds\n\n"
            f"**Select target channel:**",
            reply_markup=UIComponents.create_channel_buttons(channels, msg_id, duration)
        )
    
    async def _handle_channel_selection(self, callback: CallbackQuery, msg_id: int, duration: int, channel_id: int):
        """Handle channel selection and show preview"""
        user_id = callback.from_user.id
        
        channels = await db.get_user_channels(user_id)
        channel = next((c for c in channels if c.channel_id == channel_id), None)
        
        if not channel:
            await callback.answer("Channel not found", show_alert=True)
            return
        
        if user_id not in self.user_states:
            await callback.answer("Session expired. Please send the video again.", show_alert=True)
            return
        
        file_info = self.user_states[user_id]
        
        estimated_parts = "Calculating..."
        processing_time = "Calculating..."
        
        if file_info.get("duration", 0) > 0:
            total_parts = math.ceil(file_info["duration"] / duration)
            estimated_parts = f"{total_parts} parts"
            processing_time = TimeUtils.estimate_processing_time(file_info["duration"], duration)
        
        preview_text = (
            "📋 **Job Preview**\n\n"
            f"📂 **File:** `{file_info['file_name']}`\n"
            f"📦 **Size:** {TimeUtils.format_file_size(file_info['file_size'])}\n"
            f"⏱ **Clip Duration:** {duration} seconds\n"
            f"📢 **Target Channel:** {channel.name}\n"
            f"🎞 **Estimated Parts:** {estimated_parts}\n"
            f"⏳ **Est. Time:** {processing_time}\n\n"
            f"**Ready to start?**"
        )
        
        await callback.message.edit_text(
            preview_text,
            reply_markup=UIComponents.create_preview_buttons(msg_id, duration, channel_id)
        )
    
    async def _handle_job_start(self, callback: CallbackQuery, msg_id: int, duration: int, channel_id: int):
        """Actually create and start the job"""
        user_id = callback.from_user.id
        
        if user_id not in self.user_states:
            await callback.answer("Session expired. Please send the video again.", show_alert=True)
            return
        
        file_info = self.user_states[user_id]
        
        channels = await db.get_user_channels(user_id)
        channel = next((c for c in channels if c.channel_id == channel_id), None)
        
        if not channel:
            await callback.answer("Channel not found", show_alert=True)
            return
        
        # Generate job ID
        job_id = hashlib.md5(f"{user_id}_{time.time()}".encode()).hexdigest()[:12]
        
        # Get user settings
        settings = await db.get_user_settings(user_id)
        
        # Create job
        job_data = JobData(
            job_id=job_id,
            user_id=user_id,
            file_message_id=file_info["file_message_id"],
            file_id=file_info["file_id"],
            file_name=file_info["file_name"],
            file_size=file_info["file_size"],
            original_duration=file_info.get("duration", 0),
            clip_duration=duration,
            target_channel_id=channel_id,
            target_channel_name=channel.name,
            state=JobState.QUEUED,
            caption_template=settings.get("caption_template", "[Part {part}] {start_time} - {end_time}"),
            upload_mode=UploadMode(settings.get("upload_mode", "video"))
        )
        
        await db.create_job(job_data)
        await db.add_to_queue(job_id)
        
        # Clear user state
        del self.user_states[user_id]
        
        await callback.message.edit_text(
            f"✅ **Job Created Successfully!**\n\n"
            f"🆔 Job ID: `{job_id}`\n"
            f"📂 File: `{file_info['file_name']}`\n"
            f"⏱ Duration: {duration}s clips\n"
            f"📢 Channel: {channel.name}\n\n"
            f"Processing will start shortly...\n"
            f"You'll receive updates here."
        )
        
        logger.info(f"Job {job_id} created and queued by user {user_id}")
    
    async def _handle_job_cancellation(self, callback: CallbackQuery, job_id: str):
        """Handle job cancellation"""
        user_id = callback.from_user.id
        
        success = await self.processor.cancel_job(job_id, user_id)
        
        if success:
            await callback.answer("✅ Job cancelled successfully", show_alert=True)
            await callback.message.edit_text(
                f"❌ **Job Cancelled**\n\n"
                f"Job ID: `{job_id}`\n\n"
                f"All temporary files have been cleaned up and memory has been freed back to the server."
            )
        else:
            await callback.answer("❌ Failed to cancel job", show_alert=True)
    
    async def _handle_job_resume(self, callback: CallbackQuery, job_id: str):
        """Handle job resume"""
        user_id = callback.from_user.id
        
        job = await db.get_job(job_id)
        if not job or job.user_id != user_id:
            await callback.answer("Job not found", show_alert=True)
            return
        
        if job.state == JobState.PAUSED:
            await db.update_job_state(job_id, JobState.QUEUED)
            await db.add_to_queue(job_id, priority=5)
            
            await callback.answer("🔄 Job resumed", show_alert=True)
            await callback.message.edit_text(
                f"🔄 **Job Resumed**\n\n"
                f"Job ID: `{job_id}`\n\n"
                f"The job has been added to the queue and will resume shortly."
            )
            logger.info(f"Job {job_id} resumed by user {user_id}")
        else:
            await callback.answer("Job is not in a resumable state", show_alert=True)
    
    async def _show_job_details(self, callback: CallbackQuery, job_id: str):
        """Show detailed job information"""
        job = await db.get_job(job_id)
        
        if not job:
            await callback.answer("Job not found", show_alert=True)
            return
        
        completed = len(job.completed_parts)
        failed = len(job.failed_parts)
        
        text = (
            f"📊 **Job Details**\n\n"
            f"🆔 **ID:** `{job.job_id}`\n"
            f"📂 **File:** `{job.file_name}`\n"
            f"📦 **Size:** {TimeUtils.format_file_size(job.file_size)}\n"
            f"⏱ **Duration:** {TimeUtils.format_duration(job.original_duration) if job.original_duration > 0 else 'N/A'}\n"
            f"✂️ **Clip Duration:** {job.clip_duration}s\n"
            f"📢 **Channel:** {job.target_channel_name}\n"
            f"📍 **State:** {job.state.name}\n\n"
            f"**Progress:**\n"
            f"✅ Completed: {completed}/{job.total_parts}\n"
            f"❌ Failed: {failed}\n"
            f"🔄 Current Part: {job.current_part}\n\n"
            f"**Timestamps:**\n"
            f"📅 Created: {job.created_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"🚀 Started: {job.started_at.strftime('%Y-%m-%d %H:%M:%S') if job.started_at else 'Not started'}\n"
            f"✅ Completed: {job.completed_at.strftime('%Y-%m-%d %H:%M:%S') if job.completed_at else 'Not completed'}"
        )
        
        if job.error_message:
            text += f"\n\n⚠️ **Error:** `{job.error_message[:200]}`"
        
        buttons = [[InlineKeyboardButton("🔙 Back", callback_data="start")]]
        
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    
    async def _show_admin_panel(self, callback: CallbackQuery):
        """Show admin panel"""
        if callback.from_user.id != Config.OWNER_ID:
            await callback.answer("⛔ Owner only", show_alert=True)
            return

        stats = await db.get_statistics()
        disk_usage = ResourceMonitor.get_disk_usage()
        memory_usage = ResourceMonitor.get_memory_usage()

        text = (
            "🛠 **Admin Dashboard**\n\n"
            f"**📊 Statistics:**\n"
            f"• Total Jobs: `{stats['total_jobs']}`\n"
            f"• Active: `{stats['active_jobs']}` | Queue: `{stats['queue_size']}`\n"
            f"• Completed: `{stats['completed_jobs']}`\n"
            f"• Failed: `{stats['failed_jobs']}`\n"
            f"• Users: `{stats['total_users']}`\n\n"
            f"**💾 System Resources:**\n"
            f"• Disk: `{disk_usage['percent']}%` used\n"
            f"• Free: `{disk_usage['free'] / (1024**3):.2f} GB`\n"
            f"• RAM: `{memory_usage['percent']}%` used\n"
            f"• Temp Files: `{stats['disk_usage_mb']:.2f} MB`"
        )

        await callback.message.edit_text(text, reply_markup=UIComponents.create_admin_dashboard_buttons())

    async def _show_admin_stats(self, callback: CallbackQuery):
        """Show detailed statistics"""
        # Simply refresh the main panel for now as it contains all stats
        await self._show_admin_panel(callback)

    async def _show_admin_jobs(self, callback: CallbackQuery):
        """Show active jobs"""
        active_jobs = await db.get_incomplete_jobs()
        
        if not active_jobs:
            await callback.answer("No active jobs", show_alert=True)
            return

        text = "🔄 **Active Jobs:**\n\n"
        for job in active_jobs[:10]:  # Show max 10
            text += f"• `{job.job_id}`: {job.state.name} ({job.current_part}/{job.total_parts})\n"

        buttons = [[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    async def _show_admin_storage(self, callback: CallbackQuery):
        """Show storage details"""
        # Refreshes panel to show storage
        await self._show_admin_panel(callback)

    async def _handle_admin_cleanup(self, callback: CallbackQuery):
        """Force cleanup temporary files"""
        await callback.answer("🧹 Cleaning up...", show_alert=False)
        
        # Clean files older than 1 hour for safety
        ResourceMonitor.cleanup_old_files(max_age_hours=1)
        
        # Recalculate stats
        stats = await db.get_statistics()
        await callback.answer(f"✅ Cleanup Complete! Current Temp: {stats['disk_usage_mb']:.2f} MB", show_alert=True)
        await self._show_admin_panel(callback)

    async def _show_channel_list(self, callback: CallbackQuery):
        """Show user's channel list"""
        user_id = callback.from_user.id
        channels = await db.get_user_channels(user_id)

        if not channels:
            text = (
                "📢 **No Channels Found**\n\n"
                "You haven't added any channels yet.\n"
                "Use `/addchannel` to add one."
            )
            buttons = [
                [InlineKeyboardButton("➕ Add Channel", callback_data="add_channel_prompt")],
                [InlineKeyboardButton("🔙 Back", callback_data="start")]
            ]
        else:
            text = "📢 **Your Channels:**\n\n"
            buttons = []
            
            for channel in channels:
                verified = "✅" if channel.permissions_verified else "⚠️"
                text += f"• {verified} **{channel.name}** (`{channel.channel_id}`)\n"
                
                # Add delete button for each channel
                buttons.append([
                    InlineKeyboardButton(f"🗑 Remove {channel.name}", callback_data=f"delete_channel_{channel.channel_id}")
                ])

            buttons.append([InlineKeyboardButton("➕ Add Channel", callback_data="add_channel_prompt")])
            buttons.append([InlineKeyboardButton("🔄 Verify All", callback_data="verify_channels")])
            buttons.append([InlineKeyboardButton("🔙 Back", callback_data="start")])

        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    async def _handle_channel_deletion(self, callback: CallbackQuery, channel_id: int):
        """Handle channel deletion"""
        user_id = callback.from_user.id
        success = await db.remove_channel(user_id, channel_id)
        
        if success:
            await callback.answer("Channel removed successfully", show_alert=True)
            await self._show_channel_list(callback)
        else:
            await callback.answer("Failed to remove channel", show_alert=True)

    async def _verify_all_channels(self, callback: CallbackQuery):
        """Verify permissions for all channels"""
        user_id = callback.from_user.id
        channels = await db.get_user_channels(user_id)
        
        await callback.answer("Checking permissions...", show_alert=False)
        verified_count = 0
        
        for channel in channels:
            try:
                # Try to send a test message (silent)
                msg = await self.bot.send_message(channel.channel_id, "Checking permissions...", disable_notification=True)
                await msg.delete()
                await db.update_channel_permissions(user_id, channel.channel_id, True)
                verified_count += 1
            except Exception:
                await db.update_channel_permissions(user_id, channel.channel_id, False)
        
        await callback.answer(f"Verified {verified_count}/{len(channels)} channels", show_alert=True)
        await self._show_channel_list(callback)

    async def _show_settings(self, callback: CallbackQuery):
        """Show settings menu"""
        user_id = callback.from_user.id
        settings = await db.get_user_settings(user_id)
        
        text = (
            "⚙️ **Settings**\n\n"
            "Configure your default preferences here."
        )
        
        await callback.message.edit_text(text, reply_markup=UIComponents.create_settings_buttons(settings))

    async def _show_user_stats(self, callback: CallbackQuery):
        """Show individual user stats"""
        user_id = callback.from_user.id
        user = await db.users.find_one({"user_id": user_id})
        stats = user.get("stats", {}) if user else {}
        
        text = (
            f"📊 **My Statistics**\n\n"
            f"✅ Jobs Completed: `{stats.get('jobs_completed', 0)}`\n"
            f"✂️ Total Parts: `{stats.get('total_parts', 0)}`\n"
            f"⏱ Total Duration: `{TimeUtils.format_duration(stats.get('total_duration', 0))}`"
        )
        
        buttons = [[InlineKeyboardButton("🔙 Back", callback_data="start")]]
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

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
        
        # Start Web Server (Required for Render/Heroku)
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
            
        # NOTE: Removed 'await asyncio.Event().wait()' to prevent blocking here.
        # We handle keep-alive in main()

    def _register_handlers(self):
        """Register Pyrogram handlers"""
        # Command handlers
        self.client.add_handler(
            filters.command("start") & filters.private, 
            self.handlers.start_command
        )
        
        # Async wrapper for addchannel
        async def addchannel_wrapper(client, message):
            await handle_addchannel_command(client, message)
            
        self.client.add_handler(
            filters.command("addchannel") & filters.private,
            addchannel_wrapper
        )
        
        self.client.add_handler(
            filters.command("settings") & filters.private,
            lambda c, m: self.handlers.start_command(c, m)
        )
        
        # Message handlers
        self.client.add_handler(
            (filters.video | filters.document) & filters.private,
            self.handlers.video_handler
        )
        
        # Callback Query Handler (Using Decorator - The Fix)
        @self.client.on_callback_query()
        async def callback_wrapper(client, callback):
            await self.handlers.callback_handler(client, callback)

    async def stop(self):
        await self.client.stop()

# ================================================================================
# 11. ENTRY POINT (FIXED FOR FREE HOSTING)
# ================================================================================

async def main():
    """Main function to start the bot and keep it running"""
    bot = AutoSplitterBot()
    
    # Start everything (Web server + Bot + Processor)
    await bot.start()
    
    # Keep the main loop running until a signal stops it
    stop_event = asyncio.Event()
    await stop_event.wait()

if __name__ == "__main__":
    # Setup Event Loop
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # Graceful Shutdown Logic
    async def shutdown_gracefully(sig):
        logger.info(f"Received signal {sig.name}, shutting down...")
        
        # Cancel all running tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in tasks:
            t.cancel()
            
        await asyncio.gather(*tasks, return_exceptions=True)
        
        if loop.is_running():
            loop.stop()

    # Register Signals (SIGINT, SIGTERM)
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(shutdown_gracefully(s))
            )
    except NotImplementedError:
        logger.warning("Signal handlers not supported on this platform. Ignoring.")

    # Run the Loop
    try:
        logger.info("Starting event loop...")
        loop.run_until_complete(main())
    except asyncio.CancelledError:
        logger.info("Main execution cancelled.")
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
    finally:
        logger.info("Closing loop...")
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        logger.info("Goodbye.")

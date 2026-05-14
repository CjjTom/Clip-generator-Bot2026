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

class UserRole(Enum):
    """Explicit role levels for the permission layer"""
    OWNER = "owner"
    ADMIN = "admin"
    USER  = "user"

@dataclass
class JobData:
    """Complete job data model — series fields added"""
    job_id: str
    user_id: int
    file_message_id: int
    file_id: str
    file_name: str
    file_size: int
    original_duration: float = 0.0
    start_time: float = 0.0
    end_time: float = 0.0
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
    # --- SERIES CONTINUATION FIELDS ---
    # part_offset changes only the DISPLAY number on the caption.
    # The actual ffmpeg cut positions are never affected.
    series_name: str = ""
    episode_number: int = 0
    part_offset: int = 1          # Caption shows: part_offset + (loop_index - 1)
    previous_job_id: Optional[str] = None
    continue_from_last: bool = False
    # ------------------------------------

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
    """
    Bot configuration.
    OWNER_ID and BOOTSTRAP_ADMIN_IDS come from env vars.
    After first startup these are written into the DB (roles collection).
    All runtime permission checks use the DB, not these env values directly.
    """
    API_ID      = int(os.environ.get("API_ID", 0))
    API_HASH    = os.environ.get("API_HASH", "")
    BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")

    DATABASE_URL  = os.environ.get("DATABASE_URL", "")
    DATABASE_NAME = os.environ.get("DATABASE_NAME", "AutoVideoClipGenerate")

    # The one fixed owner — always loaded from env, never changed by the bot.
    OWNER_ID = int(os.environ.get("OWNER_ID", 0))

    # These IDs are only used ONCE at first boot to seed the DB.
    # After that, add/remove admins through the bot itself.
    _admin_list = os.environ.get("ADMIN_IDS", "").split(",")
    BOOTSTRAP_ADMIN_IDS = [int(x.strip()) for x in _admin_list if x.strip().isdigit()]
    if OWNER_ID and OWNER_ID not in BOOTSTRAP_ADMIN_IDS:
        BOOTSTRAP_ADMIN_IDS.append(OWNER_ID)

    # Paths
    WORK_DIR = os.environ.get("WORK_DIR", "downloads")
    LOG_DIR  = os.environ.get("LOG_DIR",  "logs")
    TEMP_DIR = os.environ.get("TEMP_DIR", "temp")

    # Limits & Settings
    MAX_CONCURRENT_JOBS    = int(os.environ.get("MAX_CONCURRENT_JOBS", 1))
    MAX_QUEUE_SIZE         = int(os.environ.get("MAX_QUEUE_SIZE", 10))
    MAX_CLIP_DURATION      = int(os.environ.get("MAX_CLIP_DURATION", 300))
    MIN_CLIP_DURATION      = int(os.environ.get("MIN_CLIP_DURATION", 10))
    MIN_CLIP_SIZE          = int(os.environ.get("MIN_CLIP_SIZE", 10240))
    MAX_RETRIES            = int(os.environ.get("MAX_RETRIES", 3))
    VALIDATE_EACH_CLIP     = os.environ.get("VALIDATE_EACH_CLIP", "true").lower() == "true"
    FLOOD_WAIT_BACKOFF     = int(os.environ.get("FLOOD_WAIT_BACKOFF", 2))
    MAX_FLOOD_WAIT         = int(os.environ.get("MAX_FLOOD_WAIT", 3600))
    UPLOAD_COOLDOWN        = float(os.environ.get("UPLOAD_COOLDOWN", 1.0))
    FFMPEG_PRESET          = os.environ.get("FFMPEG_PRESET", "fast")
    AUTO_CLEANUP_HOURS     = int(os.environ.get("AUTO_CLEANUP_HOURS", 24))
    MAX_DISK_USAGE_PERCENT = int(os.environ.get("MAX_DISK_USAGE_PERCENT", 90))

    # Web Server
    WEB_HOST = os.environ.get("WEB_HOST", "0.0.0.0")
    WEB_PORT = int(os.environ.get("PORT", 8080))

    # Logging
    LOG_LEVEL      = os.environ.get("LOG_LEVEL", "INFO")
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

        console_handler = logging.StreamHandler()
        console_format  = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_format)

        log_file     = Path(Config.LOG_DIR) / f"autosplitter_{datetime.now():%Y%m%d}.log"
        file_handler = logging.FileHandler(log_file)
        file_format  = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        file_handler.setFormatter(file_format)

        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        self.logger.setLevel(getattr(logging, Config.LOG_LEVEL))

        self.log_channel = None
        self.bot_client  = None

    def set_bot_client(self, client):
        self.bot_client = client

    async def log_to_telegram(self, level: str, message: str):
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
    """
    Complete database management.
    New collections: roles, action_logs
    New methods: bootstrap_roles, role management, series state, admin action logging
    """

    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(Config.DATABASE_URL)
        self.db     = self.client[Config.DATABASE_NAME]

        self.users:       AgnosticCollection = self.db.users
        self.jobs:        AgnosticCollection = self.db.jobs
        self.channels:    AgnosticCollection = self.db.channels
        self.settings:    AgnosticCollection = self.db.settings
        self.queue:       AgnosticCollection = self.db.queue
        self.logs:        AgnosticCollection = self.db.logs
        self.roles:       AgnosticCollection = self.db.roles        # NEW
        self.action_logs: AgnosticCollection = self.db.action_logs  # NEW

        self.indexes_created = False

    # ------------------------------------------------------------------
    # INDEX CREATION
    # ------------------------------------------------------------------

    async def create_indexes(self):
        """Create all database indexes"""
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

        await self.roles.create_index([("user_id", 1)], unique=True)
        await self.action_logs.create_index([("timestamp", -1)])
        await self.action_logs.create_index([("admin_id", 1)])

        self.indexes_created = True
        logger.info("Database indexes created")

    # ------------------------------------------------------------------
    # ROLE BOOTSTRAP (runs once on first start)
    # ------------------------------------------------------------------

    async def bootstrap_roles(self):
        """
        Write env-defined IDs into the DB roles collection if they do not exist yet.
        After this point, env ADMIN_IDS are never read again at runtime.
        Owner is always written with role=owner.
        Bootstrap admins are written with role=admin.
        """
        # Owner
        existing_owner = await self.roles.find_one({"user_id": Config.OWNER_ID})
        if not existing_owner:
            await self.roles.insert_one({
                "user_id":   Config.OWNER_ID,
                "role":      UserRole.OWNER.value,
                "active":    True,
                "added_by":  Config.OWNER_ID,
                "added_at":  datetime.now(),
                "notes":     "System bootstrap — owner"
            })
            logger.info(f"Owner {Config.OWNER_ID} bootstrapped into roles DB")
        else:
            # Always keep owner role correct even if DB had wrong value
            await self.roles.update_one(
                {"user_id": Config.OWNER_ID},
                {"$set": {"role": UserRole.OWNER.value, "active": True}}
            )

        # Bootstrap admins from env
        for admin_id in Config.BOOTSTRAP_ADMIN_IDS:
            if admin_id == Config.OWNER_ID:
                continue
            existing = await self.roles.find_one({"user_id": admin_id})
            if not existing:
                await self.roles.insert_one({
                    "user_id":  admin_id,
                    "role":     UserRole.ADMIN.value,
                    "active":   True,
                    "added_by": Config.OWNER_ID,
                    "added_at": datetime.now(),
                    "notes":    "Bootstrap from ADMIN_IDS env var"
                })
                logger.info(f"Admin {admin_id} bootstrapped into roles DB")

    # ------------------------------------------------------------------
    # ROLE MANAGEMENT
    # ------------------------------------------------------------------

    async def get_role(self, user_id: int) -> Optional[Dict]:
        """Get active role document for a user"""
        return await self.roles.find_one({"user_id": user_id, "active": True})

    async def add_role(self, user_id: int, role: str, added_by: int, notes: str = "") -> bool:
        """Add or update a role. Logs the action."""
        try:
            await self.roles.update_one(
                {"user_id": user_id},
                {"$set": {
                    "user_id":  user_id,
                    "role":     role,
                    "active":   True,
                    "added_by": added_by,
                    "added_at": datetime.now(),
                    "notes":    notes
                }},
                upsert=True
            )
            await self._log_action(added_by, "add_role", user_id, f"role={role} notes={notes}")
            logger.info(f"Role {role} added for user {user_id} by {added_by}")
            return True
        except Exception as e:
            logger.error(f"Failed to add role: {e}")
            return False

    async def revoke_role(self, user_id: int, revoked_by: int, reason: str = "") -> bool:
        """Deactivate a user's role. Owner cannot be revoked."""
        if user_id == Config.OWNER_ID:
            logger.warning(f"Attempt to revoke owner {user_id} by {revoked_by} — blocked")
            return False
        try:
            result = await self.roles.update_one(
                {"user_id": user_id},
                {"$set": {
                    "active":     False,
                    "revoked_by": revoked_by,
                    "revoked_at": datetime.now(),
                    "revoke_reason": reason
                }}
            )
            if result.modified_count > 0:
                await self._log_action(revoked_by, "revoke_role", user_id, f"reason={reason}")
                logger.info(f"Role revoked for user {user_id} by {revoked_by}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to revoke role: {e}")
            return False

    async def get_all_active_roles(self) -> List[Dict]:
        """Get all active role documents"""
        cursor = self.roles.find({"active": True}).sort("role", 1)
        roles = []
        async for doc in cursor:
            roles.append(doc)
        return roles

    async def get_roles_by_type(self, role: str) -> List[Dict]:
        """Get all active roles of a specific type"""
        cursor = self.roles.find({"active": True, "role": role})
        roles = []
        async for doc in cursor:
            roles.append(doc)
        return roles

    # ------------------------------------------------------------------
    # ADMIN ACTION LOGGING
    # ------------------------------------------------------------------

    async def _log_action(self, admin_id: int, action: str,
                           target_id: int = None, details: str = ""):
        """Write an admin action audit entry"""
        try:
            await self.action_logs.insert_one({
                "admin_id":  admin_id,
                "action":    action,
                "target_id": target_id,
                "details":   details,
                "timestamp": datetime.now()
            })
        except Exception as e:
            logger.error(f"Failed to write action log: {e}")

    async def get_recent_action_logs(self, limit: int = 20) -> List[Dict]:
        """Get most recent admin action log entries"""
        cursor = self.action_logs.find({}).sort("timestamp", -1).limit(limit)
        logs = []
        async for doc in cursor:
            logs.append(doc)
        return logs

    # ------------------------------------------------------------------
    # SERIES / CONTINUATION STATE
    # ------------------------------------------------------------------

    async def get_user_series_state(self, user_id: int) -> Dict:
        """Get the stored series continuation state for a user"""
        user = await self.users.find_one({"user_id": user_id})
        if user and "series_state" in user:
            return user["series_state"]
        return {}

    async def update_user_series_state(self, user_id: int, state: Dict):
        """Save series continuation state after a job completes"""
        await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"series_state": state, "updated_at": datetime.now()}}
        )

    async def clear_user_series_state(self, user_id: int):
        """Clear saved series state (user pressed 'Start from 1' / clear)"""
        await self.users.update_one(
            {"user_id": user_id},
            {"$unset": {"series_state": ""}, "$set": {"updated_at": datetime.now()}}
        )

    # ------------------------------------------------------------------
    # USER MANAGEMENT  (unchanged from v1)
    # ------------------------------------------------------------------

    async def get_or_create_user(self, user_id: int, name: str) -> Dict:
        user = await self.users.find_one({"user_id": user_id})
        if not user:
            user = {
                "user_id":    user_id,
                "name":       name,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "settings": {
                    "default_duration":    60,
                    "upload_mode":         "video",
                    "auto_resume":         True,
                    "caption_template":    "[Part {part}] {start_time} - {end_time}",
                    "notify_on_completion": True,
                    "notify_on_failure":   True
                },
                "stats": {
                    "jobs_completed": 0,
                    "jobs_failed":    0,
                    "total_parts":    0,
                    "total_duration": 0
                }
            }
            await self.users.insert_one(user)
            logger.info(f"Created new user: {user_id} ({name})")
        return user

    async def get_user_settings(self, user_id: int) -> Dict:
        user = await self.users.find_one({"user_id": user_id})
        if user and "settings" in user:
            return user["settings"]
        return {
            "default_duration":    60,
            "upload_mode":         "video",
            "auto_resume":         True,
            "caption_template":    "[Part {part}] {start_time} - {end_time}",
            "notify_on_completion": True,
            "notify_on_failure":   True
        }

    async def update_user_settings(self, user_id: int, settings: Dict):
        await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"settings": settings, "updated_at": datetime.now()}}
        )

    # ------------------------------------------------------------------
    # CHANNEL MANAGEMENT  (unchanged from v1)
    # ------------------------------------------------------------------

    async def add_channel(self, user_id: int, channel_id: int, name: str) -> bool:
        channel_data = {
            "channel_id":           channel_id,
            "user_id":              user_id,
            "name":                 name,
            "added_at":             datetime.now(),
            "last_used":            None,
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
        try:
            result = await self.channels.delete_one({"user_id": user_id, "channel_id": channel_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Failed to remove channel: {e}")
            return False

    async def update_channel_usage(self, user_id: int, channel_id: int):
        await self.channels.update_one(
            {"user_id": user_id, "channel_id": channel_id},
            {"$set": {"last_used": datetime.now()}}
        )

    async def update_channel_permissions(self, user_id: int, channel_id: int, verified: bool):
        await self.channels.update_one(
            {"user_id": user_id, "channel_id": channel_id},
            {"$set": {"permissions_verified": verified}}
        )

    # ------------------------------------------------------------------
    # JOB MANAGEMENT  (updated to include series fields)
    # ------------------------------------------------------------------

    async def create_job(self, job_data: JobData) -> str:
        job_dict = {
            "job_id":           job_data.job_id,
            "user_id":          job_data.user_id,
            "file_message_id":  job_data.file_message_id,
            "file_id":          job_data.file_id,
            "file_name":        job_data.file_name,
            "file_size":        job_data.file_size,
            "original_duration":job_data.original_duration,
            "start_time":       job_data.start_time,
            "end_time":         job_data.end_time,
            "clip_duration":    job_data.clip_duration,
            "total_parts":      job_data.total_parts,
            "current_part":     job_data.current_part,
            "completed_parts":  job_data.completed_parts,
            "failed_parts":     job_data.failed_parts,
            "state":            job_data.state.value,
            "target_channel_id":    job_data.target_channel_id,
            "target_channel_name":  job_data.target_channel_name,
            "file_path":        job_data.file_path,
            "caption_template": job_data.caption_template,
            "upload_mode":      job_data.upload_mode.value,
            "created_at":       job_data.created_at,
            "updated_at":       job_data.updated_at,
            "started_at":       job_data.started_at,
            "completed_at":     job_data.completed_at,
            "error_message":    job_data.error_message,
            "progress_message_id": job_data.progress_message_id,
            "checksum":         job_data.checksum,
            "retry_count":      job_data.retry_count,
            "flood_wait_until": job_data.flood_wait_until,
            "metadata":         job_data.metadata,
            "cancel_requested": job_data.cancel_requested,
            # Series fields
            "series_name":       job_data.series_name,
            "episode_number":    job_data.episode_number,
            "part_offset":       job_data.part_offset,
            "previous_job_id":   job_data.previous_job_id,
            "continue_from_last":job_data.continue_from_last
        }
        await self.jobs.insert_one(job_dict)
        logger.info(f"Created job {job_data.job_id} for user {job_data.user_id} "
                    f"(part_offset={job_data.part_offset})")
        return job_data.job_id

    async def update_job_state(self, job_id: str, state: JobState, **updates):
        update_data = {
            "state":      state.value,
            "updated_at": datetime.now(),
            **updates
        }
        await self.jobs.update_one({"job_id": job_id}, {"$set": update_data})
        logger.debug(f"Updated job {job_id} to state {state.name}")

    async def mark_job_cancelled(self, job_id: str):
        await self.jobs.update_one(
            {"job_id": job_id},
            {"$set": {"cancel_requested": True, "updated_at": datetime.now()}}
        )

    async def get_job(self, job_id: str) -> Optional[JobData]:
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
            cancel_requested=doc.get("cancel_requested", False),
            series_name=doc.get("series_name", ""),
            episode_number=doc.get("episode_number", 0),
            part_offset=doc.get("part_offset", 1),
            previous_job_id=doc.get("previous_job_id"),
            continue_from_last=doc.get("continue_from_last", False)
        )

    async def get_active_user_job(self, user_id: int) -> Optional[JobData]:
        doc = await self.jobs.find_one({
            "user_id": user_id,
            "state": {"$in": [
                JobState.WAITING_DURATION.value,
                JobState.WAITING_CHANNEL.value,
                JobState.PREVIEW.value,
                JobState.QUEUED.value,
                JobState.DOWNLOADING.value,
                JobState.PROCESSING.value,
                JobState.UPLOADING.value,
                JobState.PAUSED.value,
                JobState.RESUMING.value
            ]}
        })
        return await self.get_job(doc["job_id"]) if doc else None

    async def get_incomplete_jobs(self) -> List[JobData]:
        cursor = self.jobs.find({"state": {"$in": [
            JobState.QUEUED.value,
            JobState.DOWNLOADING.value,
            JobState.PROCESSING.value,
            JobState.UPLOADING.value,
            JobState.PAUSED.value
        ]}})
        jobs = []
        async for doc in cursor:
            job = await self.get_job(doc["job_id"])
            if job:
                jobs.append(job)
        return jobs

    async def get_all_jobs_for_user(self, user_id: int, limit: int = 10) -> List[JobData]:
        cursor = self.jobs.find({"user_id": user_id}).sort("created_at", -1).limit(limit)
        jobs = []
        async for doc in cursor:
            job = await self.get_job(doc["job_id"])
            if job:
                jobs.append(job)
        return jobs

    async def add_to_queue(self, job_id: str, priority: int = 0):
        queue_item = {
            "job_id":     job_id,
            "priority":   priority,
            "created_at": datetime.now(),
            "status":     "pending"
        }
        await self.queue.insert_one(queue_item)

    async def get_next_job(self) -> Optional[str]:
        doc = await self.queue.find_one_and_delete(
            {}, sort=[("priority", -1), ("created_at", 1)]
        )
        return doc["job_id"] if doc else None

    async def get_queue_size(self) -> int:
        return await self.queue.count_documents({})

    async def get_statistics(self) -> Dict:
        total_jobs    = await self.jobs.count_documents({})
        active_jobs   = await self.jobs.count_documents({"state": {"$in": [
            JobState.QUEUED.value, JobState.DOWNLOADING.value,
            JobState.PROCESSING.value, JobState.UPLOADING.value
        ]}})
        completed_jobs = await self.jobs.count_documents({"state": JobState.COMPLETED.value})
        failed_jobs    = await self.jobs.count_documents({"state": JobState.FAILED.value})
        total_users    = await self.users.count_documents({})
        total_admins   = await self.roles.count_documents({"active": True, "role": UserRole.ADMIN.value})

        disk_usage = 0
        cursor = self.jobs.find({"file_path": {"$ne": None}})
        async for doc in cursor:
            if doc.get("file_path") and os.path.exists(doc["file_path"]):
                try:
                    disk_usage += os.path.getsize(doc["file_path"])
                except:
                    pass

        return {
            "total_jobs":      total_jobs,
            "active_jobs":     active_jobs,
            "completed_jobs":  completed_jobs,
            "failed_jobs":     failed_jobs,
            "total_users":     total_users,
            "total_admins":    total_admins,
            "disk_usage_bytes":disk_usage,
            "disk_usage_mb":   disk_usage / (1024 * 1024),
            "queue_size":      await self.get_queue_size()
        }

db = DatabaseManager()

# ================================================================================
# 5. PERMISSION MANAGER  — SINGLE GATE FOR ALL ACCESS CHECKS
# ================================================================================

class PermissionManager:
    """
    All permission checks in the entire bot go through this class.
    No scattered if user_id in Config.ADMIN_IDS anywhere else.
    Owner ID is always the OWNER_ID env value — it cannot be changed via bot.
    Admin and User roles are managed in the DB roles collection.
    """

    @staticmethod
    def is_owner(user_id: int) -> bool:
        """Synchronous owner check — no DB needed, owner is fixed env value"""
        return user_id == Config.OWNER_ID

    @staticmethod
    async def is_admin(user_id: int) -> bool:
        """
        Admin means role=admin OR role=owner in the DB.
        Owner is always admin too.
        """
        if user_id == Config.OWNER_ID:
            return True
        role_doc = await db.get_role(user_id)
        if not role_doc:
            return False
        return role_doc.get("role") in [UserRole.ADMIN.value, UserRole.OWNER.value]

    @staticmethod
    async def is_allowed_user(user_id: int) -> bool:
        """
        Allowed user = owner OR admin OR explicit user role.
        This is the minimum access level to USE the bot.
        """
        if user_id == Config.OWNER_ID:
            return True
        role_doc = await db.get_role(user_id)
        if not role_doc:
            return False
        return role_doc.get("role") in [
            UserRole.OWNER.value,
            UserRole.ADMIN.value,
            UserRole.USER.value
        ]

    @staticmethod
    async def can_manage_roles(user_id: int) -> bool:
        """Only the owner can add/remove admins"""
        return user_id == Config.OWNER_ID

    @staticmethod
    async def can_manage_jobs(user_id: int) -> bool:
        """Admins can manage any user's job. Users can only manage their own."""
        return await PermissionManager.is_admin(user_id)

    @staticmethod
    async def check_access(user_id: int, level: str = "user") -> Tuple[bool, str]:
        """
        Central gate. Returns (allowed, denial_reason).
        level values: "user", "admin", "owner"
        If allowed, denial_reason is empty string.
        If denied, denial_reason is a message to send back.
        """
        if level == "owner":
            if PermissionManager.is_owner(user_id):
                return True, ""
            return False, "🔒 **Owner Only**\n\nThis action is restricted to the bot owner."

        elif level == "admin":
            if await PermissionManager.is_admin(user_id):
                return True, ""
            return False, (
                "🔒 **Admin Required**\n\n"
                f"Your ID: `{user_id}`\n"
                "This action requires Admin or Owner access.\n"
                "Contact the bot owner to request access."
            )

        elif level == "user":
            if await PermissionManager.is_allowed_user(user_id):
                return True, ""
            return False, (
                "⛔ **Access Denied**\n\n"
                f"Your ID: `{user_id}`\n"
                "This bot is private. Contact the owner to get access.\n\n"
                "Share your ID with the owner and ask them to run:\n"
                "`/grantuser <your_id>`"
            )

        return False, "⛔ Access Denied."


# ================================================================================
# 6. UTILITY FUNCTIONS
# ================================================================================

# ---- 6a. VIDEO PROCESSOR -------------------------------------------------------
# DO NOT MODIFY THIS CLASS.
# The cutting engine, ffmpeg commands, and timing logic are completely untouched.
# -------------------------------------------------------------------------------

class VideoProcessor:
    """Handle all video processing operations — CUTTING ENGINE, DO NOT MODIFY"""

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
                (s for s in info["streams"] if s["codec_type"] == "video"), None
            )
            audio_stream = next(
                (s for s in info["streams"] if s["codec_type"] == "audio"), None
            )
            return {
                "duration":      duration,
                "format":        info["format"]["format_name"],
                "size":          int(info["format"]["size"]),
                "video_codec":   video_stream["codec_name"] if video_stream else None,
                "video_width":   video_stream.get("width") if video_stream else None,
                "video_height":  video_stream.get("height") if video_stream else None,
                "audio_codec":   audio_stream["codec_name"] if audio_stream else None,
                "audio_channels":audio_stream.get("channels") if audio_stream else None,
                "bitrate":       int(info["format"].get("bit_rate", 0))
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

# ---- 6b. RESOURCE MONITOR (unchanged) -----------------------------------------

class ResourceMonitor:
    """Monitor system resources"""

    @staticmethod
    def get_disk_usage(path: str = Config.WORK_DIR) -> Dict:
        usage = shutil.disk_usage(path)
        return {
            "total":   usage.total,
            "used":    usage.used,
            "free":    usage.free,
            "percent": (usage.used / usage.total) * 100
        }

    @staticmethod
    def get_memory_usage() -> Dict:
        memory = psutil.virtual_memory()
        return {
            "total":     memory.total,
            "available": memory.available,
            "used":      memory.used,
            "percent":   memory.percent
        }

    @staticmethod
    def check_disk_space(min_free_gb: int = 1) -> bool:
        usage   = ResourceMonitor.get_disk_usage()
        free_gb = usage["free"] / (1024 ** 3)
        return free_gb >= min_free_gb

    @staticmethod
    def cleanup_old_files(max_age_hours: int = Config.AUTO_CLEANUP_HOURS):
        work_dir    = Path(Config.WORK_DIR)
        temp_dir    = Path(Config.TEMP_DIR)
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
        work_dir = Path(Config.WORK_DIR)
        temp_dir = Path(Config.TEMP_DIR)
        cleaned_count = 0
        freed_bytes   = 0
        for directory in [work_dir, temp_dir]:
            if not directory.exists():
                continue
            for file_path in directory.glob(f"{job_id}*"):
                try:
                    if file_path.is_file():
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        freed_bytes   += file_size
                        cleaned_count += 1
                        logger.info(f"Cleaned up: {file_path.name} ({file_size} bytes)")
                except Exception as e:
                    logger.error(f"Failed to cleanup {file_path}: {e}")
        import gc
        gc.collect()
        return cleaned_count, freed_bytes

# ---- 6c. TIME UTILS (unchanged) -----------------------------------------------

class TimeUtils:
    """Time-related utilities"""

    @staticmethod
    def format_timestamp(seconds: float) -> str:
        hours   = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs    = int(seconds % 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        else:
            return f"{minutes:02d}:{secs:02d}"

    @staticmethod
    def format_duration(seconds: float) -> str:
        if seconds < 60:
            return f"{int(seconds)} seconds"
        elif seconds < 3600:
            return f"{seconds / 60:.1f} minutes"
        else:
            return f"{seconds / 3600:.1f} hours"

    @staticmethod
    def format_file_size(bytes: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes < 1024.0:
                return f"{bytes:.2f} {unit}"
            bytes /= 1024.0
        return f"{bytes:.2f} TB"

    @staticmethod
    def estimate_processing_time(duration: float, clip_duration: int,
                                  include_upload: bool = True) -> str:
        total_parts           = math.ceil(duration / clip_duration)
        cut_time_per_part     = 5
        upload_time_per_part  = 10
        total_cut_time        = total_parts * cut_time_per_part
        total_upload_time     = total_parts * upload_time_per_part if include_upload else 0
        total_seconds         = total_cut_time + total_upload_time
        if total_seconds < 60:
            return f"{int(total_seconds)} seconds"
        elif total_seconds < 3600:
            return f"{int(total_seconds / 60)} minutes"
        else:
            return f"{total_seconds / 3600:.1f} hours"

# ---- 6d. UI COMPONENTS (updated) ----------------------------------------------

class UIComponents:
    """UI component generators — updated with series and admin buttons"""

    @staticmethod
    def create_progress_bar(current: int, total: int, width: int = 20) -> str:
        if total == 0:
            return "▢" * width
        progress   = current / total
        filled     = int(width * progress)
        empty      = width - filled
        bar        = "■" * filled + "□" * empty
        percentage = int(progress * 100)
        return f"{bar} {percentage}%"

    @staticmethod
    def create_trim_mode_buttons(message_id: int) -> InlineKeyboardMarkup:
        buttons = [
            [
                InlineKeyboardButton("🎬 Process Full Video",
                                     callback_data=f"mode_full_{message_id}"),
                InlineKeyboardButton("✂️ Custom Trim",
                                     callback_data=f"mode_custom_{message_id}")
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_all")]
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def create_duration_buttons(message_id: int) -> InlineKeyboardMarkup:
        buttons = [
            [
                InlineKeyboardButton("30s",  callback_data=f"dur_{message_id}_30"),
                InlineKeyboardButton("55s",  callback_data=f"dur_{message_id}_55"),
                InlineKeyboardButton("60s",  callback_data=f"dur_{message_id}_60")
            ],
            [
                InlineKeyboardButton("2m",   callback_data=f"dur_{message_id}_120"),
                InlineKeyboardButton("5m",   callback_data=f"dur_{message_id}_300"),
                InlineKeyboardButton("✏️ Custom", callback_data=f"custom_dur_{message_id}")
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_all")]
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def create_channel_buttons(channels: List[ChannelInfo], message_id: int,
                                duration: int) -> InlineKeyboardMarkup:
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
            InlineKeyboardButton("🔙 Back",       callback_data=f"back_dur_{message_id}")
        ])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def create_part_offset_buttons(message_id: int,
                                    suggested_offset: int = 1) -> InlineKeyboardMarkup:
        """
        Series continuation step — shown after channel selection, before preview.
        The suggested_offset is (last_completed_part + 1) from DB.
        """
        continue_label = (
            f"🔗 Continue from Part {suggested_offset}"
            if suggested_offset > 1
            else "🔗 Continue Series (no saved state)"
        )
        buttons = [
            [InlineKeyboardButton("🔢 Start from Part 1",
                                   callback_data=f"poffset_1_{message_id}")],
            [InlineKeyboardButton(continue_label,
                                   callback_data=f"poffset_continue_{message_id}")],
            [
                InlineKeyboardButton("✏️ Custom Part Number",
                                      callback_data=f"poffset_custom_{message_id}"),
                InlineKeyboardButton("🗑 Clear Saved Series",
                                      callback_data=f"poffset_clear_{message_id}")
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_all")]
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def create_preview_buttons(message_id: int, duration: int,
                                channel_id: int) -> InlineKeyboardMarkup:
        buttons = [
            [InlineKeyboardButton("✅ Start Processing",
                                   callback_data=f"start_job_{message_id}_{duration}_{channel_id}")],
            [
                InlineKeyboardButton("🔙 Change Channel",
                                      callback_data=f"back_to_chan_{message_id}_{duration}"),
                InlineKeyboardButton("⏱ Change Duration",
                                      callback_data=f"back_dur_{message_id}")
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_all")]
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def create_job_control_buttons(job_id: str,
                                    show_cancel: bool = True) -> InlineKeyboardMarkup:
        buttons = []
        if show_cancel:
            buttons.append([InlineKeyboardButton("⏹ Cancel Job",
                                                   callback_data=f"cancel_job_{job_id}")])
        buttons.append([InlineKeyboardButton("📊 Job Details",
                                              callback_data=f"details_{job_id}")])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def create_admin_dashboard_buttons(is_owner: bool = False) -> InlineKeyboardMarkup:
        """Admin panel — Roles & Permissions section visible only to owner"""
        buttons = [
            [
                InlineKeyboardButton("📊 Statistics", callback_data="admin_stats"),
                InlineKeyboardButton("👥 Users",      callback_data="admin_users")
            ],
            [
                InlineKeyboardButton("🔄 Active Jobs", callback_data="admin_jobs"),
                InlineKeyboardButton("💾 Storage",     callback_data="admin_storage")
            ],
            [
                InlineKeyboardButton("🧹 Cleanup",   callback_data="admin_cleanup"),
                InlineKeyboardButton("📋 Logs",      callback_data="admin_logs")
            ]
        ]
        if is_owner:
            buttons.append([
                InlineKeyboardButton("🔑 Roles & Permissions", callback_data="admin_roles")
            ])
        buttons.append([InlineKeyboardButton("❌ Close", callback_data="close")])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def create_settings_buttons(settings: Dict) -> InlineKeyboardMarkup:
        default_dur  = settings.get("default_duration", 60)
        upload_mode  = settings.get("upload_mode", "video")
        auto_resume  = settings.get("auto_resume", True)
        buttons = [
            [
                InlineKeyboardButton(f"⏱ Duration: {default_dur}s",
                                      callback_data="setting_duration"),
                InlineKeyboardButton(f"📤 Mode: {upload_mode.upper()}",
                                      callback_data="setting_mode")
            ],
            [
                InlineKeyboardButton(
                    f"🔄 Auto Resume: {'✅' if auto_resume else '❌'}",
                    callback_data="setting_auto_resume"
                )
            ],
            [InlineKeyboardButton("📝 Edit Caption Template",
                                   callback_data="setting_caption")],
            [InlineKeyboardButton("🔙 Back", callback_data="start")]
        ]
        return InlineKeyboardMarkup(buttons)


# ================================================================================
# 7. JOB PROCESSOR WITH CRASH RECOVERY
# ================================================================================

class JobProcessor:
    """
    Core job processor.
    CUTTING ENGINE IS UNCHANGED.
    Only change: caption numbering uses part_offset for series continuation.
    display_part = job.part_offset + (part_num - 1)
    """

    def __init__(self, bot_client):
        self.bot              = bot_client
        self.active_jobs:     Dict[str, asyncio.Task] = {}
        self.queue_lock       = asyncio.Lock()
        self.processing_lock  = asyncio.Semaphore(Config.MAX_CONCURRENT_JOBS)
        self.upload_timestamps: Dict[int, List[datetime]] = {}
        self.flood_wait_expiry: Dict[int, datetime]       = {}
        self.download_tasks:  Dict[str, asyncio.Task]    = {}

    async def start(self):
        """Start job processor and recover incomplete jobs"""
        logger.info("Starting job processor...")
        incomplete_jobs = await db.get_incomplete_jobs()
        if incomplete_jobs:
            logger.info(f"Found {len(incomplete_jobs)} incomplete jobs for recovery")
            for job in incomplete_jobs:
                if job.retry_count >= Config.MAX_RETRIES:
                    logger.error(f"Job {job.job_id} exceeded max retries. Marking FAILED.")
                    await db.update_job_state(
                        job.job_id, JobState.FAILED,
                        error_message="Max retries exceeded during recovery"
                    )
                    continue
                retry_count = job.retry_count if job.state == JobState.FAILED else job.retry_count
                if job.state in [JobState.QUEUED, JobState.PAUSED]:
                    await db.add_to_queue(job.job_id)
                    logger.info(f"Rescheduled job {job.job_id} for recovery")
                elif job.state in [JobState.DOWNLOADING, JobState.PROCESSING, JobState.UPLOADING]:
                    await db.update_job_state(
                        job.job_id, JobState.PAUSED,
                        error_message="Bot restarted", retry_count=retry_count
                    )
                    await db.add_to_queue(job.job_id, priority=10)
                    logger.info(f"Marked interrupted job {job.job_id} for resume")
        asyncio.create_task(self._queue_processor())
        logger.info("Job processor started")

    async def _queue_processor(self):
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
        job = await db.get_job(job_id)
        if not job:
            logger.warning(f"Job {job_id} not found")
            return
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
        job_id = job.job_id
        try:
            current_job = await db.get_job(job_id)
            if current_job.cancel_requested:
                raise asyncio.CancelledError("Job cancelled by user")

            await db.update_job_state(job_id, JobState.DOWNLOADING)
            file_path = await self._download_file(job)
            if not file_path:
                raise Exception("Failed to download file")

            current_job = await db.get_job(job_id)
            if current_job.cancel_requested:
                if os.path.exists(file_path):
                    os.remove(file_path)
                raise asyncio.CancelledError("Job cancelled by user")

            await self._update_status_message(
                status_msg,
                "🔍 **Analyzing video**\n\nGetting exact duration...",
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )

            video_info = await VideoProcessor.get_video_info(file_path)
            if not video_info or video_info["duration"] <= 0:
                raise Exception(f"Invalid video duration: {video_info.get('duration', 0)}")

            real_duration = video_info["duration"]
            logger.info(f"Real duration detected: {real_duration}s (Initial: {job.original_duration}s)")

            new_end_time = job.end_time
            if (job.end_time <= 0 or job.end_time > real_duration or
                    (job.end_time == job.original_duration and job.original_duration == 0)):
                new_end_time = real_duration
                logger.info(f"Correcting end_time to {new_end_time}s")

            await db.jobs.update_one(
                {"job_id": job_id},
                {"$set": {
                    "original_duration": real_duration,
                    "end_time": new_end_time,
                    "file_path": file_path
                }}
            )
            job.end_time         = new_end_time
            job.original_duration = real_duration

            process_duration = job.end_time - job.start_time
            total_parts      = math.ceil(process_duration / job.clip_duration)

            if total_parts <= 0:
                raise Exception(
                    f"Invalid Time Range: {job.start_time} to {job.end_time} "
                    f"(Duration: {process_duration})"
                )

            await db.update_job_state(job_id, JobState.PROCESSING, total_parts=total_parts)
            await self._process_parts(job_id, status_msg, file_path, real_duration, total_parts)

            await db.update_job_state(job_id, JobState.COMPLETED, completed_at=datetime.now())
            final_job      = await db.get_job(job_id)
            completed_count = len(final_job.completed_parts)
            failed_count    = len(final_job.failed_parts)

            # Calculate display part range for completion message
            first_display = job.part_offset
            last_display  = job.part_offset + total_parts - 1

            await self._update_status_message(
                status_msg,
                f"✅ **Job Completed!**\n\n"
                f"✓ Total parts processed: {total_parts}\n"
                f"✓ Part numbers shown: {first_display} → {last_display}\n"
                f"✓ Uploaded: {completed_count}\n"
                f"✓ Failed: {failed_count}\n"
                f"✓ Channel: {job.target_channel_name}\n\n"
                f"All files have been cleaned up."
            )

            # Save series state so the next episode knows where to continue from
            next_offset = last_display + 1
            await db.update_user_series_state(job.user_id, {
                "last_job_id":          job_id,
                "last_series_name":     job.series_name,
                "last_channel_id":      job.target_channel_id,
                "last_part_end":        last_display,
                "next_suggested_offset":next_offset,
                "updated_at":           datetime.now().isoformat()
            })

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
                status_msg, f"❌ **Job Failed**\n\nError: `{str(e)[:200]}`"
            )
            await self._cleanup_job_completely(job_id)

    async def _process_from_resume(self, job: JobData, status_msg: Message):
        job_id = job.job_id
        try:
            current_job = await db.get_job(job_id)
            if current_job.cancel_requested:
                raise asyncio.CancelledError("Job cancelled by user")

            if not job.file_path or not os.path.exists(job.file_path):
                await self._update_status_message(
                    status_msg,
                    "🔄 **Resuming — Downloading file again**",
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
                video_info  = await VideoProcessor.get_video_info(file_to_process)
                duration    = video_info["duration"]
                total_parts = math.ceil(duration / job.clip_duration)
                await db.update_job_state(
                    job_id, JobState.PROCESSING,
                    original_duration=duration, total_parts=total_parts
                )
            else:
                duration    = job.original_duration
                total_parts = job.total_parts

            await self._update_status_message(
                status_msg,
                f"🔄 **Resuming Job**\n\nResuming from part {job.current_part}/{total_parts}",
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )

            await self._process_parts(
                job_id, status_msg, file_to_process,
                duration, total_parts, job.current_part
            )
            await db.update_job_state(job_id, JobState.COMPLETED, completed_at=datetime.now())
            completed_parts = len((await db.get_job(job_id)).completed_parts)

            last_display = job.part_offset + total_parts - 1
            await self._update_status_message(
                status_msg,
                f"✅ **Job Resumed & Completed!**\n\n"
                f"Total parts: {completed_parts}/{total_parts}\n"
                f"Part numbers: {job.part_offset} → {last_display}\n\n"
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
                status_msg, f"❌ **Resume Failed**\n\nError: `{str(e)[:200]}`"
            )
            await self._cleanup_job_completely(job_id)

    async def _process_parts(
        self,
        job_id:     str,
        status_msg: Message,
        input_path: str,
        duration:   float,
        total_parts:int,
        start_part: int = 1
    ):
        """
        Process all parts — CUTTING ENGINE UNCHANGED.

        The only addition is display_part numbering for series continuation:
            display_part = job.part_offset + (part_num - 1)

        This means:
          - part_offset = 1  → displays Part 1, 2, 3 ...  (default)
          - part_offset = 36 → displays Part 36, 37, 38 ... (episode 2 continuation)
        The ffmpeg -ss and -t values are NEVER changed. Only the caption number changes.
        """
        job = await db.get_job(job_id)
        if not job:
            return

        base_start_time = job.start_time

        for part_num in range(start_part, total_parts + 1):
            current_job = await db.get_job(job_id)
            if current_job.cancel_requested or current_job.state == JobState.CANCELLED:
                logger.info(f"Job {job_id} cancelled during processing at part {part_num}")
                raise asyncio.CancelledError("Job cancelled by user")

            # --- FFMPEG TIMING (completely unchanged) ---
            relative_start   = (part_num - 1) * job.clip_duration
            abs_start_time   = base_start_time + relative_start
            time_remaining   = job.end_time - abs_start_time
            segment_duration = min(job.clip_duration, time_remaining)
            abs_end_time     = abs_start_time + segment_duration

            if segment_duration <= 0.1:
                continue

            # --- DISPLAY PART NUMBER (series offset applied here only) ---
            display_part = job.part_offset + (part_num - 1)

            progress_bar  = UIComponents.create_progress_bar(part_num - 1, total_parts)
            safe_filename = job.file_name.replace("`", "")

            await self._update_status_message(
                status_msg,
                f"⚙️ **Processing Part {display_part} ({part_num}/{total_parts})**\n\n"
                f"🎞 File: `{safe_filename}`\n"
                f"⏱ Time: {TimeUtils.format_timestamp(abs_start_time)} - "
                f"{TimeUtils.format_timestamp(abs_end_time)}\n"
                f"📊 Progress: `{progress_bar}`\n"
                f"📍 Status: ✂️ Cutting (Fast Mode)...",
                reply_markup=UIComponents.create_job_control_buttons(job_id, True)
            )

            output_path = Path(Config.TEMP_DIR) / f"{job_id}_part_{part_num}.mp4"

            # --- CUT (reencode=False — fast copy mode, preserves quality, saves RAM) ---
            success, error = await VideoProcessor.cut_video_segment(
                input_path,
                str(output_path),
                abs_start_time,
                segment_duration,
                reencode=False
            )

            if not success:
                logger.error(f"Failed to cut part {part_num}: {error}")
                await db.jobs.update_one(
                    {"job_id": job_id}, {"$push": {"failed_parts": part_num}}
                )
                continue

            await db.update_job_state(job_id, JobState.UPLOADING)

            # Caption uses display_part (offset-aware)
            caption = job.caption_template.format(
                part=display_part,
                start_time=TimeUtils.format_timestamp(abs_start_time),
                end_time=TimeUtils.format_timestamp(abs_end_time),
                total_parts=total_parts,
                file_name=safe_filename
            )

            upload_success = await self._upload_with_retry(
                job, str(output_path), caption, part_num, status_msg
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
                    {"job_id": job_id}, {"$push": {"failed_parts": part_num}}
                )

            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                    import gc
                    gc.collect()
                except:
                    pass

            await asyncio.sleep(Config.UPLOAD_COOLDOWN)
            await db.update_job_state(job_id, JobState.PROCESSING)

    async def _upload_with_retry(
        self,
        job:       JobData,
        file_path: str,
        caption:   str,
        part_num:  int,
        status_msg:Message = None
    ) -> bool:
        max_retries = Config.MAX_RETRIES
        total_parts = job.total_parts
        setattr(self, f'_up_start_{job.job_id}', time.time())

        for attempt in range(max_retries):
            try:
                current_job = await db.get_job(job.job_id)
                if current_job.cancel_requested:
                    raise asyncio.CancelledError("Job cancelled")

                if job.target_channel_id in self.flood_wait_expiry:
                    expiry = self.flood_wait_expiry[job.target_channel_id]
                    if datetime.now() < expiry:
                        wait_seconds = (expiry - datetime.now()).total_seconds()
                        if status_msg:
                            await self._update_status_message(
                                status_msg, f"⏳ FloodWait: Sleeping {int(wait_seconds)}s..."
                            )
                        await asyncio.sleep(wait_seconds)

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
                wait_time   = e.value * Config.FLOOD_WAIT_BACKOFF
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
        try:
            current_job = await db.get_job(job.job_id)
            if current_job.cancel_requested:
                raise asyncio.CancelledError("Job cancelled before download")

            message = await self.bot.get_messages(job.user_id, job.file_message_id)
            if not message or not message.media:
                raise Exception("Message or media not found")

            file_name = f"{job.job_id}_{job.file_name}"
            file_path = Path(Config.WORK_DIR) / file_name

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
        now = time.time()
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
            speed       = current / (now - job.created_at.timestamp() + 1)
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
        now = time.time()
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
            percentage   = current * 100 / total
            speed        = current / (now - getattr(self, f'_up_start_{job_id}', now) + 1)
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
        await db.update_job_state(job.job_id, job.state, progress_message_id=message.id)
        return message

    async def _update_status_message(self, message: Message, text: str, **kwargs):
        try:
            if message.text and message.text.strip() == text.strip():
                return
            await message.edit_text(text, **kwargs)
        except errors.MessageNotModified:
            pass
        except errors.FloodWait as e:
            logger.warning(f"Status update floodwait: {e.value}")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.warning(f"Failed to update status message: {e}")

    async def _update_user_stats(self, user_id: int, parts: int, duration: float):
        try:
            await db.users.update_one(
                {"user_id": user_id},
                {
                    "$inc": {
                        "stats.jobs_completed": 1,
                        "stats.total_parts":    parts,
                        "stats.total_duration": duration
                    },
                    "$set": {"updated_at": datetime.now()}
                }
            )
        except Exception as e:
            logger.error(f"Failed to update user stats: {e}")

    async def _cleanup_job_completely(self, job_id: str):
        job = await db.get_job(job_id)
        if job and job.file_path and os.path.exists(job.file_path):
            try:
                file_size = os.path.getsize(job.file_path)
                os.remove(job.file_path)
                logger.info(f"Cleaned up source file ({TimeUtils.format_file_size(file_size)})")
            except Exception as e:
                logger.error(f"Failed to cleanup source file: {e}")

        cleaned, freed = await ResourceMonitor.force_cleanup_job_files(job_id)
        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} files, freed {TimeUtils.format_file_size(freed)}")

        await db.jobs.update_one(
            {"job_id": job_id},
            {"$set": {"file_path": None, "updated_at": datetime.now()}}
        )

    async def cancel_job(self, job_id: str, user_id: int) -> bool:
        """
        Cancel a job.
        Admins can cancel any job. Regular users can only cancel their own.
        """
        job = await db.get_job(job_id)
        if not job:
            return False

        # Owner / admin can cancel any job; users can only cancel their own
        is_admin_user = await PermissionManager.is_admin(user_id)
        if not is_admin_user and job.user_id != user_id:
            logger.warning(f"User {user_id} tried to cancel job {job_id} owned by {job.user_id}")
            return False

        await db.mark_job_cancelled(job_id)
        await db.update_job_state(job_id, JobState.CANCELLED, error_message="Cancelled")

        if job_id in self.download_tasks:
            self.download_tasks[job_id].cancel()
            del self.download_tasks[job_id]

        if job_id in self.active_jobs:
            self.active_jobs[job_id].cancel()
            del self.active_jobs[job_id]

        await self._cleanup_job_completely(job_id)
        logger.info(f"Job {job_id} cancelled by user {user_id}")
        return True


# ================================================================================
# 8. BOT HANDLERS
# ================================================================================

class BotHandlers:
    """
    All Telegram bot handlers.
    Every handler passes user_id through PermissionManager before doing anything.
    The start_command / callback bug (wrong from_user.id) is fixed via _show_main_menu().
    """

    def __init__(self, bot_client, job_processor: JobProcessor):
        self.bot       = bot_client
        self.processor = job_processor
        # In-memory state per user for multi-step flows
        # Keys in each dict entry:
        #   state, file_message_id, file_id, file_name, file_size, duration,
        #   custom_start, custom_end, clip_duration, part_offset,
        #   admin_action (for waiting_add_admin etc.)
        self.user_states: Dict[int, Dict] = {}

    # ------------------------------------------------------------------
    # CORE: SHOW MAIN MENU  (replaces direct call to start_command from callbacks)
    # ------------------------------------------------------------------

    async def _show_main_menu(self, user_id: int, target_message: Message):
        """
        Build and show the main menu for user_id.
        Safe to call from both /start command and from callbacks — user_id is explicit.
        """
        allowed, denial = await PermissionManager.check_access(user_id, "user")
        if not allowed:
            try:
                await target_message.edit_text(denial)
            except:
                await self.bot.send_message(user_id, denial)
            return

        await db.get_or_create_user(
            user_id,
            getattr(getattr(target_message, "from_user", None), "first_name", "Unknown") or "Unknown"
        )

        incomplete_job = await db.get_active_user_job(user_id)
        if incomplete_job:
            buttons = [
                [
                    InlineKeyboardButton("🔄 Resume", callback_data=f"resume_{incomplete_job.job_id}"),
                    InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job_{incomplete_job.job_id}")
                ],
                [InlineKeyboardButton("📊 Details", callback_data=f"details_{incomplete_job.job_id}")]
            ]
            text = (
                f"⚠️ **Incomplete Job Found**\n\n"
                f"Job ID: `{incomplete_job.job_id}`\n"
                f"File: `{incomplete_job.file_name}`\n"
                f"State: {incomplete_job.state.name}\n\n"
                f"Resume or Cancel?"
            )
            try:
                await target_message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
            except:
                await self.bot.send_message(user_id, text, reply_markup=InlineKeyboardMarkup(buttons))
            return

        text = (
            "👋 **Welcome to Auto-Splitter Bot v3.0**\n\n"
            "**🚀 Quick Start:**\n"
            "1. Send me a video file\n"
            "2. Select clip duration\n"
            "3. Choose target channel\n"
            "4. Set part numbering (for series)\n\n"
            "**📝 Commands:**\n"
            "/addchannel — Add a channel\n"
            "/settings   — Your preferences\n"
            "/listadmins — List current admins"
        )

        is_admin_user = await PermissionManager.is_admin(user_id)
        is_owner_user = PermissionManager.is_owner(user_id)

        buttons = [
            [
                InlineKeyboardButton("📤 Send Video", callback_data="help_video"),
                InlineKeyboardButton("⚙️ Settings",   callback_data="settings_main")
            ],
            [
                InlineKeyboardButton("📢 Channels",  callback_data="channel_list"),
                InlineKeyboardButton("📊 My Stats",  callback_data="user_stats")
            ]
        ]

        # Admin Panel button visible to admins and owner
        if is_admin_user:
            buttons.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])

        try:
            await target_message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
        except:
            await self.bot.send_message(user_id, text, reply_markup=InlineKeyboardMarkup(buttons))

    # ------------------------------------------------------------------
    # /start COMMAND
    # ------------------------------------------------------------------

    async def start_command(self, client: Client, message: Message):
        user = message.from_user
        if not user:
            return
        user_id = user.id
        logger.info(f"Start command from user {user_id}")

        allowed, denial = await PermissionManager.check_access(user_id, "user")
        if not allowed:
            await message.reply(denial)
            return

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
            "👋 **Welcome to Auto-Splitter Bot v3.0**\n\n"
            "**🚀 Quick Start:**\n"
            "1. Send me a video file\n"
            "2. Select clip duration\n"
            "3. Choose target channel\n"
            "4. Set part numbering (for series)\n\n"
            "**📝 Commands:**\n"
            "/addchannel — Add a channel\n"
            "/settings   — Your preferences\n"
            "/listadmins — List current admins"
        )

        is_admin_user = await PermissionManager.is_admin(user_id)
        buttons = [
            [
                InlineKeyboardButton("📤 Send Video", callback_data="help_video"),
                InlineKeyboardButton("⚙️ Settings",   callback_data="settings_main")
            ],
            [
                InlineKeyboardButton("📢 Channels",  callback_data="channel_list"),
                InlineKeyboardButton("📊 My Stats",  callback_data="user_stats")
            ]
        ]
        if is_admin_user:
            buttons.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])

        await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))

    # ------------------------------------------------------------------
    # /settings COMMAND  (fixed — was broken, was wired to start_command)
    # ------------------------------------------------------------------

    async def settings_command(self, client: Client, message: Message):
        """Real /settings handler — not the start menu"""
        user = message.from_user
        if not user:
            return
        user_id = user.id

        allowed, denial = await PermissionManager.check_access(user_id, "user")
        if not allowed:
            await message.reply(denial)
            return

        settings = await db.get_user_settings(user_id)
        await message.reply(
            "⚙️ **Your Settings**",
            reply_markup=UIComponents.create_settings_buttons(settings)
        )

    # ------------------------------------------------------------------
    # VIDEO HANDLER
    # ------------------------------------------------------------------

    async def video_handler(self, client: Client, message: Message):
        """Handle incoming video/document files"""
        if not message.from_user:
            return
        user_id = message.from_user.id

        allowed, denial = await PermissionManager.check_access(user_id, "user")
        if not allowed:
            await message.reply(denial)
            return

        active_job = await db.get_active_user_job(user_id)
        if active_job:
            await message.reply("⚠️ You already have an active job. Please cancel it first.")
            return

        if message.video:
            file_name = message.video.file_name or f"video_{message.id}.mp4"
            file_size = message.video.file_size
            duration  = message.video.duration
            file_id   = message.video.file_id
        elif message.document:
            file_name = message.document.file_name or f"video_{message.id}.mp4"
            file_size = message.document.file_size
            duration  = getattr(message.document, "duration", 0)
            file_id   = message.document.file_id
        else:
            return

        self.user_states[user_id] = {
            "file_message_id": message.id,
            "file_id":         file_id,
            "file_name":       file_name,
            "file_size":       file_size,
            "duration":        duration,
            "state":           "waiting_mode",
            "custom_start":    0,
            "custom_end":      duration,
            "clip_duration":   60,
            "part_offset":     1
        }

        await message.reply(
            f"🎬 **Video Received**\n`{file_name}`\n\n**Select Processing Mode:**",
            reply_markup=UIComponents.create_trim_mode_buttons(message.id)
        )

    # ------------------------------------------------------------------
    # TEXT HANDLER  (handles multi-step text input states)
    # ------------------------------------------------------------------

    async def text_handler(self, client: Client, message: Message):
        """Handle text input for all multi-step flows"""
        if not message.from_user:
            return
        user_id    = message.from_user.id
        state_data = self.user_states.get(user_id)

        # --- Custom time range input ---
        if state_data and state_data.get("state") == "waiting_custom_time":
            allowed, denial = await PermissionManager.check_access(user_id, "user")
            if not allowed:
                return
            await self._handle_custom_time_input(message, user_id, state_data)
            return

        # --- Custom part offset input ---
        if state_data and state_data.get("state") == "waiting_part_offset":
            allowed, denial = await PermissionManager.check_access(user_id, "user")
            if not allowed:
                return
            await self._handle_custom_part_offset_input(message, user_id, state_data)
            return

        # --- Admin: waiting for user_id to add as admin ---
        if state_data and state_data.get("state") == "waiting_add_admin":
            allowed, denial = await PermissionManager.check_access(user_id, "owner")
            if not allowed:
                await message.reply(denial)
                del self.user_states[user_id]
                return
            await self._handle_add_admin_input(message, user_id)
            return

        # --- Admin: waiting for user_id to grant user access ---
        if state_data and state_data.get("state") == "waiting_add_user":
            allowed, denial = await PermissionManager.check_access(user_id, "admin")
            if not allowed:
                await message.reply(denial)
                del self.user_states[user_id]
                return
            await self._handle_add_user_input(message, user_id)
            return

        # --- Admin: waiting for user_id to revoke ---
        if state_data and state_data.get("state") == "waiting_revoke":
            allowed, denial = await PermissionManager.check_access(user_id, "owner")
            if not allowed:
                await message.reply(denial)
                del self.user_states[user_id]
                return
            await self._handle_revoke_input(message, user_id)
            return

    # ------------------------------------------------------------------
    # TEXT INPUT SUB-HANDLERS
    # ------------------------------------------------------------------

    async def _handle_custom_time_input(self, message: Message, user_id: int, state_data: Dict):
        text = message.text.strip()
        try:
            parts = text.split()
            if len(parts) != 2:
                await message.reply(
                    "❌ Invalid format. Send: `Start End`\nExample: `00:15:00 01:50:00`"
                )
                return

            def parse_time(t_str):
                t_parts = list(map(int, t_str.split(':')))
                if len(t_parts) == 3:
                    return t_parts[0] * 3600 + t_parts[1] * 60 + t_parts[2]
                elif len(t_parts) == 2:
                    return t_parts[0] * 60 + t_parts[1]
                else:
                    raise ValueError

            start_seconds = parse_time(parts[0])
            end_seconds   = parse_time(parts[1])

            if start_seconds >= end_seconds:
                await message.reply("❌ Start time must be less than End time.")
                return

            if state_data.get("duration") and end_seconds > state_data["duration"]:
                await message.reply(
                    f"❌ End time exceeds video duration "
                    f"({TimeUtils.format_timestamp(state_data['duration'])})"
                )
                return

            self.user_states[user_id]["custom_start"] = start_seconds
            self.user_states[user_id]["custom_end"]   = end_seconds
            self.user_states[user_id]["state"]        = "waiting_duration"

            new_duration = end_seconds - start_seconds
            await message.reply(
                f"✅ **Range Set:** {parts[0]} - {parts[1]}\n"
                f"Total Clip Time: {TimeUtils.format_duration(new_duration)}\n\n"
                f"**Now select split duration per part:**",
                reply_markup=UIComponents.create_duration_buttons(
                    state_data["file_message_id"]
                )
            )
        except ValueError:
            await message.reply(
                "❌ Invalid time format. Use HH:MM:SS (e.g., `00:15:00 01:30:00`)"
            )
        except Exception as e:
            logger.error(f"Time parse error: {e}")
            await message.reply("❌ Error parsing time.")

    async def _handle_custom_part_offset_input(self, message: Message,
                                                user_id: int, state_data: Dict):
        text = message.text.strip()
        try:
            offset = int(text)
            if offset < 1:
                await message.reply("❌ Part number must be 1 or greater.")
                return

            self.user_states[user_id]["part_offset"] = offset
            self.user_states[user_id]["state"]       = "waiting_channel"

            # Proceed to channel selection
            msg_id   = state_data["file_message_id"]
            duration = state_data.get("clip_duration", 60)
            channels = await db.get_user_channels(user_id)

            if not channels:
                await message.reply(
                    f"✅ **Part Start Set: {offset}**\n\n"
                    "❌ No channels found. Use `/addchannel` first."
                )
                return

            await message.reply(
                f"✅ **Parts will start from: {offset}**\n\n"
                f"**Select target channel:**",
                reply_markup=UIComponents.create_channel_buttons(channels, msg_id, duration)
            )
        except ValueError:
            await message.reply("❌ Please enter a valid number. Example: `36`")

    async def _handle_add_admin_input(self, message: Message, user_id: int):
        text = message.text.strip()
        try:
            target_id = int(text)
            if target_id == Config.OWNER_ID:
                await message.reply("ℹ️ That user is already the Owner.")
                del self.user_states[user_id]
                return
            success = await db.add_role(
                target_id, UserRole.ADMIN.value, user_id,
                notes=f"Added by owner via bot"
            )
            if success:
                await message.reply(
                    f"✅ **Admin Added**\n\n"
                    f"User `{target_id}` now has Admin access.\n"
                    f"They can log in and use all operational features."
                )
            else:
                await message.reply("❌ Failed to add admin. Check logs.")
        except ValueError:
            await message.reply("❌ Invalid user ID. Must be a number.")
        finally:
            if user_id in self.user_states:
                del self.user_states[user_id]

    async def _handle_add_user_input(self, message: Message, user_id: int):
        text = message.text.strip()
        try:
            target_id = int(text)
            success = await db.add_role(
                target_id, UserRole.USER.value, user_id,
                notes=f"Granted by admin {user_id} via bot"
            )
            if success:
                await message.reply(
                    f"✅ **Access Granted**\n\n"
                    f"User `{target_id}` now has User access.\n"
                    f"They can send videos and use the bot."
                )
            else:
                await message.reply("❌ Failed to grant access. Check logs.")
        except ValueError:
            await message.reply("❌ Invalid user ID. Must be a number.")
        finally:
            if user_id in self.user_states:
                del self.user_states[user_id]

    async def _handle_revoke_input(self, message: Message, user_id: int):
        text = message.text.strip()
        try:
            target_id = int(text)
            if target_id == Config.OWNER_ID:
                await message.reply("🔒 Cannot revoke the Owner's access.")
                del self.user_states[user_id]
                return
            success = await db.revoke_role(
                target_id, user_id, reason="Revoked by owner via bot"
            )
            if success:
                await message.reply(
                    f"✅ **Access Revoked**\n\n"
                    f"User `{target_id}` no longer has access to this bot."
                )
            else:
                await message.reply(
                    f"⚠️ User `{target_id}` had no active role, or revoke failed."
                )
        except ValueError:
            await message.reply("❌ Invalid user ID. Must be a number.")
        finally:
            if user_id in self.user_states:
                del self.user_states[user_id]

    # ------------------------------------------------------------------
    # CALLBACK QUERY HANDLER  — SINGLE PERMISSION GATE
    # ------------------------------------------------------------------

    async def callback_handler(self, client: Client, callback_query: CallbackQuery):
        """
        All button presses come here.
        IMPORTANT: user_id is always from callback_query.from_user.id — never from message.from_user.
        This fixes the old bug where message.from_user returned the bot's own ID.
        """
        data    = callback_query.data
        user_id = callback_query.from_user.id  # Correct — this is always the person who clicked

        # Allow 'close' through without any check (just dismiss the message)
        if data == "close":
            await callback_query.message.delete()
            return

        # --- PERMISSION GATE ---
        # Determine required level from callback data prefix
        if data.startswith("admin_") or data.startswith("role_"):
            required_level = "admin"
        else:
            required_level = "user"

        allowed, denial = await PermissionManager.check_access(user_id, required_level)
        if not allowed:
            await callback_query.answer(denial[:200], show_alert=True)
            return

        try:
            # --- MAIN MENU ---
            if data == "start":
                # FIX: pass user_id explicitly — don't use callback_query.message.from_user
                await self._show_main_menu(user_id, callback_query.message)

            elif data == "help_video":
                await callback_query.message.edit_text(
                    "📤 **How to Send Video**\n\nSimply send any video or document file here.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back", callback_data="start")
                    ]])
                )

            # --- PROCESSING MODE SELECTION ---
            elif data.startswith("mode_full_"):
                msg_id = int(data.split("_")[2])
                if user_id in self.user_states:
                    self.user_states[user_id]["custom_start"] = 0
                    self.user_states[user_id]["custom_end"]   = self.user_states[user_id]["duration"]
                    self.user_states[user_id]["state"]        = "waiting_duration"
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

            # --- DURATION SELECTION ---
            elif data.startswith("dur_"):
                _, msg_id, duration = data.split("_")
                await self._handle_duration_selection(
                    callback_query, int(msg_id), int(duration)
                )

            # --- CHANNEL SELECTION ---
            elif data.startswith("chan_"):
                _, msg_id, duration, channel_id = data.split("_")
                await self._handle_channel_selection(
                    callback_query, int(msg_id), int(duration), int(channel_id)
                )

            # --- PART OFFSET (series continuation) ---
            elif data.startswith("poffset_"):
                await self._handle_part_offset(callback_query, data, user_id)

            # --- JOB START ---
            elif data.startswith("start_job_"):
                _, _, msg_id, duration, channel_id = data.split("_")
                await self._handle_job_start(
                    callback_query, int(msg_id), int(duration), int(channel_id)
                )

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

            # --- NAVIGATION (back buttons) ---
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
                    reply_markup=UIComponents.create_channel_buttons(
                        channels, int(msg_id), int(duration)
                    )
                )

            # --- ADMIN PANEL (accessible to all admins) ---
            elif data == "admin_panel":
                await self._show_admin_panel(callback_query, user_id)

            elif data == "admin_stats":
                await self._show_admin_stats(callback_query, user_id)

            elif data == "admin_jobs":
                await self._show_admin_jobs(callback_query, user_id)

            elif data == "admin_storage":
                await self._show_admin_storage(callback_query, user_id)

            elif data == "admin_cleanup":
                await self._handle_admin_cleanup(callback_query, user_id)

            elif data == "admin_logs":
                await self._show_admin_logs(callback_query, user_id)

            elif data == "admin_users":
                await self._show_admin_users(callback_query, user_id)

            # --- ROLES (owner only) ---
            elif data == "admin_roles":
                ok, msg = await PermissionManager.check_access(user_id, "owner")
                if not ok:
                    await callback_query.answer(msg[:200], show_alert=True)
                    return
                await self._show_roles_panel(callback_query)

            elif data == "role_add_admin":
                ok, msg = await PermissionManager.check_access(user_id, "owner")
                if not ok:
                    await callback_query.answer(msg[:200], show_alert=True)
                    return
                self.user_states[user_id] = {"state": "waiting_add_admin"}
                await callback_query.message.edit_text(
                    "🔑 **Add Admin**\n\n"
                    "Send me the **Telegram user ID** to promote to Admin:\n\n"
                    "The user must have interacted with the bot at least once.\n"
                    "Type the number and send it."
                )

            elif data == "role_add_user":
                ok, msg = await PermissionManager.check_access(user_id, "admin")
                if not ok:
                    await callback_query.answer(msg[:200], show_alert=True)
                    return
                self.user_states[user_id] = {"state": "waiting_add_user"}
                await callback_query.message.edit_text(
                    "👤 **Grant User Access**\n\n"
                    "Send me the **Telegram user ID** to grant access:\n\n"
                    "Type the number and send it."
                )

            elif data == "role_revoke":
                ok, msg = await PermissionManager.check_access(user_id, "owner")
                if not ok:
                    await callback_query.answer(msg[:200], show_alert=True)
                    return
                self.user_states[user_id] = {"state": "waiting_revoke"}
                await callback_query.message.edit_text(
                    "❌ **Revoke Access**\n\n"
                    "Send me the **Telegram user ID** to revoke:\n\n"
                    "⚠️ The owner's access cannot be revoked.\n"
                    "Type the number and send it."
                )

            # --- CHANNELS ---
            elif data == "channel_list":
                await self._show_channel_list(callback_query)

            elif data == "add_channel_prompt":
                await callback_query.message.edit_text(
                    "➕ **Add Channel**\n\nUse this command:\n"
                    "`/addchannel CHANNEL_ID CHANNEL_NAME`\n\n"
                    "Example:\n`/addchannel -1001234567890 MyMovieChannel`\n\n"
                    "Make sure to add me as Admin in that channel first!",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back", callback_data="channel_list")
                    ]])
                )

            elif data.startswith("delete_channel_"):
                channel_id = int(data.split("_")[2])
                await self._handle_channel_deletion(callback_query, channel_id)

            elif data == "verify_channels":
                await self._verify_all_channels(callback_query)

            # --- SETTINGS ---
            elif data == "settings_main":
                settings = await db.get_user_settings(user_id)
                await callback_query.message.edit_text(
                    "⚙️ **Settings**",
                    reply_markup=UIComponents.create_settings_buttons(settings)
                )

            elif data == "setting_mode":
                settings    = await db.get_user_settings(user_id)
                new_mode    = "document" if settings.get("upload_mode") == "video" else "video"
                settings["upload_mode"] = new_mode
                await db.update_user_settings(user_id, settings)
                await self._show_settings_cb(callback_query, user_id)
                await callback_query.answer(f"Switched to {new_mode.upper()}", show_alert=True)

            elif data == "setting_auto_resume":
                settings = await db.get_user_settings(user_id)
                settings["auto_resume"] = not settings.get("auto_resume", True)
                await db.update_user_settings(user_id, settings)
                await self._show_settings_cb(callback_query, user_id)

            elif data == "setting_duration":
                await callback_query.answer("Use /settings to change default duration", show_alert=True)

            elif data == "setting_caption":
                await callback_query.answer("Caption template editing coming soon", show_alert=True)

            # --- USER STATS ---
            elif data == "user_stats":
                await self._show_user_stats(callback_query)

            # --- MISC ---
            elif data == "cancel_all":
                if user_id in self.user_states:
                    del self.user_states[user_id]
                await callback_query.message.delete()
                await callback_query.answer("Cancelled")

            else:
                await callback_query.answer(
                    f"❌ Unknown action. Please use the menu.", show_alert=True
                )

        except Exception as e:
            logger.error(f"Callback error for data='{data}': {e}")
            try:
                await callback_query.answer("⚠️ An error occurred. Please try again.", show_alert=False)
            except:
                pass

    # ------------------------------------------------------------------
    # PROCESS FLOW HELPERS
    # ------------------------------------------------------------------

    async def _handle_duration_selection(self, callback: CallbackQuery,
                                          msg_id: int, duration: int):
        user_id = callback.from_user.id
        if user_id in self.user_states:
            self.user_states[user_id]["clip_duration"] = duration
            self.user_states[user_id]["state"]         = "waiting_channel"

        channels = await db.get_user_channels(user_id)
        if not channels:
            await callback.message.edit_text(
                "❌ **No Channels Found**\nUse `/addchannel` first.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back", callback_data=f"back_dur_{msg_id}")
                ]])
            )
            return

        await callback.message.edit_text(
            f"⏱ **Duration:** {duration}s\n\n**Select target channel:**",
            reply_markup=UIComponents.create_channel_buttons(channels, msg_id, duration)
        )

    async def _handle_channel_selection(self, callback: CallbackQuery,
                                         msg_id: int, duration: int, channel_id: int):
        user_id  = callback.from_user.id
        channels = await db.get_user_channels(user_id)
        channel  = next((c for c in channels if c.channel_id == channel_id), None)

        if not channel:
            await callback.answer("Channel not found", show_alert=True)
            return

        file_info = self.user_states.get(user_id)
        if not file_info:
            await callback.answer("Session expired. Please send the file again.", show_alert=True)
            return

        # Save channel selection and proceed to series/part offset step
        if user_id in self.user_states:
            self.user_states[user_id]["selected_channel_id"]   = channel_id
            self.user_states[user_id]["selected_channel_name"] = channel.name
            self.user_states[user_id]["state"] = "waiting_part_offset"

        # Get suggested offset from DB series state
        series_state     = await db.get_user_series_state(user_id)
        suggested_offset = series_state.get("next_suggested_offset", 1)

        last_series  = series_state.get("last_series_name", "")
        last_channel = series_state.get("last_channel_id", 0)

        context_text = ""
        if series_state and last_channel == channel_id:
            context_text = (
                f"\n\n📼 **Previous series:** {last_series or 'unknown'}\n"
                f"📍 **Last part completed:** {series_state.get('last_part_end', '?')}"
            )

        await callback.message.edit_text(
            f"✅ **Channel:** {channel.name}\n"
            f"⏱ **Duration:** {duration}s"
            f"{context_text}\n\n"
            f"**Set part numbering for this episode:**\n"
            f"_(Only the caption number changes — cut positions are always the same)_",
            reply_markup=UIComponents.create_part_offset_buttons(msg_id, suggested_offset)
        )

    async def _handle_part_offset(self, callback: CallbackQuery, data: str, user_id: int):
        """Handle the series continuation / part offset step"""
        parts = data.split("_")
        # data format: poffset_{action}_{msg_id}
        action = parts[1]
        msg_id = int(parts[2])

        file_info = self.user_states.get(user_id)
        if not file_info:
            await callback.answer("Session expired.", show_alert=True)
            return

        duration    = file_info.get("clip_duration", 60)
        channel_id  = file_info.get("selected_channel_id", 0)
        channel_name = file_info.get("selected_channel_name", "")

        if action == "1":
            # Start from Part 1 — clear series state
            self.user_states[user_id]["part_offset"] = 1
            await db.clear_user_series_state(user_id)
            await self._show_preview(callback, msg_id, duration, channel_id, user_id, offset=1)

        elif action == "continue":
            series_state     = await db.get_user_series_state(user_id)
            suggested_offset = series_state.get("next_suggested_offset", 1)
            if suggested_offset < 1:
                suggested_offset = 1
            self.user_states[user_id]["part_offset"] = suggested_offset
            await self._show_preview(
                callback, msg_id, duration, channel_id, user_id, offset=suggested_offset
            )

        elif action == "custom":
            self.user_states[user_id]["state"] = "waiting_part_offset"
            await callback.message.edit_text(
                "✏️ **Custom Part Start Number**\n\n"
                "Send the part number this episode should start from.\n\n"
                "Example: send `36` to start from Part 36.\n"
                "_(The video cut positions remain unchanged — only captions change)_"
            )

        elif action == "clear":
            await db.clear_user_series_state(user_id)
            self.user_states[user_id]["part_offset"] = 1
            series_state     = await db.get_user_series_state(user_id)
            await callback.answer("✅ Series state cleared.", show_alert=True)
            await callback.message.edit_text(
                f"🗑 **Series state cleared.**\n\n"
                f"**Set part numbering:**",
                reply_markup=UIComponents.create_part_offset_buttons(msg_id, 1)
            )

    async def _show_preview(self, callback: CallbackQuery, msg_id: int, duration: int,
                              channel_id: int, user_id: int, offset: int = 1):
        """Show the job preview with offset information"""
        file_info   = self.user_states.get(user_id, {})
        file_dur    = file_info.get("duration", 0)
        custom_start = file_info.get("custom_start", 0)
        custom_end   = file_info.get("custom_end", file_dur)
        process_dur  = custom_end - custom_start
        total_parts  = math.ceil(process_dur / duration) if process_dur > 0 else "?"

        # Display part range
        if isinstance(total_parts, int):
            last_part = offset + total_parts - 1
            parts_str = f"{offset} → {last_part}"
        else:
            parts_str = f"{offset} → ?"

        channels     = await db.get_user_channels(user_id)
        channel      = next((c for c in channels if c.channel_id == channel_id), None)
        channel_name = channel.name if channel else "Unknown"

        preview = (
            f"📋 **Preview**\n\n"
            f"📁 File: `{file_info.get('file_name', '?')}`\n"
            f"⏱ Clip: {duration}s\n"
            f"🔢 Parts: {total_parts}\n"
            f"🏷 Part numbers: **{parts_str}**\n"
            f"📢 Target: {channel_name}"
        )
        await callback.message.edit_text(
            preview,
            reply_markup=UIComponents.create_preview_buttons(msg_id, duration, channel_id)
        )

    async def _handle_job_start(self, callback: CallbackQuery,
                                  msg_id: int, duration: int, channel_id: int):
        user_id   = callback.from_user.id
        file_info = self.user_states.get(user_id)

        if not file_info:
            await callback.answer("Session expired. Please send the file again.", show_alert=True)
            return

        channels = await db.get_user_channels(user_id)
        channel  = next((c for c in channels if c.channel_id == channel_id), None)
        if not channel:
            await callback.answer("Channel not found.", show_alert=True)
            return

        job_id    = hashlib.md5(f"{user_id}_{time.time()}".encode()).hexdigest()[:12]
        settings  = await db.get_user_settings(user_id)
        part_offset = file_info.get("part_offset", 1)

        start_time = file_info.get("custom_start", 0)
        end_time   = file_info.get("custom_end", file_info.get("duration", 0))

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
            target_channel_name=channel.name,
            state=JobState.QUEUED,
            caption_template=settings.get("caption_template", "[Part {part}]"),
            upload_mode=UploadMode(settings.get("upload_mode", "video")),
            part_offset=part_offset
        )

        await db.create_job(job_data)
        await db.add_to_queue(job_id)
        del self.user_states[user_id]

        await callback.message.edit_text(
            f"✅ **Job Queued!**\n\n"
            f"Job ID: `{job_id}`\n"
            f"Time Range: {TimeUtils.format_timestamp(start_time)} - "
            f"{TimeUtils.format_timestamp(end_time)}\n"
            f"Parts start from: **{part_offset}**"
        )

    async def _handle_job_cancellation(self, callback: CallbackQuery, job_id: str):
        user_id = callback.from_user.id
        success = await self.processor.cancel_job(job_id, user_id)
        if success:
            await callback.answer("Job cancelled", show_alert=True)
            await callback.message.edit_text("❌ **Job Cancelled**\n\nAll files cleaned up.")
        else:
            await callback.answer(
                "❌ Cannot cancel. Job not found, already done, or you don't have permission.",
                show_alert=True
            )

    async def _handle_job_resume(self, callback: CallbackQuery, job_id: str):
        job = await db.get_job(job_id)
        if job and job.state == JobState.PAUSED:
            await db.update_job_state(job_id, JobState.QUEUED)
            await db.add_to_queue(job_id, priority=5)
            await callback.message.edit_text("🔄 **Job Resumed**\n\nAdded back to queue.")
        else:
            await callback.answer(
                "Cannot resume. Job is not in a paused state.", show_alert=True
            )

    async def _show_job_details(self, callback: CallbackQuery, job_id: str):
        job = await db.get_job(job_id)
        if not job:
            await callback.answer("Job not found", show_alert=True)
            return
        first_part = job.part_offset
        last_part  = job.part_offset + max(job.total_parts - 1, 0)
        text = (
            f"📊 **Job Details**\n\n"
            f"ID: `{job.job_id}`\n"
            f"Status: {job.state.name}\n"
            f"File: `{job.file_name}`\n"
            f"Parts completed: {len(job.completed_parts)}/{job.total_parts}\n"
            f"Failed parts: {len(job.failed_parts)}\n"
            f"Part numbers: {first_part} → {last_part}\n"
            f"Channel: {job.target_channel_name}\n"
            f"Created: {job.created_at:%Y-%m-%d %H:%M}"
        )
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="start")
            ]])
        )

    # ------------------------------------------------------------------
    # ADMIN PANEL — REAL SCREENS
    # ------------------------------------------------------------------

    async def _show_admin_panel(self, callback: CallbackQuery, user_id: int):
        """Admin panel — visible to all admins; Roles section only to owner"""
        stats    = await db.get_statistics()
        is_owner = PermissionManager.is_owner(user_id)

        text = (
            f"🛠 **Admin Panel**\n\n"
            f"📋 Total Jobs: {stats['total_jobs']}\n"
            f"🔄 Active: {stats['active_jobs']}\n"
            f"📦 Queue: {stats['queue_size']}\n"
            f"👥 Users: {stats['total_users']}\n"
            f"🛡 Admins: {stats['total_admins']}"
        )
        await callback.message.edit_text(
            text,
            reply_markup=UIComponents.create_admin_dashboard_buttons(is_owner=is_owner)
        )

    async def _show_admin_stats(self, callback: CallbackQuery, user_id: int):
        """Real statistics screen"""
        stats = await db.get_statistics()
        disk  = ResourceMonitor.get_disk_usage()
        text  = (
            f"📊 **Bot Statistics**\n\n"
            f"**Jobs:**\n"
            f"  Total:     {stats['total_jobs']}\n"
            f"  Completed: {stats['completed_jobs']}\n"
            f"  Failed:    {stats['failed_jobs']}\n"
            f"  Active:    {stats['active_jobs']}\n"
            f"  Queue:     {stats['queue_size']}\n\n"
            f"**Users:**\n"
            f"  Total:  {stats['total_users']}\n"
            f"  Admins: {stats['total_admins']}\n\n"
            f"**Disk ({Config.WORK_DIR}):**\n"
            f"  Used: {TimeUtils.format_file_size(disk['used'])} "
            f"({disk['percent']:.1f}%)\n"
            f"  Free: {TimeUtils.format_file_size(disk['free'])}"
        )
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="admin_panel")
            ]])
        )

    async def _show_admin_storage(self, callback: CallbackQuery, user_id: int):
        """Real storage and memory screen"""
        disk = ResourceMonitor.get_disk_usage()
        mem  = ResourceMonitor.get_memory_usage()
        text = (
            f"💾 **Storage & System**\n\n"
            f"**Disk ({Config.WORK_DIR}):**\n"
            f"  Total: {TimeUtils.format_file_size(disk['total'])}\n"
            f"  Used:  {TimeUtils.format_file_size(disk['used'])} ({disk['percent']:.1f}%)\n"
            f"  Free:  {TimeUtils.format_file_size(disk['free'])}\n\n"
            f"**Memory:**\n"
            f"  Total:     {TimeUtils.format_file_size(mem['total'])}\n"
            f"  Used:      {TimeUtils.format_file_size(mem['used'])} ({mem['percent']:.1f}%)\n"
            f"  Available: {TimeUtils.format_file_size(mem['available'])}\n\n"
            f"**Warning threshold:** {Config.MAX_DISK_USAGE_PERCENT}%"
        )
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="admin_panel")
            ]])
        )

    async def _show_admin_jobs(self, callback: CallbackQuery, user_id: int):
        """Real active jobs screen"""
        active_jobs = await db.get_incomplete_jobs()
        if not active_jobs:
            await callback.answer("No active jobs at this time.", show_alert=True)
            return

        lines = ["🔄 **Active & Queued Jobs:**\n"]
        for j in active_jobs[:10]:
            first = j.part_offset
            last  = j.part_offset + max(j.total_parts - 1, 0)
            lines.append(
                f"`{j.job_id}` — {j.state.name}\n"
                f"  User: {j.user_id} | Parts: {first}→{last}\n"
                f"  File: {j.file_name[:30]}"
            )

        text = "\n".join(lines)
        if len(active_jobs) > 10:
            text += f"\n\n_(Showing 10 of {len(active_jobs)})_"

        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="admin_panel")
            ]])
        )

    async def _show_admin_users(self, callback: CallbackQuery, user_id: int):
        """Show users with their roles"""
        roles_list = await db.get_all_active_roles()
        if not roles_list:
            await callback.answer("No users in DB yet.", show_alert=True)
            return

        lines = ["👥 **Users & Roles:**\n"]
        for r in roles_list[:15]:
            icon = "👑" if r["role"] == "owner" else "🛡" if r["role"] == "admin" else "👤"
            lines.append(f"{icon} `{r['user_id']}` — {r['role'].upper()}")

        text = "\n".join(lines)
        if len(roles_list) > 15:
            text += f"\n\n_(Showing 15 of {len(roles_list)})_"

        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="admin_panel")
            ]])
        )

    async def _handle_admin_cleanup(self, callback: CallbackQuery, user_id: int):
        await callback.answer("Cleaning old files...", show_alert=False)
        ResourceMonitor.cleanup_old_files(1)
        stats = await db.get_statistics()
        disk  = ResourceMonitor.get_disk_usage()
        await callback.answer(
            f"Done! Disk: {disk['percent']:.1f}% used", show_alert=True
        )

    async def _show_admin_logs(self, callback: CallbackQuery, user_id: int):
        """Show recent admin action log entries"""
        logs = await db.get_recent_action_logs(15)
        if not logs:
            await callback.answer("No admin actions logged yet.", show_alert=True)
            return

        lines = ["📋 **Recent Admin Actions:**\n"]
        for entry in logs:
            ts     = entry["timestamp"].strftime("%m/%d %H:%M") if hasattr(entry["timestamp"], "strftime") else str(entry["timestamp"])[:16]
            target = f"→ `{entry['target_id']}`" if entry.get("target_id") else ""
            lines.append(
                f"`{ts}` [{entry['action']}]\n"
                f"  By: `{entry['admin_id']}` {target}"
            )

        await callback.message.edit_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="admin_panel")
            ]])
        )

    async def _show_roles_panel(self, callback: CallbackQuery):
        """Owner-only roles and permissions management panel"""
        roles_list = await db.get_all_active_roles()
        lines = ["🔑 **Roles & Permissions**\n"]
        for r in roles_list:
            icon = "👑" if r["role"] == "owner" else "🛡" if r["role"] == "admin" else "👤"
            added = r.get("added_at", "?")
            if hasattr(added, "strftime"):
                added = added.strftime("%Y-%m-%d")
            lines.append(f"{icon} `{r['user_id']}` — {r['role'].upper()} (since {added})")

        if not roles_list:
            lines.append("No roles in DB yet.")

        buttons = [
            [
                InlineKeyboardButton("➕ Add Admin",     callback_data="role_add_admin"),
                InlineKeyboardButton("➕ Grant User",    callback_data="role_add_user")
            ],
            [
                InlineKeyboardButton("❌ Revoke Access", callback_data="role_revoke")
            ],
            [
                InlineKeyboardButton("📋 Audit Log",    callback_data="admin_logs"),
                InlineKeyboardButton("🔙 Back",         callback_data="admin_panel")
            ]
        ]
        await callback.message.edit_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    # ------------------------------------------------------------------
    # CHANNEL / SETTINGS / STATS HELPERS
    # ------------------------------------------------------------------

    async def _show_channel_list(self, callback: CallbackQuery):
        channels = await db.get_user_channels(callback.from_user.id)
        text = (
            "📢 **Your Channels**\n\n" +
            "\n".join([f"{'✅' if c.permissions_verified else '⚠️'} {c.name} (`{c.channel_id}`)"
                       for c in channels])
            if channels else "📢 **Your Channels**\n\nNo channels added yet."
        )
        buttons = [
            [InlineKeyboardButton(f"🗑 {c.name}", callback_data=f"delete_channel_{c.channel_id}")]
            for c in channels
        ]
        buttons.append([InlineKeyboardButton("➕ Add Channel", callback_data="add_channel_prompt")])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="start")])
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    async def _handle_channel_deletion(self, callback: CallbackQuery, channel_id: int):
        await db.remove_channel(callback.from_user.id, channel_id)
        await callback.answer("Channel removed.", show_alert=True)
        await self._show_channel_list(callback)

    async def _verify_all_channels(self, callback: CallbackQuery):
        user_id  = callback.from_user.id
        channels = await db.get_user_channels(user_id)
        verified = 0
        failed   = 0
        for channel in channels:
            try:
                msg = await self.bot.send_message(channel.channel_id, "✅ Bot permission check")
                await msg.delete()
                await db.update_channel_permissions(user_id, channel.channel_id, True)
                verified += 1
            except Exception:
                await db.update_channel_permissions(user_id, channel.channel_id, False)
                failed += 1
        await callback.answer(
            f"Verified: {verified} ✅  Failed: {failed} ❌", show_alert=True
        )

    async def _show_settings_cb(self, callback: CallbackQuery, user_id: int):
        settings = await db.get_user_settings(user_id)
        await callback.message.edit_text(
            "⚙️ **Settings**",
            reply_markup=UIComponents.create_settings_buttons(settings)
        )

    async def _show_user_stats(self, callback: CallbackQuery):
        user  = await db.users.find_one({"user_id": callback.from_user.id})
        stats = user.get("stats", {}) if user else {}
        series_state = await db.get_user_series_state(callback.from_user.id)

        next_offset = series_state.get("next_suggested_offset", 1)
        last_end    = series_state.get("last_part_end", 0)
        series_info = (
            f"\n**Series State:**\n"
            f"  Last part: {last_end}\n"
            f"  Next start: {next_offset}"
            if series_state else "\n_No saved series state._"
        )

        text = (
            f"📊 **Your Stats**\n\n"
            f"Jobs Completed: {stats.get('jobs_completed', 0)}\n"
            f"Jobs Failed:    {stats.get('jobs_failed', 0)}\n"
            f"Total Parts:    {stats.get('total_parts', 0)}\n"
            f"Total Duration: {TimeUtils.format_duration(stats.get('total_duration', 0))}"
            f"{series_info}"
        )
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="start")
            ]])
        )


# ================================================================================
# 9. COMMAND HANDLERS
# ================================================================================

async def handle_addchannel_command(client: Client, message: Message):
    """Handle /addchannel command — admin or owner only"""
    user_id = message.from_user.id

    allowed, denial = await PermissionManager.check_access(user_id, "admin")
    if not allowed:
        await message.reply(denial)
        return

    try:
        args = message.text.split()
        if len(args) < 3:
            await message.reply(
                "❌ **Usage:** `/addchannel <channel_id> <name>`\n\n"
                "Example: `/addchannel -100123456 MyMovies`"
            )
            return

        channel_id = int(args[1])
        name       = " ".join(args[2:])

        verified = False
        try:
            msg = await client.send_message(channel_id, "✅ Bot connected successfully!")
            await msg.delete()
            verified = True
        except Exception as e:
            await message.reply(
                f"⚠️ **Warning:** Cannot send to channel.\nError: `{e}`\n\n"
                "Make sure I am an Admin there. Channel saved but marked unverified."
            )

        await db.add_channel(user_id, channel_id, name)
        if verified:
            await db.update_channel_permissions(user_id, channel_id, True)

        await message.reply(f"✅ Channel **{name}** (`{channel_id}`) added successfully!")

    except ValueError:
        await message.reply("❌ Invalid Channel ID. It must be a number (e.g., -100...)")
    except Exception as e:
        logger.error(f"Add channel error: {e}")
        await message.reply("❌ An error occurred.")


async def handle_addadmin_command(client: Client, message: Message):
    """Handle /addadmin <user_id> — owner only"""
    user_id = message.from_user.id

    allowed, denial = await PermissionManager.check_access(user_id, "owner")
    if not allowed:
        await message.reply(denial)
        return

    args = message.text.split()
    if len(args) < 2:
        await message.reply("❌ Usage: `/addadmin <user_id>`")
        return

    try:
        target_id = int(args[1])
        success   = await db.add_role(
            target_id, UserRole.ADMIN.value, user_id, notes="Added via /addadmin command"
        )
        if success:
            await message.reply(f"✅ User `{target_id}` is now an **Admin**.")
        else:
            await message.reply("❌ Failed to add admin.")
    except ValueError:
        await message.reply("❌ Invalid user ID.")


async def handle_removeadmin_command(client: Client, message: Message):
    """Handle /removeadmin <user_id> — owner only"""
    user_id = message.from_user.id

    allowed, denial = await PermissionManager.check_access(user_id, "owner")
    if not allowed:
        await message.reply(denial)
        return

    args = message.text.split()
    if len(args) < 2:
        await message.reply("❌ Usage: `/removeadmin <user_id>`")
        return

    try:
        target_id = int(args[1])
        if target_id == Config.OWNER_ID:
            await message.reply("🔒 Cannot remove the Owner's access.")
            return
        success = await db.revoke_role(target_id, user_id, reason="Removed via /removeadmin")
        if success:
            await message.reply(f"✅ Admin access for `{target_id}` has been revoked.")
        else:
            await message.reply(f"⚠️ User `{target_id}` had no active admin role.")
    except ValueError:
        await message.reply("❌ Invalid user ID.")


async def handle_grantuser_command(client: Client, message: Message):
    """Handle /grantuser <user_id> — admin or owner"""
    user_id = message.from_user.id

    allowed, denial = await PermissionManager.check_access(user_id, "admin")
    if not allowed:
        await message.reply(denial)
        return

    args = message.text.split()
    if len(args) < 2:
        await message.reply("❌ Usage: `/grantuser <user_id>`")
        return

    try:
        target_id = int(args[1])
        success   = await db.add_role(
            target_id, UserRole.USER.value, user_id,
            notes=f"Granted by admin {user_id} via /grantuser"
        )
        if success:
            await message.reply(f"✅ User `{target_id}` now has access to this bot.")
        else:
            await message.reply("❌ Failed to grant access.")
    except ValueError:
        await message.reply("❌ Invalid user ID.")


async def handle_revokeuser_command(client: Client, message: Message):
    """Handle /revokeuser <user_id> — owner only"""
    user_id = message.from_user.id

    allowed, denial = await PermissionManager.check_access(user_id, "owner")
    if not allowed:
        await message.reply(denial)
        return

    args = message.text.split()
    if len(args) < 2:
        await message.reply("❌ Usage: `/revokeuser <user_id>`")
        return

    try:
        target_id = int(args[1])
        if target_id == Config.OWNER_ID:
            await message.reply("🔒 Cannot revoke the Owner's access.")
            return
        success = await db.revoke_role(target_id, user_id, reason="Revoked via /revokeuser")
        if success:
            await message.reply(f"✅ Access revoked for user `{target_id}`.")
        else:
            await message.reply(f"⚠️ User `{target_id}` had no active role.")
    except ValueError:
        await message.reply("❌ Invalid user ID.")


async def handle_listadmins_command(client: Client, message: Message):
    """Handle /listadmins — any allowed user can see the list"""
    user_id = message.from_user.id

    allowed, denial = await PermissionManager.check_access(user_id, "user")
    if not allowed:
        await message.reply(denial)
        return

    roles_list = await db.get_all_active_roles()
    if not roles_list:
        await message.reply("No roles configured yet.")
        return

    lines = ["**Current Roles:**\n"]
    for r in roles_list:
        icon = "👑" if r["role"] == "owner" else "🛡" if r["role"] == "admin" else "👤"
        lines.append(f"{icon} `{r['user_id']}` — {r['role'].upper()}")

    await message.reply("\n".join(lines))


# ================================================================================
# 10. WEB SERVER
# ================================================================================

async def web_server():
    """Web server for health checks"""
    routes = web.RouteTableDef()

    @routes.get('/')
    async def root_route(request):
        return web.json_response({
            "status":    "running",
            "uptime":    "online",
            "timestamp": datetime.now().isoformat()
        })

    @routes.get('/health')
    async def health_route(request):
        return web.Response(text="OK", status=200)

    app = web.Application()
    app.add_routes(routes)
    return app

# ================================================================================
# 11. MAIN APPLICATION
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
        self.handlers      = BotHandlers(self.client, self.job_processor)

    async def start(self):
        Config.validate()

        await db.create_indexes()

        # Seed owner + bootstrap admins into DB (only writes if not already present)
        await db.bootstrap_roles()

        app    = await web_server()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, Config.WEB_HOST, Config.WEB_PORT)
        await site.start()
        logger.info(f"Web server started on port {Config.WEB_PORT}")

        self._register_handlers()

        await self.client.start()
        logger.set_bot_client(self.client)

        me = await self.client.get_me()
        logger.info(f"Bot started as @{me.username}")

        await self.job_processor.start()

        try:
            await self.client.send_message(
                Config.OWNER_ID,
                f"✅ **Bot Started Successfully!**\n"
                f"Version: v3.0\n"
                f"Date: {datetime.now():%Y-%m-%d %H:%M:%S}\n\n"
                f"Owner: `{Config.OWNER_ID}`\n"
                f"DB: {Config.DATABASE_NAME}"
            )
        except:
            pass

        await asyncio.Event().wait()

    def _register_handlers(self):
        """Register all Pyrogram handlers"""

        # 1. /start
        self.client.add_handler(MessageHandler(
            self.handlers.start_command,
            filters.command("start") & filters.private
        ))

        # 2. /settings  (fixed — now goes to real settings handler, not start_command)
        self.client.add_handler(MessageHandler(
            self.handlers.settings_command,
            filters.command("settings") & filters.private
        ))

        # 3. /addchannel
        self.client.add_handler(MessageHandler(
            lambda c, m: handle_addchannel_command(c, m),
            filters.command("addchannel") & filters.private
        ))

        # 4. /addadmin (owner only)
        self.client.add_handler(MessageHandler(
            lambda c, m: handle_addadmin_command(c, m),
            filters.command("addadmin") & filters.private
        ))

        # 5. /removeadmin (owner only)
        self.client.add_handler(MessageHandler(
            lambda c, m: handle_removeadmin_command(c, m),
            filters.command("removeadmin") & filters.private
        ))

        # 6. /grantuser (admin or owner)
        self.client.add_handler(MessageHandler(
            lambda c, m: handle_grantuser_command(c, m),
            filters.command("grantuser") & filters.private
        ))

        # 7. /revokeuser (owner only)
        self.client.add_handler(MessageHandler(
            lambda c, m: handle_revokeuser_command(c, m),
            filters.command("revokeuser") & filters.private
        ))

        # 8. /listadmins
        self.client.add_handler(MessageHandler(
            lambda c, m: handle_listadmins_command(c, m),
            filters.command("listadmins") & filters.private
        ))

        # 9. Video / Document
        self.client.add_handler(MessageHandler(
            self.handlers.video_handler,
            (filters.video | filters.document) & filters.private
        ))

        # 10. Callback queries
        self.client.add_handler(CallbackQueryHandler(
            self.handlers.callback_handler
        ))

        # 11. Text handler (for multi-step flows — MUST be last)
        self.client.add_handler(MessageHandler(
            self.handlers.text_handler,
            filters.text & filters.private & ~filters.command([
                "start", "settings", "addchannel", "addadmin",
                "removeadmin", "grantuser", "revokeuser", "listadmins"
            ])
        ))

    async def stop(self):
        await self.client.stop()


# ================================================================================
# 12. ENTRY POINT
# ================================================================================

async def main():
    bot = AutoSplitterBot()
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

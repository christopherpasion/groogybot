"""
Download History Manager - Persistent JSON-based download tracking
Stores history in ~/.novel_cache/download_history.json
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from threading import Lock
from datetime import datetime

logger = logging.getLogger(__name__)

# Default cache directory
CACHE_DIR = Path.home() / '.novel_cache'
HISTORY_FILE = CACHE_DIR / 'download_history.json'

# Maximum history entries per user (to prevent unbounded growth)
MAX_HISTORY_PER_USER = 500


class DownloadHistoryManager:
    """Manages download history with persistent JSON storage"""
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        """Singleton pattern to ensure single instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        self._file_lock = Lock()
        self._ensure_directories()
        self._load_history()
        self._initialized = True
    
    def _ensure_directories(self):
        """Create cache directories if they don't exist"""
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            logger.info(f"History directory: {CACHE_DIR}")
        except Exception as e:
            logger.error(f"Failed to create cache directories: {e}")
    
    def _load_history(self):
        """Load history from JSON file"""
        with self._file_lock:
            try:
                if HISTORY_FILE.exists():
                    with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                        self._history = json.load(f)
                    total_entries = sum(len(v) for v in self._history.values())
                    logger.info(f"Loaded history: {len(self._history)} users, {total_entries} entries")
                else:
                    self._history = {}
                    logger.info("No existing history file, starting fresh")
            except json.JSONDecodeError as e:
                logger.error(f"Corrupted history file, resetting: {e}")
                self._history = {}
            except Exception as e:
                logger.error(f"Failed to load history: {e}")
                self._history = {}
    
    def _save_history(self):
        """Save history to JSON file"""
        with self._file_lock:
            try:
                with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
                    json.dump(self._history, f, indent=2, ensure_ascii=False)
                logger.debug(f"Saved history for {len(self._history)} users")
            except Exception as e:
                logger.error(f"Failed to save history: {e}")
    
    def add_download(self, user_id: str, title: str, novel_url: str,
                     chapter_start: int, chapter_end: int, format: str,
                     metadata: Dict[str, Any] = None) -> bool:
        """Add a download entry to user's history"""
        user_id = str(user_id)
        
        entry = {
            'title': title,
            'novel_url': novel_url,
            'chapter_start': chapter_start,
            'chapter_end': chapter_end,
            'format': format,
            'timestamp': datetime.utcnow().isoformat(),
            'metadata': metadata or {}
        }
        
        # Initialize user history if needed
        if user_id not in self._history:
            self._history[user_id] = []
        
        # Add to beginning (most recent first)
        self._history[user_id].insert(0, entry)
        
        # Trim if exceeds max
        if len(self._history[user_id]) > MAX_HISTORY_PER_USER:
            self._history[user_id] = self._history[user_id][:MAX_HISTORY_PER_USER]
        
        self._save_history()
        logger.info(f"Added download to history: {title} for user {user_id}")
        return True
    
    def get_history(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get user's download history"""
        user_id = str(user_id)
        history = self._history.get(user_id, [])
        return history[:limit]
    
    def get_last_download(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user's most recent download"""
        user_id = str(user_id)
        history = self._history.get(user_id, [])
        return history[0] if history else None
    
    def get_library(self, user_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """Get user's library grouped by novel title"""
        user_id = str(user_id)
        history = self._history.get(user_id, [])
        
        library = {}
        for entry in history:
            title = entry.get('title', 'Unknown')
            # Use novel URL as unique key (titles might vary slightly)
            novel_url = entry.get('novel_url', '')
            key = novel_url or title
            
            if key not in library:
                library[key] = {
                    'title': title,
                    'novel_url': novel_url,
                    'downloads': []
                }
            library[key]['downloads'].append(entry)
        
        return library
    
    def check_duplicate(self, user_id: str, novel_url: str, 
                        chapter_start: int, chapter_end: int) -> Optional[Dict[str, Any]]:
        """Check if user has already downloaded these chapters"""
        user_id = str(user_id)
        history = self._history.get(user_id, [])
        
        for entry in history:
            if entry.get('novel_url') == novel_url:
                # Check if chapter range overlaps
                prev_start = entry.get('chapter_start', 0)
                prev_end = entry.get('chapter_end', 0)
                
                # Exact match
                if prev_start == chapter_start and prev_end == chapter_end:
                    return entry
                
                # Subset (requested range is within previous download)
                if prev_start <= chapter_start and prev_end >= chapter_end:
                    return entry
        
        return None
    
    def find_novel(self, user_id: str, query: str) -> Optional[Dict[str, Any]]:
        """Find a novel in user's history by title search"""
        user_id = str(user_id)
        history = self._history.get(user_id, [])
        query_lower = query.lower()
        
        for entry in history:
            title = entry.get('title', '').lower()
            if query_lower in title:
                return entry
        
        return None
    
    def get_stats(self, user_id: str) -> Dict[str, Any]:
        """Get download statistics for a user"""
        user_id = str(user_id)
        history = self._history.get(user_id, [])
        
        if not history:
            return {
                'total_downloads': 0,
                'unique_novels': 0,
                'total_chapters': 0,
                'favorite_format': None,
                'first_download': None,
                'last_download': None
            }
        
        unique_novels = set()
        total_chapters = 0
        format_counts = {}
        
        for entry in history:
            novel_url = entry.get('novel_url', entry.get('title', ''))
            unique_novels.add(novel_url)
            
            ch_start = entry.get('chapter_start', 0)
            ch_end = entry.get('chapter_end', 0)
            total_chapters += max(0, ch_end - ch_start + 1)
            
            fmt = entry.get('format', 'unknown')
            format_counts[fmt] = format_counts.get(fmt, 0) + 1
        
        favorite_format = max(format_counts, key=format_counts.get) if format_counts else None
        
        return {
            'total_downloads': len(history),
            'unique_novels': len(unique_novels),
            'total_chapters': total_chapters,
            'favorite_format': favorite_format,
            'first_download': history[-1].get('timestamp') if history else None,
            'last_download': history[0].get('timestamp') if history else None
        }
    
    def clear_history(self, user_id: str) -> bool:
        """Clear all history for a user"""
        user_id = str(user_id)
        if user_id in self._history:
            del self._history[user_id]
            self._save_history()
            logger.info(f"Cleared history for user {user_id}")
            return True
        return False


# Global instance
history_manager = DownloadHistoryManager()


# Convenience functions
def add_download(user_id: str, title: str, novel_url: str,
                 chapter_start: int, chapter_end: int, format: str,
                 metadata: Dict[str, Any] = None) -> bool:
    """Add a download to history"""
    return history_manager.add_download(user_id, title, novel_url,
                                        chapter_start, chapter_end, format, metadata)


def get_history(user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Get user's download history"""
    return history_manager.get_history(user_id, limit)


def get_last_download(user_id: str) -> Optional[Dict[str, Any]]:
    """Get user's last download"""
    return history_manager.get_last_download(user_id)


def get_library(user_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """Get user's library"""
    return history_manager.get_library(user_id)


def check_duplicate(user_id: str, novel_url: str,
                    chapter_start: int, chapter_end: int) -> Optional[Dict[str, Any]]:
    """Check for duplicate download"""
    return history_manager.check_duplicate(user_id, novel_url, chapter_start, chapter_end)


def get_stats(user_id: str) -> Dict[str, Any]:
    """Get user's download stats"""
    return history_manager.get_stats(user_id)

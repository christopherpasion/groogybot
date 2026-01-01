"""
Novel Cache System

Caches novel metadata and chapter content to reduce requests and avoid IP blocks.
- Novel info (title, description, chapter list) cached for 24 hours
- Chapter content cached permanently (doesn't change)
- Only fetches new chapters that aren't in cache
"""

import os
import json
import hashlib
import time
from pathlib import Path
from typing import Optional, Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class NovelCache:
    """Cache for novel data to reduce HTTP requests and avoid IP blocking."""
    
    def __init__(self, cache_dir: str = None):
        """Initialize cache with specified directory."""
        if cache_dir is None:
            # Default to user's app data folder
            cache_dir = os.path.join(os.path.expanduser("~"), ".novel_cache")
        
        self.cache_dir = Path(cache_dir)
        self.novels_dir = self.cache_dir / "novels"
        self.chapters_dir = self.cache_dir / "chapters"
        self.search_dir = self.cache_dir / "search"
        
        # Create directories
        self.novels_dir.mkdir(parents=True, exist_ok=True)
        self.chapters_dir.mkdir(parents=True, exist_ok=True)
        self.search_dir.mkdir(parents=True, exist_ok=True)
        
        # Cache expiry times (in seconds)
        self.SEARCH_CACHE_TTL = 3600  # 1 hour for search results
        self.NOVEL_INFO_TTL = 86400   # 24 hours for novel info
        self.CHAPTER_TTL = None       # Never expire chapter content
        
        logger.info(f"[Cache] Initialized at {self.cache_dir}")
    
    def _url_hash(self, url: str) -> str:
        """Create a safe filename hash from URL."""
        return hashlib.md5(url.encode()).hexdigest()
    
    def _is_expired(self, cache_file: Path, ttl: Optional[int]) -> bool:
        """Check if cache file is expired."""
        if ttl is None:
            return False  # Never expires
        if not cache_file.exists():
            return True
        age = time.time() - cache_file.stat().st_mtime
        return age > ttl
    
    # === Search Cache ===
    
    def get_search_results(self, query: str, source: str = None) -> Optional[List[Dict]]:
        """Get cached search results for a query."""
        cache_key = f"{query}_{source or 'all'}"
        cache_file = self.search_dir / f"{self._url_hash(cache_key)}.json"
        
        if self._is_expired(cache_file, self.SEARCH_CACHE_TTL):
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.debug(f"[Cache] Search hit for '{query}'")
                return data.get('results', [])
        except Exception:
            return None
    
    def set_search_results(self, query: str, results: List[Dict], source: str = None):
        """Cache search results."""
        cache_key = f"{query}_{source or 'all'}"
        cache_file = self.search_dir / f"{self._url_hash(cache_key)}.json"
        
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'query': query,
                    'source': source,
                    'timestamp': time.time(),
                    'results': results
                }, f, ensure_ascii=False, indent=2)
            logger.debug(f"[Cache] Saved {len(results)} search results for '{query}'")
        except Exception as e:
            logger.warning(f"[Cache] Failed to save search: {e}")
    
    # === Novel Info Cache ===
    
    def get_novel_info(self, novel_url: str) -> Optional[Dict]:
        """Get cached novel info (title, description, chapter list)."""
        cache_file = self.novels_dir / f"{self._url_hash(novel_url)}.json"
        
        if self._is_expired(cache_file, self.NOVEL_INFO_TTL):
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.debug(f"[Cache] Novel info hit for {novel_url[:50]}")
                return data
        except Exception:
            return None
    
    def set_novel_info(self, novel_url: str, info: Dict):
        """Cache novel info."""
        cache_file = self.novels_dir / f"{self._url_hash(novel_url)}.json"
        
        try:
            info['_cached_at'] = time.time()
            info['_url'] = novel_url
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)
            logger.debug(f"[Cache] Saved novel info for {novel_url[:50]}")
        except Exception as e:
            logger.warning(f"[Cache] Failed to save novel info: {e}")
    
    def get_cached_chapters(self, novel_url: str) -> List[str]:
        """Get list of cached chapter URLs for a novel."""
        novel_hash = self._url_hash(novel_url)
        novel_chapters_dir = self.chapters_dir / novel_hash
        
        if not novel_chapters_dir.exists():
            return []
        
        cached = []
        for f in novel_chapters_dir.glob("*.json"):
            try:
                with open(f, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    if '_url' in data:
                        cached.append(data['_url'])
            except Exception:
                pass
        return cached
    
    # === Chapter Content Cache ===
    
    def get_chapter(self, chapter_url: str, novel_url: str = None) -> Optional[Dict]:
        """Get cached chapter content."""
        # Try novel-specific directory first
        if novel_url:
            novel_hash = self._url_hash(novel_url)
            cache_file = self.chapters_dir / novel_hash / f"{self._url_hash(chapter_url)}.json"
        else:
            cache_file = self.chapters_dir / f"{self._url_hash(chapter_url)}.json"
        
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.debug(f"[Cache] Chapter hit for {chapter_url[:50]}")
                return data
        except Exception:
            return None
    
    def set_chapter(self, chapter_url: str, content: Dict, novel_url: str = None):
        """Cache chapter content."""
        if novel_url:
            novel_hash = self._url_hash(novel_url)
            chapter_dir = self.chapters_dir / novel_hash
            chapter_dir.mkdir(exist_ok=True)
            cache_file = chapter_dir / f"{self._url_hash(chapter_url)}.json"
        else:
            cache_file = self.chapters_dir / f"{self._url_hash(chapter_url)}.json"
        
        try:
            content['_cached_at'] = time.time()
            content['_url'] = chapter_url
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(content, f, ensure_ascii=False, indent=2)
            logger.debug(f"[Cache] Saved chapter {chapter_url[:50]}")
        except Exception as e:
            logger.warning(f"[Cache] Failed to save chapter: {e}")
    
    # === Utility Methods ===
    
    def get_uncached_chapters(self, chapter_urls: List[str], novel_url: str = None) -> List[str]:
        """Get list of chapters that are NOT in cache (need to be fetched)."""
        uncached = []
        for url in chapter_urls:
            if self.get_chapter(url, novel_url) is None:
                uncached.append(url)
        return uncached
    
    def clear_expired(self):
        """Remove expired cache entries."""
        now = time.time()
        removed = 0
        
        # Clear expired search results
        for f in self.search_dir.glob("*.json"):
            if self._is_expired(f, self.SEARCH_CACHE_TTL):
                f.unlink()
                removed += 1
        
        # Clear expired novel info
        for f in self.novels_dir.glob("*.json"):
            if self._is_expired(f, self.NOVEL_INFO_TTL):
                f.unlink()
                removed += 1
        
        if removed:
            logger.info(f"[Cache] Cleared {removed} expired entries")
    
    def clear_all(self):
        """Clear entire cache."""
        import shutil
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        self.__init__(str(self.cache_dir))
        logger.info("[Cache] Cleared all cache")

    def clear_novel_chapters(self, novel_url: str):
        """Clear all cached chapters for a specific novel."""
        novel_hash = self._url_hash(novel_url)
        novel_chapters_dir = self.chapters_dir / novel_hash
        if novel_chapters_dir.exists():
            import shutil
            shutil.rmtree(novel_chapters_dir, ignore_errors=True)
            logger.info(f"[Cache] Cleared chapters cache for {novel_url[:80]}")
    
    def get_stats(self) -> Dict:
        """Get cache statistics."""
        search_count = len(list(self.search_dir.glob("*.json")))
        novel_count = len(list(self.novels_dir.glob("*.json")))
        chapter_count = sum(1 for _ in self.chapters_dir.rglob("*.json"))
        
        # Calculate total size
        total_size = sum(f.stat().st_size for f in self.cache_dir.rglob("*") if f.is_file())
        
        return {
            'search_results': search_count,
            'novels': novel_count,
            'chapters': chapter_count,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'cache_dir': str(self.cache_dir)
        }
    
    # === Download Progress (Resume Support) ===
    
    def save_download_progress(self, user_id: str, novel_url: str, progress: Dict):
        """Save download progress for resume capability.
        
        Args:
            user_id: Discord user ID
            novel_url: URL of the novel being downloaded
            progress: Dict with keys: title, chapter_start, chapter_end, 
                     completed_chapters (list), failed_chapters (list), status
        """
        progress_dir = self.cache_dir / "progress"
        progress_dir.mkdir(exist_ok=True)
        
        key = f"{user_id}_{self._url_hash(novel_url)}"
        cache_file = progress_dir / f"{key}.json"
        
        try:
            progress['_updated_at'] = time.time()
            progress['_user_id'] = user_id
            progress['_novel_url'] = novel_url
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(progress, f, ensure_ascii=False, indent=2)
            logger.debug(f"[Cache] Saved progress for user {user_id}: {len(progress.get('completed_chapters', []))} chapters")
        except Exception as e:
            logger.warning(f"[Cache] Failed to save progress: {e}")
    
    def get_download_progress(self, user_id: str, novel_url: str = None) -> Optional[Dict]:
        """Get download progress for a user.
        
        If novel_url is provided, returns progress for that specific novel.
        Otherwise, returns the most recent incomplete download.
        """
        progress_dir = self.cache_dir / "progress"
        if not progress_dir.exists():
            return None
        
        if novel_url:
            key = f"{user_id}_{self._url_hash(novel_url)}"
            cache_file = progress_dir / f"{key}.json"
            
            if cache_file.exists():
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except Exception:
                    return None
        else:
            # Find most recent incomplete download for this user
            user_files = list(progress_dir.glob(f"{user_id}_*.json"))
            if not user_files:
                return None
            
            # Sort by modification time, newest first
            user_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
            
            for f in user_files:
                try:
                    with open(f, 'r', encoding='utf-8') as file:
                        data = json.load(file)
                        if data.get('status') in ['in_progress', 'failed', 'partial']:
                            return data
                except Exception:
                    continue
        
        return None
    
    def clear_download_progress(self, user_id: str, novel_url: str = None):
        """Clear download progress (after successful completion or manual cancel)."""
        progress_dir = self.cache_dir / "progress"
        if not progress_dir.exists():
            return
        
        if novel_url:
            key = f"{user_id}_{self._url_hash(novel_url)}"
            cache_file = progress_dir / f"{key}.json"
            if cache_file.exists():
                cache_file.unlink()
                logger.debug(f"[Cache] Cleared progress for user {user_id}")
        else:
            # Clear all progress for user
            for f in progress_dir.glob(f"{user_id}_*.json"):
                f.unlink()
            logger.debug(f"[Cache] Cleared all progress for user {user_id}")


# Global cache instance
_cache = None

def get_cache() -> NovelCache:
    """Get or create the global cache instance."""
    global _cache
    if _cache is None:
        _cache = NovelCache()
    return _cache

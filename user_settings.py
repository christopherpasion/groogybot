"""
User Settings Manager - Persistent JSON-based user preferences
Stores settings in ~/.novel_cache/user_settings.json
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from threading import Lock

logger = logging.getLogger(__name__)

# Default cache directory
CACHE_DIR = Path.home() / '.novel_cache'
SETTINGS_FILE = CACHE_DIR / 'user_settings.json'
CUSTOM_CSS_DIR = CACHE_DIR / 'custom_css'

# Default settings for all users
DEFAULT_SETTINGS = {
    'epub_format': 'epub2',      # epub2 (default) or epub3
    'style': 'classic',          # classic, modern, compact, cozy
    'audio': False,              # Include TTS audio (Coffee+ only)
    'show_notes': True,          # Show TL notes/footnotes at chapter end (Coffee+ can hide)
    'voice': 'female',           # TTS voice: male or female (Coffee+ only)
}

# Available styles with CSS definitions
EPUB_STYLES = {
    'classic': {
        'name': 'Classic',
        'description': 'Serif, spaced paragraphs, justified text',
        'font_family': 'Georgia, "Times New Roman", serif',
        'paragraph_indent': '0',
        'paragraph_spacing': '0.8em',
        'text_align': 'justify',
        'line_height': '1.5',
        'chapter_title_align': 'center',
    },
    'modern': {
        'name': 'Modern',
        'description': 'Sans-serif, spaced paragraphs, left-aligned',
        'font_family': 'Arial, Helvetica, sans-serif',
        'paragraph_indent': '0',
        'paragraph_spacing': '1em',
        'text_align': 'left',
        'line_height': '1.6',
        'chapter_title_align': 'left',
    },
    'compact': {
        'name': 'Compact',
        'description': 'Narrow margins, tighter spacing, more text per page',
        'font_family': 'Georgia, serif',
        'paragraph_indent': '0',
        'paragraph_spacing': '0.5em',
        'text_align': 'justify',
        'line_height': '1.3',
        'chapter_title_align': 'center',
    },
    'cozy': {
        'name': 'Cozy',
        'description': 'Larger text, more spacing, easy reading',
        'font_family': 'Arial, sans-serif',
        'paragraph_indent': '0',
        'paragraph_spacing': '1.2em',
        'text_align': 'left',
        'line_height': '1.8',
        'chapter_title_align': 'center',
    },
}


class UserSettingsManager:
    """Manages user settings with persistent JSON storage"""
    
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
            
        self._settings: Dict[str, Dict[str, Any]] = {}
        self._file_lock = Lock()
        self._ensure_directories()
        self._load_settings()
        self._initialized = True
    
    def _ensure_directories(self):
        """Create cache directories if they don't exist"""
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            CUSTOM_CSS_DIR.mkdir(parents=True, exist_ok=True)
            logger.info(f"Settings directory: {CACHE_DIR}")
        except Exception as e:
            logger.error(f"Failed to create cache directories: {e}")
    
    def _load_settings(self):
        """Load settings from JSON file"""
        with self._file_lock:
            try:
                if SETTINGS_FILE.exists():
                    with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                        self._settings = json.load(f)
                    logger.info(f"Loaded settings for {len(self._settings)} users")
                else:
                    self._settings = {}
                    logger.info("No existing settings file, starting fresh")
            except json.JSONDecodeError as e:
                logger.error(f"Corrupted settings file, resetting: {e}")
                self._settings = {}
            except Exception as e:
                logger.error(f"Failed to load settings: {e}")
                self._settings = {}
    
    def _save_settings(self):
        """Save settings to JSON file"""
        with self._file_lock:
            try:
                with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(self._settings, f, indent=2, ensure_ascii=False)
                logger.debug(f"Saved settings for {len(self._settings)} users")
            except Exception as e:
                logger.error(f"Failed to save settings: {e}")
    
    def get_user_settings(self, user_id: str) -> Dict[str, Any]:
        """Get all settings for a user, with defaults for missing values"""
        user_id = str(user_id)
        user_settings = self._settings.get(user_id, {})
        
        # Merge with defaults
        result = DEFAULT_SETTINGS.copy()
        result.update(user_settings)
        return result
    
    def get_setting(self, user_id: str, key: str) -> Any:
        """Get a single setting for a user"""
        user_id = str(user_id)
        user_settings = self._settings.get(user_id, {})
        return user_settings.get(key, DEFAULT_SETTINGS.get(key))
    
    def set_setting(self, user_id: str, key: str, value: Any) -> bool:
        """Set a single setting for a user"""
        user_id = str(user_id)
        
        # Validate setting key
        if key not in DEFAULT_SETTINGS:
            logger.warning(f"Invalid setting key: {key}")
            return False
        
        # Validate values
        if key == 'epub_format' and value not in ['epub2', 'epub3']:
            return False
        if key == 'style' and value not in EPUB_STYLES:
            return False
        if key == 'audio' and not isinstance(value, bool):
            return False
        if key == 'show_notes' and not isinstance(value, bool):
            return False
        if key == 'voice' and value not in ['male', 'female']:
            return False
        
        # Update settings
        if user_id not in self._settings:
            self._settings[user_id] = {}
        
        self._settings[user_id][key] = value
        self._save_settings()
        logger.info(f"User {user_id} set {key} = {value}")
        return True
    
    def toggle_audio(self, user_id: str) -> bool:
        """Toggle audio setting, returns new value"""
        current = self.get_setting(user_id, 'audio')
        new_value = not current
        self.set_setting(user_id, 'audio', new_value)
        return new_value
    
    def toggle_notes(self, user_id: str) -> bool:
        """Toggle show_notes setting, returns new value"""
        current = self.get_setting(user_id, 'show_notes')
        new_value = not current
        self.set_setting(user_id, 'show_notes', new_value)
        return new_value
    
    def toggle_voice(self, user_id: str) -> str:
        """Toggle voice between male/female, returns new value"""
        current = self.get_setting(user_id, 'voice')
        new_value = 'male' if current == 'female' else 'female'
        self.set_setting(user_id, 'voice', new_value)
        return new_value
    
    def set_voice(self, user_id: str, voice: str) -> bool:
        """Set voice gender (male or female)"""
        if voice not in ['male', 'female']:
            return False
        return self.set_setting(user_id, 'voice', voice)
    
    def set_epub_format(self, user_id: str, format: str) -> bool:
        """Set EPUB format (epub2 or epub3)"""
        if format not in ['epub2', 'epub3']:
            return False
        return self.set_setting(user_id, 'epub_format', format)
    
    def set_style(self, user_id: str, style: str) -> bool:
        """Set EPUB style"""
        if style not in EPUB_STYLES:
            return False
        return self.set_setting(user_id, 'style', style)
    
    def reset_settings(self, user_id: str):
        """Reset user settings to defaults"""
        user_id = str(user_id)
        if user_id in self._settings:
            del self._settings[user_id]
            self._save_settings()
            logger.info(f"Reset settings for user {user_id}")
    
    def get_style_css(self, user_id: str, tier: str = 'verified') -> str:
        """Get CSS for user's selected style"""
        user_id = str(user_id)
        
        # Check for custom CSS (Sponsor only)
        if tier.lower() == 'sponsor':
            custom_css_file = CUSTOM_CSS_DIR / f"{user_id}.css"
            if custom_css_file.exists():
                try:
                    with open(custom_css_file, 'r', encoding='utf-8') as f:
                        custom_css = f.read()
                    if custom_css.strip():
                        return custom_css
                except Exception as e:
                    logger.warning(f"Failed to load custom CSS for {user_id}: {e}")
        
        # Get style settings
        style_name = self.get_setting(user_id, 'style')
        style = EPUB_STYLES.get(style_name, EPUB_STYLES['classic'])
        
        # Generate CSS
        css = f'''
body {{
    font-family: {style['font_family']};
    font-size: 12pt;
    line-height: {style['line_height']};
}}

p {{
    text-indent: {style['paragraph_indent']};
    margin-bottom: {style['paragraph_spacing']};
    text-align: {style['text_align']};
}}

h1, h2 {{
    text-align: {style['chapter_title_align']};
    margin-top: 1.5em;
    margin-bottom: 0.8em;
}}

h1 {{ font-size: 1.8em; }}
h2 {{ font-size: 1.4em; }}

.title-page {{
    text-align: center;
}}

.disclaimer {{
    font-size: 0.9em;
    margin: 2em;
}}
'''
        return css.strip()
    
    def save_custom_css(self, user_id: str, css_content: str) -> bool:
        """Save custom CSS for Sponsor users"""
        user_id = str(user_id)
        
        try:
            custom_css_file = CUSTOM_CSS_DIR / f"{user_id}.css"
            with open(custom_css_file, 'w', encoding='utf-8') as f:
                f.write(css_content)
            logger.info(f"Saved custom CSS for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to save custom CSS: {e}")
            return False
    
    def delete_custom_css(self, user_id: str) -> bool:
        """Delete custom CSS for a user"""
        user_id = str(user_id)
        custom_css_file = CUSTOM_CSS_DIR / f"{user_id}.css"
        
        try:
            if custom_css_file.exists():
                custom_css_file.unlink()
                logger.info(f"Deleted custom CSS for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete custom CSS: {e}")
            return False
    
    def get_settings_display(self, user_id: str, tier: str = 'verified') -> str:
        """Get formatted settings display for Discord"""
        settings = self.get_user_settings(user_id)
        
        # Format values
        format_display = "EPUB 3.0" if settings['epub_format'] == 'epub3' else "EPUB 2.0"
        style_info = EPUB_STYLES.get(settings['style'], EPUB_STYLES['classic'])
        style_display = f"{style_info['name']}"
        audio_display = "On" if settings['audio'] else "Off"
        notes_display = "Show at end" if settings.get('show_notes', True) else "Hidden"
        voice_display = settings.get('voice', 'female').capitalize()
        
        # Check for custom CSS
        has_custom_css = False
        if tier.lower() == 'sponsor':
            custom_css_file = CUSTOM_CSS_DIR / f"{user_id}.css"
            has_custom_css = custom_css_file.exists()
        
        lines = [
            "⚙️ **Your Settings:**",
            f"• Format: {format_display}",
            f"• Style: {style_display}",
        ]
        
        # Only show audio, voice, and notes for Coffee+
        if tier.lower() in ['coffee', 'catnip', 'sponsor']:
            lines.append(f"• Audio: {audio_display}")
            lines.append(f"• Voice: {voice_display}")
            lines.append(f"• TL Notes: {notes_display}")
        
        # Show custom CSS status for Sponsor
        if tier.lower() == 'sponsor':
            css_status = "✅ Active" if has_custom_css else "Not set"
            lines.append(f"• Custom CSS: {css_status}")
        
        # Add hint based on tier
        if tier.lower() == 'verified':
            lines.append("")
            lines.append("☕ *Upgrade to Coffee for more styles, EPUB 3.0, audio & voice*")
        else:
            lines.append("")
            lines.append("*!settings epub2 | epub3 | style [name] | audio | voice | notes | reset*")
        
        return "\n".join(lines)


# Global instance
settings_manager = UserSettingsManager()


# Convenience functions
def get_user_settings(user_id: str) -> Dict[str, Any]:
    """Get all settings for a user"""
    return settings_manager.get_user_settings(user_id)


def get_setting(user_id: str, key: str) -> Any:
    """Get a single setting"""
    return settings_manager.get_setting(user_id, key)


def set_setting(user_id: str, key: str, value: Any) -> bool:
    """Set a single setting"""
    return settings_manager.set_setting(user_id, key, value)


def get_style_css(user_id: str, tier: str = 'verified') -> str:
    """Get CSS for user's style"""
    return settings_manager.get_style_css(user_id, tier)


def get_settings_display(user_id: str, tier: str = 'verified') -> str:
    """Get formatted settings for Discord display"""
    return settings_manager.get_settings_display(user_id, tier)

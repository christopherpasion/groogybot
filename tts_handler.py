"""
TTS Handler using edge-tts for free, high-quality text-to-speech
"""

import asyncio
import logging
import os
import tempfile
import re
from typing import Optional, Dict, Any
from collections import deque

logger = logging.getLogger(__name__)

# Try to import edge_tts
try:
    import edge_tts
    EDGE_TTS_AVAILABLE = True
except ImportError:
    EDGE_TTS_AVAILABLE = False
    logger.warning("edge-tts not installed. Run: pip install edge-tts")


# Voice mappings for edge-tts
EDGE_VOICES = {
    'en': {
        'male': 'en-US-GuyNeural',
        'female': 'en-US-JennyNeural',
    },
    'zh': {
        'male': 'zh-CN-YunxiNeural',
        'female': 'zh-CN-XiaoxiaoNeural',
    },
    'ja': {
        'male': 'ja-JP-KeitaNeural',
        'female': 'ja-JP-NanamiNeural',
    },
    'ko': {
        'male': 'ko-KR-InJoonNeural',
        'female': 'ko-KR-SunHiNeural',
    },
    'es': {
        'male': 'es-ES-AlvaroNeural',
        'female': 'es-ES-ElviraNeural',
    },
    'fr': {
        'male': 'fr-FR-HenriNeural',
        'female': 'fr-FR-DeniseNeural',
    },
    'de': {
        'male': 'de-DE-ConradNeural',
        'female': 'de-DE-KatjaNeural',
    },
    'pt': {
        'male': 'pt-BR-AntonioNeural',
        'female': 'pt-BR-FranciscaNeural',
    },
}


def detect_language(text: str) -> str:
    """Detect language from text based on character analysis"""
    if not text:
        return 'en'
    
    # Count character types
    cjk_chars = len(re.findall(r'[\u4e00-\u9fff]', text))  # Chinese
    hiragana = len(re.findall(r'[\u3040-\u309f]', text))  # Japanese hiragana
    katakana = len(re.findall(r'[\u30a0-\u30ff]', text))  # Japanese katakana
    hangul = len(re.findall(r'[\uac00-\ud7af]', text))  # Korean
    
    total = len(text)
    if total == 0:
        return 'en'
    
    # Determine language based on character frequency
    if (hiragana + katakana) / total > 0.1:
        return 'ja'
    if hangul / total > 0.1:
        return 'ko'
    if cjk_chars / total > 0.2:
        return 'zh'
    
    return 'en'


class TTSHandler:
    """Handles TTS generation and playback"""
    
    def __init__(self):
        self.queue = deque()
        self.is_playing = False
        self.current_voice_client = None
        self.guild_settings: Dict[int, Dict[str, Any]] = {}
        self._stop_requested = False
        
    def get_guild_settings(self, guild_id: int) -> Dict[str, Any]:
        """Get TTS settings for a guild"""
        if guild_id not in self.guild_settings:
            self.guild_settings[guild_id] = {
                'gender': 'female',
                'speed': 1.0,
                'stability': 0.5,
                'style': 'default',
            }
        return self.guild_settings[guild_id]
    
    def set_gender(self, guild_id: int, gender: str) -> bool:
        """Set voice gender for guild"""
        if gender.lower() not in ('male', 'female'):
            return False
        self.get_guild_settings(guild_id)['gender'] = gender.lower()
        return True
    
    def set_speed(self, guild_id: int, speed: float) -> bool:
        """Set TTS speed for guild (0.5 - 2.0)"""
        if not 0.5 <= speed <= 2.0:
            return False
        self.get_guild_settings(guild_id)['speed'] = speed
        return True
    
    def set_stability(self, guild_id: int, stability: float) -> bool:
        """Set voice stability for guild (0.0 - 1.0)"""
        if not 0.0 <= stability <= 1.0:
            return False
        self.get_guild_settings(guild_id)['stability'] = stability
        return True
    
    def set_style(self, guild_id: int, style: str) -> bool:
        """Set voice style for guild"""
        valid_styles = ('default', 'cheerful', 'sad', 'angry', 'terrified')
        if style.lower() not in valid_styles:
            return False
        self.get_guild_settings(guild_id)['style'] = style.lower()
        return True
    
    def stop(self):
        """Stop current playback"""
        self._stop_requested = True
        if self.current_voice_client and self.current_voice_client.is_playing():
            self.current_voice_client.stop()
        self.clear_queue()
    
    def clear_queue(self):
        """Clear the TTS queue"""
        self.queue.clear()
        self.is_playing = False
        self._stop_requested = False
    
    async def generate_speech(self, text: str, guild_id: int) -> Optional[str]:
        """Generate speech audio file using edge-tts"""
        if not EDGE_TTS_AVAILABLE:
            logger.error("edge-tts not available")
            return None
        
        try:
            settings = self.get_guild_settings(guild_id)
            language = detect_language(text)
            gender = settings['gender']
            
            # Get voice
            voice_map = EDGE_VOICES.get(language, EDGE_VOICES['en'])
            voice = voice_map.get(gender, voice_map['female'])
            
            # Calculate rate adjustment
            speed = settings['speed']
            rate = f"+{int((speed - 1) * 100)}%" if speed >= 1 else f"{int((speed - 1) * 100)}%"
            
            # Generate audio
            communicate = edge_tts.Communicate(text, voice, rate=rate)
            
            # Create temp file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp3')
            temp_path = temp_file.name
            temp_file.close()
            
            await communicate.save(temp_path)
            
            if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                return temp_path
            else:
                logger.error("Generated audio file is empty")
                return None
                
        except Exception as e:
            logger.error(f"TTS generation error: {e}")
            return None
    
    async def add_to_queue(self, username: str, content: str, guild_id: int, 
                          voice_client):
        """Add message to TTS queue and process"""
        if not EDGE_TTS_AVAILABLE:
            logger.warning("TTS not available - edge-tts not installed")
            return
        
        if not voice_client or not voice_client.is_connected():
            return
        
        # Format message
        tts_text = f"{username} says: {content}"
        
        # Add to queue
        self.queue.append({
            'text': tts_text,
            'guild_id': guild_id,
            'voice_client': voice_client
        })
        
        # Process queue if not already playing
        if not self.is_playing:
            await self._process_queue()
    
    async def _process_queue(self):
        """Process TTS queue"""
        if self.is_playing:
            return
        
        self.is_playing = True
        self._stop_requested = False
        
        while self.queue and not self._stop_requested:
            item = self.queue.popleft()
            
            voice_client = item['voice_client']
            if not voice_client or not voice_client.is_connected():
                continue
            
            self.current_voice_client = voice_client
            
            # Generate audio
            audio_path = await self.generate_speech(item['text'], item['guild_id'])
            
            if not audio_path or self._stop_requested:
                continue
            
            try:
                # Play audio using discord.py
                import discord
                
                # Wait for current audio to finish if any
                while voice_client.is_playing() and not self._stop_requested:
                    await asyncio.sleep(0.1)
                
                if self._stop_requested:
                    break
                
                # Play the audio
                audio_source = discord.FFmpegPCMAudio(audio_path)
                voice_client.play(audio_source)
                
                # Wait for playback to complete
                while voice_client.is_playing() and not self._stop_requested:
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"TTS playback error: {e}")
            finally:
                # Cleanup temp file
                try:
                    if audio_path and os.path.exists(audio_path):
                        os.remove(audio_path)
                except Exception:
                    pass
        
        self.is_playing = False
        self.current_voice_client = None
    
    async def generate_audiobook(self, novel_data: dict, user_id: str, 
                                 guild_id: int = 0) -> Optional[str]:
        """Generate audiobook from novel chapters
        
        This is a Coffee+ tier feature. Returns path to generated audiobook.
        """
        if not EDGE_TTS_AVAILABLE:
            logger.error("edge-tts not available for audiobook generation")
            return None
        
        try:
            import tempfile
            import os
            from utils import clean_filename
            
            novel_name = novel_data.get('title', 'Novel')
            chapters = novel_data.get('chapters', [])
            
            if not chapters:
                return None
            
            # Create temp directory for chapter audio
            temp_dir = tempfile.mkdtemp(prefix='audiobook_')
            audio_files = []
            
            settings = self.get_guild_settings(guild_id)
            gender = settings.get('gender', 'female')
            
            for i, chapter in enumerate(chapters):
                content = chapter.get('content', '')
                title = chapter.get('title', f'Chapter {i + 1}')
                
                if not content:
                    continue
                
                # Detect language from content
                language = detect_language(content[:500])
                voice_map = EDGE_VOICES.get(language, EDGE_VOICES['en'])
                voice = voice_map.get(gender, voice_map['female'])
                
                # Prepare text
                full_text = f"{title}.\n\n{content}"
                
                # Generate audio
                communicate = edge_tts.Communicate(full_text, voice)
                
                audio_path = os.path.join(temp_dir, f'chapter_{i:04d}.mp3')
                await communicate.save(audio_path)
                
                if os.path.exists(audio_path):
                    audio_files.append(audio_path)
                
                logger.info(f"Generated audio for chapter {i + 1}/{len(chapters)}")
            
            if not audio_files:
                return None
            
            # TODO: Combine audio files into single audiobook
            # For now, return the directory with separate chapter files
            return temp_dir
            
        except Exception as e:
            logger.error(f"Audiobook generation error: {e}")
            return None


# Singleton instance
tts_handler = TTSHandler()

from ebooklib import epub
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
import os
import re
import requests
import logging
import tempfile
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

COVER_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".novel_cache", "covers")
os.makedirs(COVER_CACHE_DIR, exist_ok=True)

# Try to import user settings for style support
try:
    from user_settings import get_style_css, get_setting, EPUB_STYLES
    USER_SETTINGS_AVAILABLE = True
except ImportError:
    USER_SETTINGS_AVAILABLE = False
    def get_style_css(user_id, tier='verified'):
        return None
    def get_setting(user_id, key):
        return None


# HTTP headers for requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}


def _cover_cache_paths(title: str) -> Tuple[str, str]:
    cleaned = (title or "untitled").strip().lower() or "untitled"
    key = hashlib.md5(cleaned.encode('utf-8')).hexdigest()
    data_path = os.path.join(COVER_CACHE_DIR, f"{key}.bin")
    meta_path = os.path.join(COVER_CACHE_DIR, f"{key}.meta")
    return data_path, meta_path


def _read_cover_cache(title: str) -> Optional[Tuple[bytes, str]]:
    data_path, meta_path = _cover_cache_paths(title)
    if not os.path.exists(data_path):
        return None
    try:
        with open(data_path, 'rb') as f:
            data = f.read()
        content_type = 'image/jpeg'
        if os.path.exists(meta_path):
            with open(meta_path, 'r', encoding='utf-8') as m:
                stored = m.read().strip()
                if stored:
                    content_type = stored
        logger.info(f"Using cached cover for '{title}'")
        return data, content_type
    except Exception as e:
        logger.debug(f"Failed to read cover cache: {e}")
        return None


def _write_cover_cache(title: str, data: bytes, content_type: str) -> None:
    data_path, meta_path = _cover_cache_paths(title)
    try:
        with open(data_path, 'wb') as f:
            f.write(data)
        with open(meta_path, 'w', encoding='utf-8') as m:
            m.write(content_type or 'image/jpeg')
        logger.debug(f"Cached cover for '{title}' -> {data_path}")
    except Exception as e:
        logger.debug(f"Failed to write cover cache: {e}")


def compress_image(image_data: bytes, max_size: tuple = (800, 1200), quality: int = 75) -> Tuple[bytes, str]:
    """Compress and optimize an image for EPUB/PDF.
    
    Converts to JPEG for smaller file size, resizes if too large.
    
    Args:
        image_data: Raw image bytes
        max_size: Maximum (width, height) - will resize proportionally
        quality: JPEG quality (1-100, lower = smaller file)
    
    Returns:
        (compressed_bytes, media_type)
    """
    try:
        from PIL import Image
        from io import BytesIO
        
        # Load image
        img = Image.open(BytesIO(image_data))
        
        # Convert to RGB if necessary (for JPEG)
        if img.mode in ('RGBA', 'P', 'LA'):
            # Create white background for transparent images
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            if 'A' in img.mode:
                background.paste(img, mask=img.split()[-1])
            else:
                background.paste(img)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Resize if too large
        if img.width > max_size[0] or img.height > max_size[1]:
            img.thumbnail(max_size, Image.Resampling.LANCZOS)
            logger.debug(f"Resized image to {img.size}")
        
        # Compress to JPEG
        output = BytesIO()
        img.save(output, format='JPEG', quality=quality, optimize=True)
        compressed = output.getvalue()
        
        logger.debug(f"Compressed image: {len(image_data)} -> {len(compressed)} bytes ({100*len(compressed)//len(image_data)}%)")
        
        return compressed, 'image/jpeg'
        
    except Exception as e:
        logger.warning(f"Image compression failed: {e}, using original")
        return image_data, 'image/jpeg'


def fetch_cover_image(title: str, source_url: str = None, metadata: dict = None) -> Tuple[Optional[bytes], str]:
    """
    Fetch cover image with fallback chain:
    1. Source website (from metadata)
    2. NovelUpdates.com search  
    3. Google Books API
    4. Open Library API
    5. Generate a placeholder cover
    
    Returns: (image_bytes, media_type) or (None, '') if not found
    """
    cached = _read_cover_cache(title)
    if cached:
        return cached
    
    # 1. Try from metadata first
    if metadata:
        cover_url = metadata.get('cover_image', '')
        if cover_url and not cover_url.endswith('placeholder.jpg') and 'placeholder' not in cover_url.lower():
            try:
                resp = requests.get(cover_url, timeout=10, headers=HEADERS)
                if resp.status_code == 200 and len(resp.content) > 1000:
                    content_type = resp.headers.get('content-type', 'image/jpeg')
                    compressed, media_type = compress_image(resp.content)
                    logger.info(f"Got cover from source: {cover_url}")
                    _write_cover_cache(title, compressed, media_type or content_type)
                    return compressed, media_type or content_type
            except Exception as e:
                logger.warning(f"Failed to get cover from source: {e}")
    
    # 2. Try NovelUpdates.com
    try:
        search_title = re.sub(r'[^\w\s]', '', title).strip()
        search_url = f"https://www.novelupdates.com/?s={requests.utils.quote(search_title)}&post_type=seriesplan"
        resp = requests.get(search_url, timeout=10, headers=HEADERS)
        if resp.status_code == 200:
            # NovelUpdates patterns for cover images
            patterns = [
                r'<img[^>]+class="[^"]*list-seriescover[^"]*"[^>]+src="([^"]+)"',
                r'<img[^>]+src="(https://cdn\.novelupdates\.com/imgmid/[^"]+)"',
                r'<img[^>]+src="(https://cdn\.novelupdates\.com/images/[^"]+)"',
                r'src="(https://[^"]*novelupdates[^"]*\.(?:jpg|jpeg|png|webp))"',
            ]
            for pattern in patterns:
                match = re.search(pattern, resp.text, re.IGNORECASE)
                if match:
                    img_url = match.group(1)
                    if img_url and 'placeholder' not in img_url.lower() and 'nocover' not in img_url.lower():
                        img_resp = requests.get(img_url, timeout=10, headers=HEADERS)
                        if img_resp.status_code == 200 and len(img_resp.content) > 1000:
                            content_type = img_resp.headers.get('content-type', 'image/jpeg')
                            compressed, media_type = compress_image(img_resp.content)
                            logger.info(f"Got cover from NovelUpdates: {img_url}")
                            _write_cover_cache(title, compressed, media_type or content_type)
                            return compressed, media_type or content_type
    except Exception as e:
        logger.warning(f"NovelUpdates cover search failed: {e}")

    # 3. Try Google Books API (exact/fuzzy title match only; no loose fallback to avoid wrong covers)
    try:
        gb_url = f"https://www.googleapis.com/books/v1/volumes?q={requests.utils.quote(title)}"
        resp = requests.get(gb_url, timeout=10, headers=HEADERS)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get('items', []) or []
            for item in items:
                volume_info = item.get('volumeInfo', {})
                book_title = volume_info.get('title', '').lower()
                if title.lower() in book_title or book_title in title.lower():
                    image_links = volume_info.get('imageLinks', {})
                    for size in ['extraLarge', 'large', 'medium', 'thumbnail', 'smallThumbnail']:
                        img_url = image_links.get(size, '')
                        if img_url:
                            img_url = img_url.replace('&edge=curl', '').replace('edge=curl&', '')
                            img_resp = requests.get(img_url, timeout=10, headers=HEADERS)
                            if img_resp.status_code == 200 and len(img_resp.content) > 1000:
                                content_type = img_resp.headers.get('content-type', 'image/jpeg')
                                compressed, media_type = compress_image(img_resp.content)
                                logger.info(f"Got cover from Google Books (exact match): {img_url}")
                                _write_cover_cache(title, compressed, media_type or content_type)
                                return compressed, media_type or content_type
    except Exception as e:
        logger.warning(f"Google Books cover search failed: {e}")
    
    # 4. Try Open Library API
    try:
        search_query = title.replace(' ', '+')
        api_url = f"https://openlibrary.org/search.json?title={requests.utils.quote(title)}&limit=5"
        resp = requests.get(api_url, timeout=10, headers=HEADERS)
        if resp.status_code == 200:
            data = resp.json()
            docs = data.get('docs', [])
            for doc in docs:
                cover_id = doc.get('cover_i')
                if cover_id:
                    # Get large cover
                    img_url = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg"
                    img_resp = requests.get(img_url, timeout=10, headers=HEADERS)
                    if img_resp.status_code == 200 and len(img_resp.content) > 1000:
                        content_type = 'image/jpeg'
                        compressed, media_type = compress_image(img_resp.content)
                        logger.info(f"Got cover from Open Library: {img_url}")
                        _write_cover_cache(title, compressed, media_type or content_type)
                        return compressed, media_type or content_type
    except Exception as e:
        logger.warning(f"Open Library cover search failed: {e}")
    
    # 5. Generate a simple placeholder cover if all else fails
    try:
        placeholder = generate_placeholder_cover(title, metadata)
        if placeholder:
            logger.info(f"Generated placeholder cover for: {title}")
            _write_cover_cache(title, placeholder, 'image/png')
            return placeholder, 'image/png'
    except Exception as e:
        logger.warning(f"Failed to generate placeholder cover: {e}")
    
    logger.warning(f"No cover image found for: {title}")
    return None, ''


def generate_placeholder_cover(title: str, metadata: dict = None) -> Optional[bytes]:
    """Generate a simple placeholder cover image using PIL"""
    try:
        from PIL import Image, ImageDraw, ImageFont
        from io import BytesIO
        
        # Create a gradient-like background
        width, height = 400, 600
        img = Image.new('RGB', (width, height), '#1a1a2e')
        draw = ImageDraw.Draw(img)
        
        # Add a decorative border
        border_color = '#e94560'
        draw.rectangle([10, 10, width-10, height-10], outline=border_color, width=3)
        draw.rectangle([20, 20, width-20, height-20], outline='#16213e', width=2)
        
        # Try to use a nice font, fall back to default
        try:
            font_large = ImageFont.truetype("arial.ttf", 32)
            font_small = ImageFont.truetype("arial.ttf", 18)
        except:
            font_large = ImageFont.load_default()
            font_small = ImageFont.load_default()
        
        # Wrap title text
        words = title.split()
        lines = []
        current_line = ""
        for word in words:
            test_line = f"{current_line} {word}".strip()
            if len(test_line) <= 18:
                current_line = test_line
            else:
                if current_line:
                    lines.append(current_line)
                current_line = word
        if current_line:
            lines.append(current_line)
        
        # Draw title
        y_pos = 150
        for line in lines[:5]:  # Max 5 lines
            bbox = draw.textbbox((0, 0), line, font=font_large)
            text_width = bbox[2] - bbox[0]
            x_pos = (width - text_width) // 2
            draw.text((x_pos, y_pos), line, fill='#ffffff', font=font_large)
            y_pos += 45
        
        # Add author if available
        if metadata and metadata.get('author'):
            author = metadata.get('author', 'Unknown')
            if len(author) > 25:
                author = author[:22] + "..."
            bbox = draw.textbbox((0, 0), author, font=font_small)
            text_width = bbox[2] - bbox[0]
            x_pos = (width - text_width) // 2
            draw.text((x_pos, height - 100), f"by {author}", fill='#aaaaaa', font=font_small)
        
        # Add decorative element
        draw.text((width//2 - 20, height - 60), "📖", fill='#e94560', font=font_large)
        
        # Save to bytes
        buffer = BytesIO()
        img.save(buffer, format='PNG', quality=95)
        return buffer.getvalue()
        
    except ImportError:
        logger.warning("PIL not available for placeholder cover generation")
        return None
    except Exception as e:
        logger.warning(f"Failed to generate placeholder: {e}")
        return None


def clean_filename(title):
    """Clean filename, remove special characters"""
    clean = re.sub(r'[\\/*?:"<>|]', "", title)
    clean = clean.replace(' ', '_')[:100]  # Limit length
    return clean


def get_novel_abbreviation(title: str) -> str:
    """Generate smart abbreviation from novel title - works for ANY novel.

    Examples:
    - "Desolate Era" → "DE"
    - "I Shall Seal The Heavens" → "ISTH"
    - "A Monster Who Levels Up" → "MWLU" (removes article)
    - "Against the Gods" → "AG"
    - "The Great Ruler" → "GR" (removes article)
    """
    # Common articles/prepositions to skip
    skip_words = {
        'a', 'an', 'the', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'by',
        'for', 'is', 'with'
    }

    # Split into words and filter
    words = title.split()
    meaningful_words = [
        w for w in words if w.lower() not in skip_words and len(w) > 0
    ]

    # If no meaningful words, use original words
    if not meaningful_words:
        meaningful_words = words

    # Take first letter of each meaningful word (max 4 words)
    abbrev = ''.join(w[0].upper() for w in meaningful_words[:4])

    # If abbreviation is too long or too short, adjust
    if len(abbrev) > 5:
        # Too long - use first 3-4 chars of title
        abbrev = re.sub(r'[aeiou]', '', title.upper())[:4]
        if not abbrev:  # Fallback
            abbrev = title.upper()[:4]
    elif len(abbrev) < 2:
        # Too short - use first N chars of title
        abbrev = re.sub(r'[^a-zA-Z]', '', title).upper()[:3]

    return abbrev


def extract_novel_name(full_title):
    """Extract clean novel name from full title"""
    # Remove common suffixes like "Novel", "Read...", etc.
    name = full_title
    # Remove "Novel - Read ... For Free - Novel Bin" pattern
    name = re.sub(r'\s*-\s*Read.*', '', name)
    name = re.sub(r'\s*-\s*[A-Z][a-z]*\s*Bin', '', name)
    name = name.replace('Novel', '').strip()
    return name


def process_chapter_content(content: str, show_notes: bool = True) -> str:
    """
    Process chapter content to extract TL notes and footnotes,
    moving them to the end of the chapter or removing them entirely.
    
    Args:
        content: Raw chapter content text
        show_notes: If True, move notes to end. If False, remove entirely.
    
    Returns:
        Processed content with notes at end or removed
    """
    if not content:
        return content
    
    # Patterns for TL notes and footnotes
    tl_patterns = [
        r'\[T/?N:?\s*[^\]]+\]',           # [T/N: ...] or [TN: ...]
        r'\[Translator[\'s]*\s*[Nn]ote:?\s*[^\]]+\]',  # [Translator's note: ...]
        r'\(T/?N:?\s*[^\)]+\)',           # (T/N: ...) or (TN: ...)
        r'\[A/?N:?\s*[^\]]+\]',           # [A/N: ...] Author notes
        r'\(A/?N:?\s*[^\)]+\)',           # (A/N: ...)
        r'\[E/?N:?\s*[^\]]+\]',           # [E/N: ...] Editor notes
        r'\(E/?N:?\s*[^\)]+\)',           # (E/N: ...)
        r'\[PR:?\s*[^\]]+\]',             # [PR: ...] Proofreader notes
        r'\[Note:?\s*[^\]]+\]',           # [Note: ...]
    ]
    
    footnote_patterns = [
        r'\[\d+\]',                        # [1], [2], etc.
        r'\(\d+\)',                        # (1), (2), etc.
        r'\*+\s*[^\n\*]+\*+',             # *...*  (asterisk notes)
    ]
    
    # Collect all notes found
    notes_found = []
    processed_content = content
    
    # Extract TL notes
    for pattern in tl_patterns:
        matches = re.findall(pattern, processed_content, re.IGNORECASE)
        for match in matches:
            notes_found.append(('TL', match))
        # Remove from main content
        processed_content = re.sub(pattern, '', processed_content, flags=re.IGNORECASE)
    
    # Extract footnotes (numbered ones)
    for pattern in footnote_patterns[:2]:  # Only numbered footnotes
        matches = re.findall(pattern, processed_content)
        for match in matches:
            notes_found.append(('FN', match))
        # Remove from main content
        processed_content = re.sub(pattern, '', processed_content)
    
    # Clean up extra whitespace from removals
    processed_content = re.sub(r'\n{3,}', '\n\n', processed_content)
    processed_content = re.sub(r'  +', ' ', processed_content)
    processed_content = processed_content.strip()
    
    # If show_notes is True and we found notes, append them at the end
    if show_notes and notes_found:
        # Group notes by type
        tl_notes = [n[1] for n in notes_found if n[0] == 'TL']
        fn_notes = [n[1] for n in notes_found if n[0] == 'FN']
        
        notes_section = "\n\n---\n"
        
        if tl_notes:
            notes_section += "\n**Translator Notes:**\n"
            for i, note in enumerate(tl_notes, 1):
                # Clean up the note format
                clean_note = re.sub(r'^\[|\]$|\(|\)$', '', note).strip()
                clean_note = re.sub(r'^T/?N:?\s*|^A/?N:?\s*|^E/?N:?\s*|^PR:?\s*|^Note:?\s*', '', clean_note, flags=re.IGNORECASE).strip()
                if clean_note:
                    notes_section += f"{i}. {clean_note}\n"
        
        if fn_notes:
            notes_section += "\n**Footnotes:**\n"
            for note in fn_notes:
                notes_section += f"{note}\n"
        
        processed_content += notes_section
    
    return processed_content


def create_epub(novel_data, user_id: str = None, user_tier: str = 'verified', 
                include_audio: bool = False, audio_files: list = None):
    """Create EPUB file from novel data with info page and chapters
    
    Args:
        novel_data: Dictionary with title, chapters, metadata
        user_id: User ID for fetching personalized settings (styles)
        user_tier: User tier for feature access (epub3, custom styles)
        include_audio: Whether to bundle audio files (EPUB 3.0 only)
        audio_files: List of audio file paths to bundle
    """
    book = epub.EpubBook()
    full_title = novel_data.get('title', 'Novel')
    novel_name = extract_novel_name(full_title)
    metadata = novel_data.get('metadata', {})

    book.set_identifier(
        f'epub_{clean_filename(novel_name)}_{os.urandom(8).hex()}')
    book.set_title(full_title)
    book.set_language(
        metadata.get('language', 'en').lower() if metadata.get('language'
                                                               ) else 'en')

    # Set author from metadata if available
    author = metadata.get('author', 'Unknown Author')
    if author and author != 'Unknown Author':
        book.add_author(author)
    else:
        book.add_author('Web Novel')

    # === COMPREHENSIVE EPUB METADATA (Dublin Core) ===
    
    # Add translator as contributor if available
    if metadata.get('translator'):
        book.add_metadata('DC', 'contributor', metadata['translator'],
                          {'opf:role': 'trl'})  # trl = translator

    # Add genre/subject tags (DC:subject)
    if metadata.get('genre'):
        genres = [g.strip() for g in metadata['genre'].split(',')]
        for genre in genres[:5]:  # Limit to 5 genres
            book.add_metadata('DC', 'subject', genre)

    # Add publication date
    from datetime import datetime
    book.add_metadata('DC', 'date', datetime.now().strftime('%Y-%m-%d'))
    
    # Add original publication year if available
    if metadata.get('year'):
        book.add_metadata('DC', 'issued', metadata['year'])

    # Add status information as custom metadata
    if metadata.get('status'):
        book.add_metadata(None, 'meta', metadata['status'], {'name': 'novel-status'})

    # Add novel type
    if metadata.get('novel_type'):
        book.add_metadata(None, 'meta', metadata['novel_type'], {'name': 'novel-type'})

    # Add description/synopsis (DC:description)
    description = metadata.get('description') or metadata.get('synopsis')
    if description:
        desc_clean = description.replace('\n', ' ').strip()
        if len(desc_clean) > 1000:
            desc_clean = desc_clean[:997] + '...'
        book.add_metadata('DC', 'description', desc_clean)

    # Add chapter count as custom metadata
    chapters_list = novel_data.get('chapters', [])
    book.add_metadata(None, 'meta', str(len(chapters_list)), {'name': 'chapter-count'})

    # Add missing chapters info
    missing_chapters = novel_data.get('missing_chapters', [])
    if missing_chapters:
        book.add_metadata(None, 'meta', str(len(missing_chapters)), {'name': 'missing-chapters'})

    # Add source URL (DC:source)
    source_url = novel_data.get('novel_url', '')
    if source_url:
        book.add_metadata('DC', 'source', source_url)

    # Add publisher (Groggy Bot)
    book.add_metadata('DC', 'publisher', 'Groggy Bot - Web Novel Aggregator')

    # Add rights/copyright notice
    book.add_metadata('DC', 'rights', 
        'This is a free eBook for personal use. All content is publicly available online.')

    # Add type (DC:type)
    book.add_metadata('DC', 'type', 'Web Novel')

    # Get user's preferred style CSS and settings
    custom_style = None
    show_notes = True  # Default: show notes at chapter end
    use_epub3 = False  # Default: EPUB 2.0
    
    if user_id and USER_SETTINGS_AVAILABLE:
        custom_style = get_style_css(user_id, user_tier)
        # Coffee+ users can hide TL notes/footnotes and use EPUB 3.0
        if user_tier in ['coffee', 'catnip', 'sponsor']:
            show_notes = get_setting(user_id, 'show_notes')
            if show_notes is None:
                show_notes = True  # Default to showing notes
            
            # Check EPUB format preference
            epub_format = get_setting(user_id, 'epub_format')
            use_epub3 = (epub_format == 'epub3')
    
    # Set EPUB version in metadata (EPUB 3.0 specific)
    if use_epub3:
        # EPUB 3.0 uses different namespace and features
        book.add_metadata('OPF', 'meta', '3.0', {'property': 'dcterms:modified'})
        logger.info(f"Creating EPUB 3.0 for user {user_id}")
    else:
        logger.info(f"Creating EPUB 2.0 for user {user_id}")
    
    # Default CSS (used if no custom style) - COMPLETELY removes all indents
    default_style = '''
    /* Reset all text-indent to 0 - critical for EPUB readers */
    * { text-indent: 0 !important; }
    
    body { 
        font-family: Georgia, "Times New Roman", serif; 
        font-size: 12pt; 
        line-height: 1.8; 
        margin: 20px; 
        padding: 0;
        text-indent: 0 !important;
    }
    
    h1, h2, h3, h4, h5, h6 { 
        text-indent: 0 !important; 
        margin-left: 0 !important;
        padding-left: 0 !important;
    }
    
    h1 { font-size: 24pt; margin-top: 40pt; margin-bottom: 20pt; text-align: center; }
    h2 { font-size: 16pt; margin-top: 20pt; margin-bottom: 12pt; }
    
    p { 
        margin: 0 0 12pt 0; 
        padding: 0;
        text-align: justify; 
        text-indent: 0 !important;
        margin-left: 0 !important;
        padding-left: 0 !important;
    }
    
    /* Cover and title pages */
    .cover-page { width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; }
    .cover-img { max-width: 100%; height: auto; display: block; }
    .title-page { padding: 40pt; text-align: center; }
    .title-page h1 { font-size: 32pt; margin: 120pt 0 30pt 0; font-weight: bold; letter-spacing: 2px; }
    .title-page .author { font-size: 14pt; margin: 10pt 0 5pt 0; }
    .title-page .translator { font-size: 12pt; margin: 8pt 0 80pt 0; color: #888; }
    .disclaimer { padding: 40pt; text-align: left; font-size: 11pt; line-height: 1.6; }
    .disclaimer p { margin-bottom: 12pt; text-indent: 0 !important; }
    
    /* System notifications - game-like UI boxes */
    .system-box { 
        font-family: Consolas, Monaco, "Courier New", monospace; 
        font-size: 11pt; 
        text-align: center; 
        background: linear-gradient(to bottom, #f8f8f8, #e8e8e8);
        background-color: #f0f0f0;
        border: 1px solid #ccc;
        border-radius: 4px;
        padding: 12px 20px;
        margin: 16px auto;
        max-width: 90%;
        color: #333;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-indent: 0 !important;
    }
    .system-box.alert { border-color: #e74c3c; background-color: #fdf2f2; }
    .system-box.success { border-color: #27ae60; background-color: #f0fff4; }
    .system-box.info { border-color: #3498db; background-color: #f0f8ff; }
    
    /* Dialogue styling */
    .dialogue { 
        margin: 0 0 12pt 0; 
        text-indent: 0 !important; 
        padding-left: 0 !important;
    }
    
    /* Scene breaks */
    .scene-break { 
        text-align: center; 
        margin: 24pt 0; 
        font-size: 14pt; 
        letter-spacing: 8px;
        color: #888;
        text-indent: 0 !important;
    }
    
    /* Status/Stats boxes */
    .status-box {
        font-family: Consolas, Monaco, "Courier New", monospace;
        font-size: 10pt;
        background-color: #1a1a2e;
        color: #00ff88;
        border: 2px solid #00ff88;
        border-radius: 6px;
        padding: 16px;
        margin: 20px auto;
        max-width: 85%;
        line-height: 1.6;
        text-indent: 0 !important;
    }
    .status-box .label { color: #888; }
    .status-box .value { color: #00ff88; font-weight: bold; }
    
    /* Chapter content - no indent */
    .chapter-content p {
        text-indent: 0 !important;
        margin-left: 0 !important;
    }
    '''
    
    # Use custom style if available, otherwise default
    style = custom_style if custom_style else default_style

    # Create CSS stylesheet item
    css_item = epub.EpubItem(
        uid="style_default",
        file_name="styles/main.css",
        media_type="text/css",
        content=style.encode('utf-8')
    )
    book.add_item(css_item)

    epub_chapters = []
    cover_image_obj = None
    img_ext = 'jpg'

    # ===== PAGE 1: COVER PAGE =====
    cover_page = epub.EpubHtml(title='Cover',
                               file_name='cover.xhtml',
                               lang='en')

    cover_html = '<html><head><meta charset="utf-8"/><style type="text/css">'
    cover_html += 'html, body { margin: 0; padding: 0; width: 100%; height: 100%; } '
    cover_html += '.cover-container { width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; text-align: center; } '
    cover_html += 'img { max-width: 100%; max-height: 100%; width: auto; height: auto; object-fit: contain; }'
    cover_html += '</style></head><body><div class="cover-container">'

    # Fetch cover image with fallback chain (source → NovelUpdates → DuckDuckGo)
    cover_data, content_type = fetch_cover_image(novel_name, novel_data.get('novel_url'), metadata)
    has_cover = False
    
    if cover_data:
        # Compress cover image for smaller file size
        cover_data, content_type = compress_image(cover_data, max_size=(800, 1200), quality=75)
        img_ext = 'jpg'
        media_type = 'image/jpeg'

        cover_image_obj = epub.EpubImage()
        cover_image_obj.file_name = f'images/cover.{img_ext}'
        cover_image_obj.media_type = media_type
        cover_image_obj.content = cover_data
        book.add_item(cover_image_obj)
        cover_html += f'<img src="images/cover.{img_ext}" alt="Cover"/>'
        has_cover = True
        
        # Note: Don't use book.set_cover() as it creates a duplicate cover.xhtml

    # If still no cover, show title as fallback (shouldn't happen often now)
    if not has_cover:
        cover_html += f'<div style="text-align:center; margin-top:40%; font-size: 2em; font-weight: bold;">{novel_name}</div>'

    cover_html += '</div></body></html>'
    cover_page.content = cover_html
    book.add_item(cover_page)

    # ===== PAGE 2: TITLE PAGE =====
    title_page = epub.EpubHtml(title='Title Page',
                               file_name='title.xhtml',
                               lang='en')

    def escape_html(text):
        return text.replace('&', '&amp;').replace('<',
                                                  '&lt;').replace('>', '&gt;')

    # Minimal styling for maximum EPUB compatibility (no XML declaration)
    title_html = '<html><head><meta charset="utf-8"/></head><body style="margin: 0; padding: 0;">'

    # Use empty paragraphs to create vertical centering (most EPUB-compatible approach)
    for _ in range(8):
        title_html += '<p style="margin: 0; line-height: 2;">&nbsp;</p>'

    # Title - large and centered
    title_html += '<p style="text-align: center; font-size: 2.2em; font-weight: bold; margin: 0; line-height: 1.3;">' + escape_html(
        novel_name) + '</p>'

    # Thin decorative line - short and centered
    title_html += '<p style="text-align: center; margin: 12px 0 0 0; line-height: 1;"><span style="display: inline-block; width: 100px; border-top: 0.5px solid #999;"></span></p>'

    # Author - centered below title
    if metadata.get('author'):
        title_html += '<p style="margin: 20px 0 0 0; line-height: 1;">&nbsp;</p>'
        title_html += '<p style="text-align: center; font-size: 1.2em; margin: 0; line-height: 1.2;">' + escape_html(
            metadata['author']) + '</p>'

    # Translator section with spacing
    if metadata.get('translator'):
        title_html += '<p style="margin: 25px 0 0 0; line-height: 1;">&nbsp;</p>'
        title_html += '<p style="text-align: center; font-size: 1em; margin: 0; line-height: 1.4;">Translated by</p>'
        title_html += '<p style="text-align: center; font-size: 1.1em; font-weight: bold; margin: 0; line-height: 1.2;">' + escape_html(
            metadata['translator']).upper() + '</p>'

    # Decorative box - larger and more visible
    title_html += '<p style="margin: 30px 0 0 0; line-height: 1;">&nbsp;</p>'
    title_html += '<p style="text-align: center;"><span style="display: inline-block; width: 100px; height: 100px; border: 1.5px solid; background: #f9f9f9;"></span></p>'

    title_html += '</body></html>'
    title_page.content = title_html
    book.add_item(title_page)

    # ===== PAGE 3: DISCLAIMER PAGE =====
    disclaimer_page = epub.EpubHtml(title='Disclaimer',
                                     file_name='disclaimer.xhtml',
                                     lang='en')

    disclaimer_html = '<html><head><meta charset="utf-8"/></head><body style="margin: 0; padding: 40px;">'
    
    # Disclaimer title - at top, centered
    disclaimer_html += '<p style="text-align: center; font-size: 1.5em; font-weight: bold; margin: 0 0 20px 0;">Disclaimer</p>'
    
    # Thin decorative line
    disclaimer_html += '<p style="text-align: center; margin: 0 0 25px 0;"><span style="display: inline-block; width: 80px; border-top: 1px solid #999;"></span></p>'
    
    # Bot credit - centered
    disclaimer_html += '<p style="text-align: center; font-size: 1.1em; font-weight: bold; margin: 0 0 20px 0;">Automated EPUB Conversion Using Groggy Bot</p>'
    
    # Disclaimer text - centered
    disclaimer_html += '<p style="text-align: center; font-size: 1em; margin: 0 0 15px 0; line-height: 1.5;">This is a free eBook. You are free to give it away (in unmodified form) to whomever you wish.</p>'
    
    disclaimer_html += '<p style="text-align: center; font-size: 1em; margin: 0 0 15px 0; line-height: 1.5;">We do not own this novel. All content is publicly available online, and this tool only aggregates it for reading convenience.</p>'
    
    disclaimer_html += '<p style="text-align: center; font-size: 1em; margin: 0; line-height: 1.5;">Users are encouraged to visit the original websites to support the authors and translators.</p>'
    
    disclaimer_html += '</body></html>'
    disclaimer_page.content = disclaimer_html
    book.add_item(disclaimer_page)

    # Add chapter content - SORT by chapter_num to ensure correct order (numeric, not string sort)
    novel_abbrev = get_novel_abbreviation(full_title)
    chapters_to_add = novel_data.get('chapters', [])

    # Sort by chapter_num field (handles prologue as 0, regular chapters as 1+)
    # If chapter_num is missing, use index as fallback
    chapters_to_add = sorted(chapters_to_add,
                             key=lambda c: c.get('chapter_num', 0))

    for file_index, chap in enumerate(chapters_to_add, 1):
        # Skip empty chapters or placeholders (Chapter 0/Prologue with no content)
        content = chap.get('content', '').strip()
        if not content or len(content) < 50: # Very short content usually placeholder
            # If it's a known placeholder or empty, skip it
            logger.info(f"Skipping potentially empty chapter: {chap.get('title', 'Unknown')}")
            continue

        # Use chapter_num from chapter dict (this is the ACTUAL chapter number, not array index)
        chapter_num = chap.get('chapter_num', file_index)

        # Ensure chapter_num is int (defensive check for string values)
        if isinstance(chapter_num, str):
            try:
                chapter_num = int(chapter_num)
            except ValueError:
                chapter_num = file_index

        # Create simple chapter title (no novel abbreviation - cleaner in chapter list)
        chap_title = chap.get('title', '').replace('&', '&amp;').replace(
            '<', '&lt;').replace('>', '&gt;')
        if chap_title and (
                re.match(r'^Chapter\s+\d+', chap_title, re.IGNORECASE)
                or re.match(r'^Prologue', chap_title, re.IGNORECASE)):
            # Title already starts with "Chapter X" or "Prologue" - use as-is
            display_title = chap_title
        elif chap_title:
            # Has a chapter title - format as "Chapter X: Title"
            display_title = f'Chapter {chapter_num}: {chap_title}'
        else:
            # No title - just "Chapter X"
            display_title = f'Chapter {chapter_num}'

        c = epub.EpubHtml(title=display_title,
                          file_name=f'chap_{file_index:05d}.xhtml',
                          lang='en')
        
        # Link CSS to chapter
        c.add_item(css_item)

        # Format chapter HTML with CSS link
        chapter_html = f'<html><head><meta charset="utf-8"/><link rel="stylesheet" type="text/css" href="styles/main.css"/></head><body>'
        chapter_html += f'<h2>{display_title}</h2>'

        # Process content - move TL notes/footnotes to end or remove based on setting
        content = chap.get('content', '')
        content = process_chapter_content(content, show_notes)
        
        # Smart paragraph processing with styling
        paragraphs = content.split('\n')
        for para in paragraphs:
            # Strip all whitespace including non-breaking spaces
            para = para.strip()
            para = para.replace('\xa0', ' ').replace('\u00a0', ' ')  # Replace non-breaking spaces
            para = ' '.join(para.split())  # Normalize all whitespace to single spaces
            if not para:
                continue
            
            # Escape HTML entities
            para_escaped = para.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            # Detect paragraph type and apply appropriate class
            if para.startswith('---') or para == '***' or para == '* * *' or para == '---':
                # Scene break
                chapter_html += '<p class="scene-break">• • •</p>'
            elif re.match(r'^\[SYSTEM:', para, re.IGNORECASE):
                # System notification
                chapter_html += f'<div class="system-box">{para_escaped}</div>'
            elif re.match(r'^\[Status\b', para, re.IGNORECASE) or re.match(r'^\[Stats?\b', para, re.IGNORECASE):
                # Status/Stats box - game UI style
                chapter_html += f'<div class="status-box">{para_escaped}</div>'
            elif re.match(r'^\[[A-Z][A-Za-z\s]+:', para):
                # Other bracketed notifications like [Quest:], [Skill:], [Alert:], etc.
                chapter_html += f'<div class="system-box">{para_escaped}</div>'
            elif re.match(r'^\[.+?\]$', para):
                # Single bracketed line (full line is just [something])
                chapter_html += f'<div class="system-box">{para_escaped}</div>'
            elif para.startswith('"') or para.startswith("'") or para.startswith('"') or para.startswith("'"):
                # Dialogue
                chapter_html += f'<p class="dialogue">{para_escaped}</p>'
            else:
                # Regular paragraph
                chapter_html += f'<p>{para_escaped}</p>'

        chapter_html += '</body></html>'
        c.content = chapter_html
        book.add_item(c)
        epub_chapters.append(c)

    # Bundle audio files if provided (EPUB 3.0 only)
    if include_audio and audio_files and use_epub3:
        for i, audio_path in enumerate(audio_files):
            if os.path.exists(audio_path):
                try:
                    with open(audio_path, 'rb') as f:
                        audio_data = f.read()
                    
                    # Determine audio format
                    audio_filename = os.path.basename(audio_path)
                    if audio_filename.endswith('.mp3'):
                        media_type = 'audio/mpeg'
                    elif audio_filename.endswith('.m4a'):
                        media_type = 'audio/mp4'
                    else:
                        media_type = 'audio/mpeg'  # Default to MP3
                    
                    # Create audio item
                    audio_item = epub.EpubItem(
                        uid=f'audio_{i}',
                        file_name=f'audio/{audio_filename}',
                        media_type=media_type,
                        content=audio_data
                    )
                    book.add_item(audio_item)
                    logger.info(f"Added audio: {audio_filename}")
                except Exception as e:
                    logger.warning(f"Failed to add audio {audio_path}: {e}")
        
        logger.info(f"Bundled {len(audio_files)} audio files into EPUB 3.0")

    # Add NCX and Nav files
    # TOC: Cover, Title, Disclaimer, then all chapters directly (no Section wrapper)
    book.toc = [cover_page, title_page, disclaimer_page] + epub_chapters
    book.add_item(epub.EpubNcx())
    book.add_item(epub.EpubNav())
    # Spine without 'nav' at end - prevents navigation page from appearing in reading flow
    book.spine = [cover_page, title_page, disclaimer_page] + epub_chapters

    # Final filename
    chapter_start = 1
    chapter_end = 1
    if chapters_to_add:
        try:
            chapter_start = chapters_to_add[0].get('chapter_num', 1)
            chapter_end = chapters_to_add[-1].get('chapter_num',
                                                  len(chapters_to_add))
        except (IndexError, KeyError) as e:
            logger.warning(
                f"Error determining chapter range for filename: {e}")

    filename = f"{clean_filename(novel_name)} {chapter_start}-{chapter_end}.epub"
    
    # Write with EPUB version options
    epub_options = {}
    if use_epub3:
        # EPUB 3.0 specific options
        epub_options['epub3_pages'] = True
        epub_options['epub2_guide'] = False
    
    epub.write_epub(filename, book, epub_options)
    return filename


def create_pdf(novel_data, user_id: str = None, user_tier: str = 'verified'):
    """Create PDF file from novel data with cover, title page, disclaimer, and chapters
    
    Matches EPUB format and styling for consistency.
    
    Args:
        novel_data: Dictionary with title, chapters, metadata
        user_id: User ID for fetching personalized settings
        user_tier: User tier for feature access
    """
    import time
    from io import BytesIO
    
    pdf_start = time.time()
    full_title = novel_data.get('title', 'Novel')
    novel_name = extract_novel_name(full_title)
    chapters = sorted(novel_data.get('chapters', []),
                      key=lambda c: c.get('chapter_num', 0))
    metadata = novel_data.get('metadata', {})

    # Get user settings for notes
    show_notes = True
    if user_id and USER_SETTINGS_AVAILABLE and user_tier in ['coffee', 'catnip', 'sponsor']:
        show_notes = get_setting(user_id, 'show_notes')
        if show_notes is None:
            show_notes = True

    chapter_start = 1
    chapter_end = 1
    if chapters:
        try:
            chapter_start = chapters[0].get('chapter_num', 1)
            chapter_end = chapters[-1].get('chapter_num', len(chapters))
        except (IndexError, KeyError) as e:
            logger.warning(f"Error determining chapter range for filename: {e}")

    filename = f"{clean_filename(novel_name)} {chapter_start}-{chapter_end}.pdf"

    doc = SimpleDocTemplate(filename,
                            pagesize=letter,
                            rightMargin=0.75 * inch,
                            leftMargin=0.75 * inch,
                            topMargin=0.75 * inch,
                            bottomMargin=0.75 * inch)

    styles = getSampleStyleSheet()
    
    # Cover page title style (large, centered)
    cover_title_style = ParagraphStyle('CoverTitle',
                                       parent=styles['Heading1'],
                                       fontSize=36,
                                       spaceAfter=20,
                                       spaceBefore=50,
                                       alignment=TA_CENTER,
                                       fontName='Helvetica-Bold')
    
    # Title page styles
    title_style = ParagraphStyle('NovelTitle',
                                 parent=styles['Heading1'],
                                 fontSize=28,
                                 spaceAfter=20,
                                 spaceBefore=80,
                                 alignment=TA_CENTER,
                                 fontName='Helvetica-Bold')
    
    author_style = ParagraphStyle('AuthorStyle',
                                  parent=styles['BodyText'],
                                  fontSize=14,
                                  spaceAfter=10,
                                  alignment=TA_CENTER)
    
    # Disclaimer styles
    disclaimer_title_style = ParagraphStyle('DisclaimerTitle',
                                            parent=styles['Heading2'],
                                            fontSize=18,
                                            spaceAfter=20,
                                            spaceBefore=30,
                                            alignment=TA_CENTER,
                                            fontName='Helvetica-Bold')
    
    disclaimer_style = ParagraphStyle('DisclaimerText',
                                      parent=styles['BodyText'],
                                      fontSize=10,
                                      spaceAfter=8,
                                      alignment=TA_CENTER,
                                      leading=14,
                                      textColor='#555555')
    
    # Chapter styles - clean, book-like
    chapter_title_style = ParagraphStyle('ChapterTitle',
                                         parent=styles['Heading2'],
                                         fontSize=18,
                                         spaceAfter=30,
                                         spaceBefore=40,
                                         fontName='Helvetica-Bold',
                                         alignment=TA_CENTER)
    
    # Body text - NO indent, clean paragraphs with proper spacing
    body_style = ParagraphStyle('CustomBody',
                                parent=styles['BodyText'],
                                fontSize=11,
                                fontName='Times-Roman',
                                alignment=TA_LEFT,
                                spaceAfter=12,
                                leading=18,
                                firstLineIndent=0)
    
    # First paragraph after chapter title - no indent
    first_para_style = ParagraphStyle('FirstPara',
                                      parent=body_style,
                                      firstLineIndent=0)
    
    # Dialogue style - slightly different for conversations
    dialogue_style = ParagraphStyle('Dialogue',
                                    parent=body_style,
                                    firstLineIndent=0,
                                    leftIndent=0)
    
    # System/notification style for [SYSTEM:] messages
    system_style = ParagraphStyle('SystemText',
                                  parent=body_style,
                                  fontSize=10,
                                  fontName='Courier',
                                  alignment=TA_CENTER,
                                  spaceAfter=12,
                                  spaceBefore=8,
                                  textColor='#444444',
                                  backColor='#f5f5f5',
                                  borderPadding=8,
                                  leftIndent=20,
                                  rightIndent=20)
    
    # Scene break style
    scene_break_style = ParagraphStyle('SceneBreak',
                                       parent=styles['BodyText'],
                                       fontSize=14,
                                       alignment=TA_CENTER,
                                       spaceBefore=20,
                                       spaceAfter=20)

    story = []
    
    # ===== PAGE 1: COVER PAGE =====
    # Fetch cover image
    cover_data, content_type = fetch_cover_image(novel_name, novel_data.get('novel_url'), metadata)
    
    if cover_data:
        try:
            # Compress cover image for smaller PDF file size
            cover_data, _ = compress_image(cover_data, max_size=(600, 900), quality=70)
            
            # Save cover to temp file and add to PDF
            img_buffer = BytesIO(cover_data)
            cover_img = RLImage(img_buffer)
            
            # Scale to fit page nicely (max 5 inches wide, 7 inches tall)
            max_width = 5 * inch
            max_height = 7 * inch
            
            # Calculate aspect ratio
            img_width = cover_img.drawWidth
            img_height = cover_img.drawHeight
            
            if img_width > max_width:
                scale = max_width / img_width
                img_width = max_width
                img_height = img_height * scale
            
            if img_height > max_height:
                scale = max_height / img_height
                img_height = max_height
                img_width = img_width * scale
            
            cover_img.drawWidth = img_width
            cover_img.drawHeight = img_height
            cover_img.hAlign = 'CENTER'
            
            story.append(Spacer(1, 50))
            story.append(cover_img)
            story.append(PageBreak())
        except Exception as e:
            logger.warning(f"Failed to add cover to PDF: {e}")
            # Fallback to text cover
            story.append(Spacer(1, 200))
            story.append(Paragraph(novel_name, cover_title_style))
            story.append(PageBreak())
    else:
        # Text-only cover
        story.append(Spacer(1, 200))
        story.append(Paragraph(novel_name, cover_title_style))
        story.append(PageBreak())
    
    # ===== PAGE 2: TITLE PAGE =====
    story.append(Spacer(1, 100))
    story.append(Paragraph(novel_name, title_style))
    story.append(Spacer(1, 20))
    
    # Decorative line
    story.append(Paragraph("─" * 20, ParagraphStyle('Line', alignment=TA_CENTER, fontSize=12)))
    story.append(Spacer(1, 20))
    
    if metadata.get('author'):
        story.append(Paragraph(f"by {metadata['author']}", author_style))
    
    if metadata.get('translator'):
        story.append(Spacer(1, 30))
        story.append(Paragraph("Translated by", ParagraphStyle('TransLabel', alignment=TA_CENTER, fontSize=10)))
        story.append(Paragraph(metadata['translator'], author_style))
    
    story.append(PageBreak())
    
    # ===== PAGE 3: DISCLAIMER PAGE =====
    story.append(Spacer(1, 30))
    story.append(Paragraph("Disclaimer", disclaimer_title_style))
    story.append(Paragraph("─" * 15, ParagraphStyle('Line', alignment=TA_CENTER, fontSize=10)))
    story.append(Spacer(1, 20))
    story.append(Paragraph("<b>Automated PDF Conversion Using Groggy Bot</b>", disclaimer_style))
    story.append(Spacer(1, 10))
    story.append(Paragraph("This is a free eBook. You are free to give it away (in unmodified form) to whomever you wish.", disclaimer_style))
    story.append(Spacer(1, 10))
    story.append(Paragraph("We do not own this novel. All content is publicly available online, and this tool only aggregates it for reading convenience.", disclaimer_style))
    story.append(Spacer(1, 10))
    story.append(Paragraph("Users are encouraged to visit the original websites to support the authors and translators.", disclaimer_style))
    story.append(PageBreak())
    
    # ===== CHAPTERS =====
    for chap in chapters:
        # Skip empty chapters or placeholders
        content = chap.get('content', '').strip()
        if not content or len(content) < 50:
            continue

        chapter_num = chap.get('chapter_num', 0)
        chap_title = chap.get('title', '')
        
        # Format title like EPUB
        if chap_title and (re.match(r'^Chapter\s+\d+', chap_title, re.IGNORECASE) or 
                          re.match(r'^Prologue', chap_title, re.IGNORECASE)):
            display_title = chap_title
        elif chap_title:
            display_title = f"Chapter {chapter_num}: {chap_title}"
        else:
            display_title = f"Chapter {chapter_num}"
        
        story.append(Paragraph(display_title, chapter_title_style))
        
        # Process content (handle TL notes like EPUB)
        content = chap.get('content', '')
        content = process_chapter_content(content, show_notes)
        
        # Smart paragraph processing
        paragraphs = content.split('\n')
        is_first_para = True
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            # Escape HTML entities
            para_escaped = para.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            try:
                # Detect paragraph type and apply appropriate style
                if para.startswith('---') or para == '***' or para == '* * *':
                    # Scene break
                    story.append(Paragraph("• • •", scene_break_style))
                elif re.match(r'^\[SYSTEM:', para) or re.match(r'^\[.*?\]$', para):
                    # System notification or bracketed text
                    story.append(Paragraph(para_escaped, system_style))
                elif para.startswith('"') or para.startswith("'") or para.startswith('"') or para.startswith("'"):
                    # Dialogue
                    story.append(Paragraph(para_escaped, dialogue_style))
                elif is_first_para:
                    # First paragraph of chapter - no indent, drop cap feel
                    story.append(Paragraph(para_escaped, first_para_style))
                    is_first_para = False
                else:
                    # Regular body text
                    story.append(Paragraph(para_escaped, body_style))
                    
            except Exception as e:
                logger.warning(f"Paragraph failed: {e}")
                continue
        
        story.append(PageBreak())

    doc.build(story)
    logger.info(f"PDF created in {time.time() - pdf_start:.2f}s: {filename}")
    return filename

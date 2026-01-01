"""
Manga Utilities - Convert downloaded manga images to PDF or ZIP
"""

import os
import io
import re
import zipfile
import shutil
import logging
import requests
from typing import Dict, List, Any, Optional, Tuple
from PIL import Image
from reportlab.platypus import SimpleDocTemplate, Image as RLImage, PageBreak, Paragraph, Spacer
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

logger = logging.getLogger(__name__)

# Discord file size limit (8MB for non-Nitro)
DISCORD_FILE_LIMIT = 8 * 1024 * 1024  # 8MB


def upload_to_catbox(filepath: str, timeout: int = 300) -> Optional[str]:
    """Upload file to Catbox.moe (permanent hosting, 200MB limit)"""
    try:
        with open(filepath, 'rb') as f:
            files = {'fileToUpload': (os.path.basename(filepath), f)}
            data = {'reqtype': 'fileupload'}
            resp = requests.post('https://catbox.moe/user/api.php', files=files, data=data, timeout=timeout)
            
            if resp.status_code == 200 and resp.text.startswith('https://'):
                url = resp.text.strip()
                logger.info(f"Uploaded to Catbox: {url}")
                
                # Verify the file is accessible
                try:
                    import time
                    time.sleep(1)  # Wait for file to be available
                    verify_resp = requests.head(url, timeout=15, allow_redirects=True)
                    if verify_resp.status_code == 200:
                        content_length = verify_resp.headers.get('Content-Length', '0')
                        if int(content_length) > 1000:
                            logger.info(f"Catbox file verified: {content_length} bytes")
                            return url
                        else:
                            logger.warning(f"Catbox file too small or empty: {content_length} bytes")
                    else:
                        logger.warning(f"Catbox file not accessible: HTTP {verify_resp.status_code}")
                except Exception as ve:
                    logger.warning(f"Catbox verification failed: {ve}")
                    # Return URL anyway if verification fails (might still work)
                    return url
    except requests.exceptions.Timeout:
        logger.warning("Catbox upload timed out")
    except Exception as e:
        logger.error(f"Catbox upload failed: {e}")
    return None


def upload_to_litterbox(filepath: str, expiry: str = "72h", timeout: int = 300) -> Optional[str]:
    """Upload file to Litterbox (temporary hosting, expires after set time)
    expiry options: 1h, 12h, 24h, 72h (default)
    """
    try:
        with open(filepath, 'rb') as f:
            files = {'fileToUpload': (os.path.basename(filepath), f)}
            data = {'reqtype': 'fileupload', 'time': expiry}
            resp = requests.post('https://litterbox.catbox.moe/resources/internals/api.php', 
                               files=files, data=data, timeout=timeout)
            
            if resp.status_code == 200 and resp.text.startswith('https://'):
                logger.info(f"Uploaded to Litterbox: {resp.text}")
                return resp.text.strip()
    except requests.exceptions.Timeout:
        logger.warning("Litterbox upload timed out")
    except Exception as e:
        logger.error(f"Litterbox upload failed: {e}")
    return None


def upload_to_gofile(filepath: str, timeout: int = 300) -> Optional[str]:
    """Upload file to GoFile (temporary hosting, no limit)"""
    try:
        # Get server
        server_resp = requests.get('https://api.gofile.io/servers', timeout=15)
        if server_resp.status_code != 200:
            return None
        
        servers = server_resp.json().get('data', {}).get('servers', [])
        if not servers:
            return None
        
        server = servers[0].get('name', 'store1')
        
        # Upload file
        with open(filepath, 'rb') as f:
            files = {'file': (os.path.basename(filepath), f)}
            resp = requests.post(f'https://{server}.gofile.io/uploadFile', 
                               files=files, timeout=timeout)
            
            if resp.status_code == 200:
                result = resp.json()
                if result.get('status') == 'ok':
                    data = result.get('data', {})
                    download_url = data.get('downloadPage', '')
                    if download_url:
                        logger.info(f"Uploaded to GoFile: {download_url}")
                        return download_url
    except requests.exceptions.Timeout:
        logger.warning("GoFile upload timed out")
    except Exception as e:
        logger.error(f"GoFile upload failed: {e}")
    return None


def upload_to_transfersh(filepath: str, timeout: int = 600) -> Optional[str]:
    """Upload file to Transfer.sh (10GB max, 14 days storage, direct download)"""
    try:
        filename = os.path.basename(filepath)
        with open(filepath, 'rb') as f:
            # Use PUT method with --upload-file equivalent
            resp = requests.put(
                f'https://transfer.sh/{filename}',
                data=f,
                headers={
                    'Max-Days': '14',
                    'Content-Type': 'application/octet-stream'
                },
                timeout=timeout
            )
            
            if resp.status_code == 200 and resp.text.startswith('https://'):
                url = resp.text.strip()
                logger.info(f"Uploaded to Transfer.sh: {url}")
                return url
    except requests.exceptions.Timeout:
        logger.warning("Transfer.sh upload timed out")
    except Exception as e:
        logger.error(f"Transfer.sh upload failed: {e}")
    return None


def upload_to_fileio(filepath: str, timeout: int = 300) -> Optional[str]:
    """Upload file to File.io (100MB max, expires after 14 days or 1 download)
    Note: Files are deleted after first download by default
    """
    try:
        file_size = os.path.getsize(filepath)
        if file_size > 100 * 1024 * 1024:  # 100MB limit
            logger.warning("File too large for File.io (100MB limit)")
            return None
            
        with open(filepath, 'rb') as f:
            files = {'file': (os.path.basename(filepath), f)}
            data = {'expires': '14d'}  # 14 days expiry
            resp = requests.post('https://file.io', files=files, data=data, timeout=timeout)
            
            if resp.status_code == 200:
                result = resp.json()
                if result.get('success'):
                    url = result.get('link', '')
                    if url:
                        logger.info(f"Uploaded to File.io: {url}")
                        return url
    except requests.exceptions.Timeout:
        logger.warning("File.io upload timed out")
    except Exception as e:
        logger.error(f"File.io upload failed: {e}")
    return None


def upload_to_pixeldrain(filepath: str, timeout: int = 600) -> Optional[str]:
    """Upload file to Pixeldrain (20GB max, direct download)"""
    try:
        filename = os.path.basename(filepath)
        file_size = os.path.getsize(filepath)
        # Increase timeout for large files: 300s base + 1s per 100MB
        calculated_timeout = max(300, timeout + (file_size // (100 * 1024 * 1024)))
        
        with open(filepath, 'rb') as f:
            files = {'file': (filename, f)}
            resp = requests.post('https://pixeldrain.com/api/file', files=files, timeout=calculated_timeout)
            
            if resp.status_code == 201:
                result = resp.json()
                file_id = result.get('id', '')
                if file_id:
                    url = f"https://pixeldrain.com/u/{file_id}"
                    logger.info(f"Uploaded to Pixeldrain: {url}")
                    return url
    except requests.exceptions.Timeout:
        logger.warning(f"Pixeldrain upload timed out (file size: {os.path.getsize(filepath) / (1024*1024):.1f}MB)")
    except Exception as e:
        logger.error(f"Pixeldrain upload failed: {e}")
    return None


def upload_to_0x0(filepath: str, timeout: int = 600) -> Optional[str]:
    """Upload file to 0x0.st (512MB max, expires based on size)"""
    try:
        file_size = os.path.getsize(filepath)
        if file_size > 512 * 1024 * 1024:  # 512MB limit
            logger.warning("File too large for 0x0.st (512MB limit)")
            return None
            
        with open(filepath, 'rb') as f:
            files = {'file': (os.path.basename(filepath), f)}
            resp = requests.post('https://0x0.st', files=files, timeout=timeout)
            
            if resp.status_code == 200 and resp.text.startswith('https://'):
                url = resp.text.strip()
                logger.info(f"Uploaded to 0x0.st: {url}")
                return url
    except requests.exceptions.Timeout:
        logger.warning("0x0.st upload timed out")
    except Exception as e:
        logger.error(f"0x0.st upload failed: {e}")
    return None


def upload_large_file(filepath: str, progress_callback=None, skip_hosts: List[str] = None) -> Tuple[Optional[str], str]:
    """Upload large file to external hosting. Returns (url, service_name) or (None, error)
    
    Priority order: Most reliable services first
    progress_callback: Optional function that takes (service_name, status) for progress updates
    skip_hosts: List of host names to skip (for retry with different host)
    """
    from typing import Callable
    file_size = os.path.getsize(filepath)
    file_size_mb = file_size / (1024 * 1024)
    skip_hosts = skip_hosts or []
    
    # Normalize skip_hosts to simple names for comparison
    skip_names = [h.split(' ')[0].lower() for h in skip_hosts]
    
    logger.info(f"Uploading {file_size_mb:.1f}MB file to external host... (skipping: {skip_names})")
    
    # 1. Pixeldrain FIRST - very reliable, 20GB max, direct download
    if 'pixeldrain' not in skip_names:
        if progress_callback:
            progress_callback("Pixeldrain", "uploading")
        url = upload_to_pixeldrain(filepath)
        if url:
            if progress_callback:
                progress_callback("Pixeldrain", "success")
            return url, "Pixeldrain (direct download)"
        if progress_callback:
            progress_callback("Pixeldrain", "failed")
    
    
    # 3. 0x0.st (512MB max, direct download)
    if '0x0.st' not in skip_names and file_size < 512 * 1024 * 1024:
        if progress_callback:
            progress_callback("0x0.st", "uploading")
        url = upload_to_0x0(filepath)
        if url:
            if progress_callback:
                progress_callback("0x0.st", "success")
            return url, "0x0.st (direct download)"
        if progress_callback:
            progress_callback("0x0.st", "failed")
    
    # 4. Litterbox (direct download, 72h expiry)
    if 'litterbox' not in skip_names:
        if progress_callback:
            progress_callback("Litterbox", "uploading")
        url = upload_to_litterbox(filepath, "72h")
        if url:
            if progress_callback:
                progress_callback("Litterbox", "success")
            return url, "Litterbox (expires in 72 hours)"
        if progress_callback:
            progress_callback("Litterbox", "failed")
    
    # 5. GoFile (unlimited size, requires clicking download)
    if 'gofile' not in skip_names:
        if progress_callback:
            progress_callback("GoFile", "uploading")
        url = upload_to_gofile(filepath)
        if url:
            if progress_callback:
                progress_callback("GoFile", "success")
            return url, "GoFile (click 'Download' on page)"
        if progress_callback:
            progress_callback("GoFile", "failed")
    
    # 6. Transfer.sh (often unreachable)
    if 'transfer.sh' not in skip_names:
        if progress_callback:
            progress_callback("Transfer.sh", "uploading")
        url = upload_to_transfersh(filepath)
        if url:
            if progress_callback:
                progress_callback("Transfer.sh", "success")
            return url, "Transfer.sh (expires in 14 days)"
        if progress_callback:
            progress_callback("Transfer.sh", "failed")
    
    return None, "All upload services failed"


def is_file_too_large_for_discord(filepath: str) -> bool:
    """Check if file exceeds Discord's upload limit"""
    return os.path.getsize(filepath) > DISCORD_FILE_LIMIT


def clean_filename(name: str) -> str:
    """Clean filename for safe saving"""
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'\s+', '_', name)
    return name[:100]


def create_manga_pdf(manga_data: Dict[str, Any]) -> str:
    """Create PDF from manga chapters with images using canvas for flexible page sizes"""
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import inch
    
    title = manga_data.get('title', 'Manga')
    chapters = manga_data.get('chapters', [])
    output_dir = manga_data.get('output_dir', '')
    cover_url = manga_data.get('cover', '')
    source = manga_data.get('source', 'Unknown')
    
    if not chapters:
        raise ValueError("No chapters to create PDF from")
    
    # Get chapter range from first and last chapter
    chapter_start = 1
    chapter_end = len(chapters)
    
    # Try to extract chapter numbers from titles
    first_title = chapters[0].get('title', '')
    last_title = chapters[-1].get('title', '') if len(chapters) > 1 else first_title
    
    match_start = re.search(r'chapter\s*(\d+)', first_title, re.IGNORECASE)
    match_end = re.search(r'chapter\s*(\d+)', last_title, re.IGNORECASE)
    
    if match_start:
        chapter_start = int(match_start.group(1))
    if match_end:
        chapter_end = int(match_end.group(1))
    
    # Simplified filename format: "Title 1-100.pdf"
    filename = f"{clean_filename(title)} {chapter_start}-{chapter_end}.pdf"
    
    # Use canvas directly for flexible page sizes
    c = canvas.Canvas(filename)
    page_width, page_height = A4
    
    # ===== PAGE 1: Title Page with Cover and Info =====
    c.setPageSize(A4)
    
    # Try to download and embed cover image
    cover_path = None
    if cover_url:
        try:
            cover_resp = requests.get(cover_url, timeout=10)
            if cover_resp.status_code == 200:
                cover_path = os.path.join(output_dir or '.', 'temp_cover.jpg')
                with open(cover_path, 'wb') as f:
                    f.write(cover_resp.content)
        except Exception as e:
            logger.warning(f"Failed to download cover: {e}")
    
    y_pos = page_height - 60
    
    # Title
    c.setFont("Helvetica-Bold", 28)
    c.drawCentredString(page_width / 2, y_pos, title)
    y_pos -= 40
    
    # Cover image (if available)
    if cover_path and os.path.exists(cover_path):
        try:
            with Image.open(cover_path) as img:
                img_w, img_h = img.size
                # Scale to fit nicely on page (max 300pt wide, 400pt tall)
                max_w, max_h = 300, 400
                scale = min(max_w / img_w, max_h / img_h, 1.0)
                draw_w = img_w * scale
                draw_h = img_h * scale
                x_pos = (page_width - draw_w) / 2
                c.drawImage(cover_path, x_pos, y_pos - draw_h, width=draw_w, height=draw_h)
                y_pos -= draw_h + 30
        except Exception as e:
            logger.warning(f"Failed to add cover to PDF: {e}")
    
    # Info section
    c.setFont("Helvetica", 14)
    c.drawCentredString(page_width / 2, y_pos, f"Chapters {chapter_start} - {chapter_end}")
    y_pos -= 25
    c.drawCentredString(page_width / 2, y_pos, f"Total: {len(chapters)} chapters")
    y_pos -= 25
    c.drawCentredString(page_width / 2, y_pos, f"Source: {source}")
    y_pos -= 40
    
    # Disclaimer
    c.setFont("Helvetica-Oblique", 10)
    c.drawCentredString(page_width / 2, y_pos, "Generated by Groogy Bot")
    y_pos -= 15
    c.drawCentredString(page_width / 2, y_pos, "For personal use only. Please support the original creators.")
    
    c.showPage()
    
    # ===== PAGE 2: Table of Contents =====
    c.setPageSize(A4)
    y_pos = page_height - 60
    
    c.setFont("Helvetica-Bold", 22)
    c.drawCentredString(page_width / 2, y_pos, "Table of Contents")
    y_pos -= 40
    
    c.setFont("Helvetica", 12)
    page_num = 3  # TOC starts after title and TOC pages
    
    for i, chapter in enumerate(chapters):
        chapter_title = chapter.get('title', f'Chapter {i+1}')
        image_count = len(chapter.get('images', []))
        
        # Truncate long titles
        if len(chapter_title) > 50:
            chapter_title = chapter_title[:47] + "..."
        
        # Draw chapter entry
        c.drawString(50, y_pos, f"{i+1}. {chapter_title}")
        c.drawRightString(page_width - 50, y_pos, f"Page {page_num}")
        y_pos -= 20
        
        # Calculate pages for this chapter (1 title page + images)
        page_num += 1 + image_count
        
        # Start new TOC page if needed
        if y_pos < 60:
            c.showPage()
            c.setPageSize(A4)
            y_pos = page_height - 60
            c.setFont("Helvetica-Bold", 16)
            c.drawCentredString(page_width / 2, y_pos, "Table of Contents (continued)")
            y_pos -= 40
            c.setFont("Helvetica", 12)
    
    c.showPage()
    
    # Cleanup temp cover
    if cover_path and os.path.exists(cover_path):
        try:
            os.remove(cover_path)
        except:
            pass
    
    # ===== CHAPTER PAGES =====
    for chapter in chapters:
        chapter_title = chapter.get('title', 'Chapter')
        images = chapter.get('images', [])
        
        if not images:
            continue
        
        # Chapter title page
        c.setPageSize(A4)
        c.setFont("Helvetica-Bold", 18)
        c.drawCentredString(A4[0] / 2, A4[1] / 2, chapter_title)
        c.showPage()
        
        # Add images - each on its own page sized to the image
        for img_path in images:
            try:
                if not os.path.exists(img_path):
                    continue
                
                # Open image to get dimensions
                with Image.open(img_path) as img:
                    img_width, img_height = img.size
                    
                    # Set page size to match image aspect ratio
                    # Use A4 width as base, scale height proportionally
                    target_width = A4[0]
                    scale = target_width / img_width
                    target_height = img_height * scale
                    
                    # Set page size to fit the image
                    c.setPageSize((target_width, target_height))
                    
                    # Draw image to fill the page
                    c.drawImage(img_path, 0, 0, width=target_width, height=target_height)
                    c.showPage()
                
            except Exception as e:
                logger.warning(f"Failed to add image {img_path}: {e}")
    
    # Save PDF
    try:
        c.save()
        logger.info(f"Created manga PDF: {filename}")
        return filename
    except Exception as e:
        logger.error(f"Failed to create PDF: {e}")
        raise


def create_manga_zip(manga_data: Dict[str, Any]) -> str:
    """Create ZIP file with manga images organized by chapter"""
    title = manga_data.get('title', 'Manga')
    chapters = manga_data.get('chapters', [])
    output_dir = manga_data.get('output_dir', '')
    
    if not chapters:
        raise ValueError("No chapters to create ZIP from")
    
    # Get chapter range
    chapter_start = 1
    chapter_end = len(chapters)
    
    first_title = chapters[0].get('title', '')
    last_title = chapters[-1].get('title', '') if len(chapters) > 1 else first_title
    
    match_start = re.search(r'chapter\s*(\d+)', first_title, re.IGNORECASE)
    match_end = re.search(r'chapter\s*(\d+)', last_title, re.IGNORECASE)
    
    if match_start:
        chapter_start = int(match_start.group(1))
    if match_end:
        chapter_end = int(match_end.group(1))
    
    # Simplified filename format: "Title 1-100.zip"
    filename = f"{clean_filename(title)} {chapter_start}-{chapter_end}.zip"
    
    with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as zf:
        for i, chapter in enumerate(chapters, 1):
            chapter_title = chapter.get('title', f'Chapter_{i}')
            chapter_title = clean_filename(chapter_title)[:50]
            images = chapter.get('images', [])
            
            for j, img_path in enumerate(images):
                if os.path.exists(img_path):
                    # Create archive path: MangaTitle/Chapter_XX/001.jpg
                    ext = os.path.splitext(img_path)[1]
                    archive_name = f"{clean_filename(title)}/{chapter_title}/{j+1:03d}{ext}"
                    zf.write(img_path, archive_name)
    
    logger.info(f"Created manga ZIP: {filename}")
    return filename


def cleanup_manga_temp(output_dir: str):
    """Clean up temporary manga download directory"""
    try:
        if output_dir and os.path.exists(output_dir):
            shutil.rmtree(output_dir)
            logger.info(f"Cleaned up temp directory: {output_dir}")
    except Exception as e:
        logger.warning(f"Failed to cleanup temp dir: {e}")

import asyncio
import json
import logging
import os
import random
import threading
from typing import Optional, Dict, List, Tuple, Any
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from playwright_stealth import Stealth

# Global semaphore to limit concurrent Playwright instances to 1 to prevent resource exhaustion
_pw_semaphore = asyncio.Semaphore(1)

# Threading lock for sync Playwright functions (used by ThreadPoolExecutor workers)
_sync_pw_lock = threading.Lock()

async def apply_stealth(page: Page):
    """Apply stealth scripts to a page."""
    stealth = Stealth()
    await stealth.apply_stealth_async(page)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COOKIES_FILE = os.path.join(os.path.dirname(__file__), 'cookies.json')

def load_cookies_from_file(filepath: str = COOKIES_FILE) -> List[Dict[str, Any]]:
    """Load cookies from a JSON file for authenticated sessions."""
    if not os.path.exists(filepath):
        logger.debug(f"Cookies file not found: {filepath}")
        return []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            cookies = json.load(f)
        
        if not isinstance(cookies, list):
            logger.warning("Cookies file should contain a list of cookie objects")
            return []
        
        playwright_cookies = []
        for cookie in cookies:
            pc = {
                'name': cookie.get('name', ''),
                'value': cookie.get('value', ''),
                'domain': cookie.get('domain', ''),
                'path': cookie.get('path', '/'),
            }
            if cookie.get('expires'):
                pc['expires'] = cookie.get('expires')
            if cookie.get('httpOnly') is not None:
                pc['httpOnly'] = cookie.get('httpOnly')
            if cookie.get('secure') is not None:
                pc['secure'] = cookie.get('secure')
            if cookie.get('sameSite'):
                pc['sameSite'] = cookie.get('sameSite')
            
            if pc['name'] and pc['value'] and pc['domain']:
                playwright_cookies.append(pc)
        
        logger.info(f"Loaded {len(playwright_cookies)} cookies from {filepath}")
        return playwright_cookies
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in cookies file: {e}")
        return []
    except Exception as e:
        logger.error(f"Error loading cookies: {e}")
        return []

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

_pw_loop: Optional[asyncio.AbstractEventLoop] = None
_pw_thread: Optional[threading.Thread] = None
_pw_lock = threading.Lock()
_pw_ready = threading.Event()
_pw_error: Optional[Exception] = None
_scraper_instance: Optional['PlaywrightScraper'] = None

def _start_pw_loop():
    global _pw_loop, _scraper_instance, _pw_error
    try:
        _pw_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_pw_loop)
        
        async def init_scraper():
            global _scraper_instance
            _scraper_instance = PlaywrightScraper()
        
        _pw_loop.run_until_complete(init_scraper())
        _pw_error = None
    except Exception as e:
        _pw_error = e
        logger.error(f"Failed to initialize Playwright loop: {e}")
    finally:
        _pw_ready.set()
    
    if _pw_loop and _pw_error is None:
        _pw_loop.run_forever()

def get_pw_loop() -> asyncio.AbstractEventLoop:
    global _pw_loop, _pw_thread, _pw_error
    with _pw_lock:
        if _pw_loop is None or not _pw_loop.is_running():
            _pw_ready.clear()
            _pw_error = None
            _pw_thread = threading.Thread(target=_start_pw_loop, daemon=True)
            _pw_thread.start()
    
    _pw_ready.wait()
    
    if _pw_error is not None:
        raise RuntimeError(f"Playwright initialization failed: {_pw_error}")
    if _pw_loop is None:
        raise RuntimeError("Playwright event loop not initialized")
    return _pw_loop

def run_in_pw_loop(coro, timeout: float = 120.0):
    loop = get_pw_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=timeout)

def get_scraper_instance() -> 'PlaywrightScraper':
    get_pw_loop()
    if _scraper_instance is None:
        raise RuntimeError("PlaywrightScraper not initialized")
    return _scraper_instance

class PlaywrightScraper:
    def __init__(self):
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.webnovel_context: Optional[BrowserContext] = None
        self._playwright = None
        self._lock = asyncio.Lock()
        self._webnovel_cookies: List[Dict[str, Any]] = []
        self._context_blocking_applied = False
        self._webnovel_blocking_applied = False

    async def _apply_text_only_blocking(self, context: BrowserContext) -> None:
        """Abort heavy resources to keep bandwidth low (images/media/fonts/styles)."""
        async def _block(route):
            try:
                if route.request.resource_type in {"image", "media", "font", "stylesheet"}:
                    await route.abort()
                else:
                    await route.continue_()
            except Exception:
                try:
                    await route.continue_()
                except Exception:
                    pass
        try:
            await context.route("**/*", _block)
        except Exception as e:
            logger.debug(f"Failed to register blocking route: {e}")
    
    async def _ensure_browser(self) -> BrowserContext:
        async with _pw_semaphore:  # Limit to 1 concurrent browser instance
            async with self._lock:
                if self.browser is None or not self.browser.is_connected():
                    self._playwright = await async_playwright().start()
                    self.browser = await self._playwright.chromium.launch(
                        headless=True,
                        args=[
                            '--disable-blink-features=AutomationControlled',
                            '--disable-dev-shm-usage',
                            '--no-sandbox',
                            '--disable-setuid-sandbox',
                            '--disable-gpu',
                            '--disable-web-security',
                        ]
                    )
                    self.context = await self.browser.new_context(
                        viewport={'width': 1920, 'height': 1080},
                        user_agent=random.choice(USER_AGENTS),
                        locale='en-US',
                        timezone_id='America/New_York',
                        java_script_enabled=True,
                    )
                    if not self._context_blocking_applied:
                        await self._apply_text_only_blocking(self.context)
                        self._context_blocking_applied = True
                return self.context
    
    async def _ensure_webnovel_context(self) -> BrowserContext:
        """Create or return a browser context with WebNovel cookies loaded."""
        async with _pw_semaphore:  # Limit to 1 concurrent browser instance
            async with self._lock:
                if self.browser is None or not self.browser.is_connected():
                    await self._ensure_browser()
                
                if self.webnovel_context is None:
                    self.webnovel_context = await self.browser.new_context(
                        viewport={'width': 1920, 'height': 1080},
                        user_agent=random.choice(USER_AGENTS),
                        locale='en-US',
                        timezone_id='America/New_York',
                        java_script_enabled=True,
                    )

                    if not self._webnovel_blocking_applied:
                        await self._apply_text_only_blocking(self.webnovel_context)
                        self._webnovel_blocking_applied = True
                    
                    cookies = load_cookies_from_file()
                    webnovel_cookies = [c for c in cookies if 'webnovel' in c.get('domain', '').lower()]
                    
                    if webnovel_cookies:
                        try:
                            await self.webnovel_context.add_cookies(webnovel_cookies)
                            self._webnovel_cookies = webnovel_cookies
                            logger.info(f"Added {len(webnovel_cookies)} WebNovel cookies to context")
                        except Exception as e:
                            logger.warning(f"Failed to add cookies: {e}")
                    else:
                        logger.info("No WebNovel cookies found in cookies.json")
                
                return self.webnovel_context
    
    def has_webnovel_cookies(self) -> bool:
        """Check if WebNovel cookies are available."""
        cookies = load_cookies_from_file()
        return any('webnovel' in c.get('domain', '').lower() for c in cookies)
    
    async def close(self):
        async with self._lock:
            if self.webnovel_context:
                await self.webnovel_context.close()
                self.webnovel_context = None
            if self.context:
                await self.context.close()
                self.context = None
            if self.browser:
                await self.browser.close()
                self.browser = None
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
    
    async def get_webnovel_page(self, url: str, wait_selector: Optional[str] = None, timeout: int = 45000) -> Tuple[str, str]:
        """Fetch a WebNovel page using authenticated cookies if available."""
        context = await self._ensure_webnovel_context()
        page = await context.new_page()
        
        try:
            await apply_stealth(page)
            
            # Use networkidle to fully bypass Cloudflare
            await page.goto(url, wait_until='networkidle', timeout=timeout)
            
            # Wait for content to load
            await asyncio.sleep(3)
            
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=15000)
                except:
                    pass
            
            await asyncio.sleep(random.uniform(1, 2))
            
            content = await page.content()
            title = await page.title()
            
            # Check if we're still on Cloudflare
            if 'Just a moment' in title:
                logger.warning("Still on Cloudflare challenge page, waiting longer...")
                await asyncio.sleep(5)
                content = await page.content()
                title = await page.title()
            
            return content, title
            
        except Exception as e:
            logger.error(f"Playwright WebNovel error fetching {url}: {e}")
            raise
        finally:
            await page.close()
    
    async def get_webnovel_chapter(self, url: str) -> Dict[str, Any]:
        """Fetch a WebNovel chapter with authenticated cookies."""
        context = await self._ensure_webnovel_context()
        page = await context.new_page()
        
        try:
            await apply_stealth(page)
            
            # Use networkidle to wait for all content to load (including auth checks)
            await page.goto(url, wait_until='networkidle', timeout=45000)
            
            # Wait longer for authenticated content to load
            await asyncio.sleep(3)
            
            # Try to wait for content area
            try:
                await page.wait_for_selector('.cha-words, .chapter-content, #chapter-content', timeout=10000)
            except:
                pass
            
            # Scroll to bottom to trigger lazy loading of all content
            await page.evaluate('''async () => {
                const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
                const scrollStep = 500;
                let lastHeight = 0;
                let currentHeight = document.body.scrollHeight;
                
                // Scroll down in steps to trigger lazy loading
                while (lastHeight < currentHeight) {
                    lastHeight = currentHeight;
                    window.scrollBy(0, scrollStep);
                    await delay(200);
                    currentHeight = document.body.scrollHeight;
                }
                
                // Scroll back to top
                window.scrollTo(0, 0);
            }''')
            
            await asyncio.sleep(random.uniform(1, 2))
            
            title = ''
            content = ''
            
            title_selectors = ['.cha-tit h3', '.chapter-title', 'h1.title', '.j_chapterName']
            for selector in title_selectors:
                try:
                    el = await page.query_selector(selector)
                    if el:
                        title = await el.inner_text()
                        if title:
                            break
                except:
                    continue
            
            content_selectors = ['.cha-words', '.chapter-content', '#chapter-content', '.j_contentWrap']
            for selector in content_selectors:
                try:
                    el = await page.query_selector(selector)
                    if el:
                        paragraphs = await el.query_selector_all('p')
                        if paragraphs:
                            texts = []
                            for p in paragraphs:
                                text = await p.inner_text()
                                if text and len(text.strip()) > 0:
                                    texts.append(text.strip())
                            if texts:
                                content = '\n\n'.join(texts)
                                break
                        else:
                            content = await el.inner_text()
                            if content and len(content) > 100:
                                break
                except:
                    continue
            
            # Check for paywall indicators
            is_locked = await page.query_selector('.j_locked, .cha-lock, .chapter-lock, .premium-content, .unlock-btn, [class*="locked"], [class*="paywall"]')
            
            # Also check if content is suspiciously short (paywall truncation)
            content_seems_truncated = len(content) < 500 and len(content) > 0
            
            # Log for debugging
            if content:
                logger.info(f"WebNovel chapter fetched: {len(content)} chars, locked={is_locked is not None}, truncated={content_seems_truncated}")
            
            return {
                'title': title.strip() if title else '',
                'content': content.strip() if content else '',
                'url': url,
                'locked': is_locked is not None or content_seems_truncated
            }
            
        except Exception as e:
            logger.error(f"Playwright WebNovel chapter error {url}: {e}")
            return {'title': '', 'content': '', 'url': url, 'error': str(e), 'locked': False}
        finally:
            await page.close()
    
    async def get_page_content(self, url: str, wait_selector: Optional[str] = None, timeout: int = 30000) -> Tuple[str, str]:
        context = await self._ensure_browser()
        page = await context.new_page()
        try:
            await apply_stealth(page)
            # Log User-Agent and cookies before navigation
            ua = await page.evaluate("navigator.userAgent")
            cookies = await context.cookies()
            logger.info(f"[Playwright] User-Agent: {ua}")
            logger.info(f"[Playwright] Cookies: {cookies}")
            await page.goto(url, wait_until='domcontentloaded', timeout=timeout)
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=10000)
                except:
                    pass
            await asyncio.sleep(random.uniform(0.5, 1.5))
            content = await page.content()
            title = await page.title()
            return content, title
        except Exception as e:
            logger.error(f"Playwright error fetching {url}: {e}")
            raise
        finally:
            await page.close()
    
    async def get_chapter_content(self, url: str, content_selectors: List[str] = None) -> Dict[str, Any]:
        if content_selectors is None:
            content_selectors = [
                '#chr-content', '#chapter-content', '.chapter-content',
                '#content', '.content', '.reading-content',
                '.text-left', 'article', '.entry-content',
                '.chapter-c', '#chapter-container', '.chapter-body'
            ]
        
        context = await self._ensure_browser()
        page = await context.new_page()
        
        try:
            await apply_stealth(page)
            
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            for selector in content_selectors:
                try:
                    element = await page.wait_for_selector(selector, timeout=3000)
                    if element:
                        break
                except:
                    continue
            
            await asyncio.sleep(random.uniform(0.3, 0.8))
            
            title = ''
            for title_sel in ['h1', '.chapter-title', '.chr-title', 'h2', '.title']:
                try:
                    title_el = await page.query_selector(title_sel)
                    if title_el:
                        title = await title_el.inner_text()
                        if title:
                            break
                except:
                    continue
            
            content = ''
            for selector in content_selectors:
                try:
                    content_el = await page.query_selector(selector)
                    if content_el:
                        paragraphs = await content_el.query_selector_all('p')
                        if paragraphs:
                            texts = []
                            for p in paragraphs:
                                text = await p.inner_text()
                                if text and len(text.strip()) > 0:
                                    texts.append(text.strip())
                            if texts:
                                content = '\n\n'.join(texts)
                                break
                        else:
                            content = await content_el.inner_text()
                            if content and len(content) > 100:
                                break
                except:
                    continue
            
            return {
                'title': title.strip() if title else '',
                'content': content.strip() if content else '',
                'url': url
            }
            
        except Exception as e:
            logger.error(f"Playwright error fetching chapter {url}: {e}")
            return {'title': '', 'content': '', 'url': url, 'error': str(e)}
        finally:
            await page.close()
    
    async def search_site(self, search_url: str, result_selector: str, 
                          title_attr: str = None, max_results: int = 5) -> List[Dict[str, str]]:
        context = await self._ensure_browser()
        page = await context.new_page()
        results = []
        
        try:
            await apply_stealth(page)
            
            await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            
            await asyncio.sleep(random.uniform(1, 2))
            
            try:
                await page.wait_for_selector(result_selector, timeout=10000)
            except:
                pass
            
            elements = await page.query_selector_all(result_selector)
            
            for el in elements[:max_results]:
                try:
                    href = await el.get_attribute('href')
                    title = await el.get_attribute(title_attr) if title_attr else await el.inner_text()
                    if href and title:
                        results.append({
                            'title': title.strip(),
                            'url': href if href.startswith('http') else ''
                        })
                except:
                    continue
            
            return results
            
        except Exception as e:
            logger.error(f"Playwright search error: {e}")
            return []
        finally:
            await page.close()
    
    async def get_chapter_links(self, novel_url: str, link_selectors: List[str] = None) -> List[str]:
        if link_selectors is None:
            link_selectors = [
                'a[href*="chapter"]', 'a[href*="/c"]',
                '.chapter-item a', '.list-chapter a',
                '#list-chapter a', '.chapter-list a',
                'ul.list-chapter a', '.chapters a'
            ]
        
        context = await self._ensure_browser()
        page = await context.new_page()
        links = []
        
        try:
            await apply_stealth(page)
            
            await page.goto(novel_url, wait_until='domcontentloaded', timeout=30000)
            
            await asyncio.sleep(random.uniform(1, 2))
            
            show_all_btn = await page.query_selector('button:has-text("Show All"), a:has-text("Show All"), .show-all')
            if show_all_btn:
                try:
                    await show_all_btn.click()
                    await asyncio.sleep(1)
                except:
                    pass
            
            for selector in link_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for el in elements:
                        href = await el.get_attribute('href')
                        if href and ('chapter' in href.lower() or '/c' in href):
                            if not href.startswith('http'):
                                base_url = '/'.join(novel_url.split('/')[:3])
                                href = base_url + href if href.startswith('/') else novel_url.rsplit('/', 1)[0] + '/' + href
                            if href not in links:
                                links.append(href)
                except:
                    continue
            
            return links
            
        except Exception as e:
            logger.error(f"Playwright error getting chapter links: {e}")
            return []
        finally:
            await page.close()
    
    async def download_image(self, url: str, headers: Dict[str, str] = None) -> Optional[bytes]:
        context = await self._ensure_browser()
        page = await context.new_page()
        
        try:
            await apply_stealth(page)
            
            if headers:
                await page.set_extra_http_headers(headers)
            
            response = await page.goto(url, timeout=30000)
            
            if response and response.ok:
                return await response.body()
            return None
            
        except Exception as e:
            logger.error(f"Playwright error downloading image {url}: {e}")
            return None
        finally:
            await page.close()


async def fetch_with_stealth(url: str, wait_selector: Optional[str] = None) -> Tuple[str, str]:
    scraper = get_scraper_instance()
    return await scraper.get_page_content(url, wait_selector)

async def fetch_chapter_stealth(url: str, content_selectors: List[str] = None) -> Dict[str, Any]:
    scraper = get_scraper_instance()
    return await scraper.get_chapter_content(url, content_selectors)

async def fetch_image_stealth(url: str, headers: Dict[str, str] = None) -> Optional[bytes]:
    scraper = get_scraper_instance()
    return await scraper.download_image(url, headers)

async def fetch_webnovel_page(url: str, wait_selector: Optional[str] = None) -> Tuple[str, str]:
    """Fetch WebNovel page with cookies."""
    scraper = get_scraper_instance()
    return await scraper.get_webnovel_page(url, wait_selector)

async def fetch_webnovel_chapter(url: str) -> Dict[str, Any]:
    """Fetch WebNovel chapter with cookies."""
    scraper = get_scraper_instance()
    return await scraper.get_webnovel_chapter(url)

def has_webnovel_cookies() -> bool:
    """Check if WebNovel cookies are available."""
    scraper = get_scraper_instance()
    return scraper.has_webnovel_cookies()

def playwright_fetch_with_stealth(url: str, wait_for_selector: Optional[str] = None, timeout: int = 30000) -> Optional[str]:
    """Synchronous function to fetch a URL using Playwright with stealth mode.
    
    Uses sync Playwright directly for reliability (async version has timeout issues).
    Uses a threading lock to prevent multiple concurrent browser instances.
    
    Args:
        url: URL to fetch
        wait_for_selector: Optional CSS selector to wait for before getting content
        timeout: Timeout in milliseconds
        
    Returns:
        HTML content as string, or None if failed
    """
    # Use threading lock to prevent resource exhaustion from concurrent browsers
    with _sync_pw_lock:
        try:
            from playwright.sync_api import sync_playwright
            from playwright_stealth import Stealth
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                page = context.new_page()
                
                # Apply stealth
                stealth = Stealth()
                stealth.apply_stealth_sync(page)
                
                page.goto(url, timeout=timeout, wait_until='networkidle')
                
                # Wait for content
                import time
                time.sleep(3)
                
                if wait_for_selector:
                    try:
                        page.wait_for_selector(wait_for_selector, timeout=10000)
                    except:
                        pass
                
                html = page.content()
                title = page.title()
                
                # Check if still on Cloudflare
                if 'Just a moment' in title:
                    logger.warning("Still on Cloudflare, waiting longer...")
                    time.sleep(5)
                    html = page.content()
                
                browser.close()
                return html
                
        except Exception as e:
            logger.error(f"playwright_fetch_with_stealth failed for {url}: {e}")
            return None


def fetch_webnovel_catalog_sync(book_id: str) -> Optional[str]:
    """Fetch WebNovel catalog page using sync Playwright with stealth.
    
    This is more reliable than the async version for Cloudflare bypass.
    """
    url = f"https://www.webnovel.com/book/{book_id}/catalog"
    return playwright_fetch_with_stealth(url, wait_for_selector='.volume-item, .chapter-item', timeout=45000)


def fetch_webnovel_chapter_sync(chapter_url: str) -> Optional[str]:
    """Fetch WebNovel chapter content using sync Playwright with stealth.
    
    WebNovel chapters are protected by Cloudflare, so we need Playwright.
    Uses fresh browser per request for reliability (sequential scraping).
    """
    return playwright_fetch_with_stealth(chapter_url, wait_for_selector='.cha-words, .chapter-words, .chapter-content', timeout=30000)


import threading

class WebNovelBrowserManager:
    """Manages a single shared browser for WebNovel chapter fetching.
    
    This prevents resource exhaustion from launching many browsers in parallel.
    """
    
    def __init__(self):
        self._browser = None
        self._context = None
        self._lock = threading.Lock()
        self._playwright = None
    
    def _ensure_browser(self):
        """Ensure browser is running, create if needed."""
        if self._browser is None or not self._browser.is_connected:
            from playwright.sync_api import sync_playwright
            from playwright_stealth import Stealth
            
            if self._playwright is None:
                self._playwright = sync_playwright().start()
            
            self._browser = self._playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-gpu',
                ]
            )
            self._context = self._browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
    
    def fetch_chapter(self, url: str) -> Optional[str]:
        """Fetch a WebNovel chapter using the shared browser."""
        with self._lock:  # Ensure only one fetch at a time
            try:
                self._ensure_browser()
                
                page = self._context.new_page()
                try:
                    from playwright_stealth import Stealth
                    stealth = Stealth()
                    stealth.apply_stealth_sync(page)
                    
                    page.goto(url, timeout=30000, wait_until='domcontentloaded')
                    
                    # Wait for content
                    import time
                    time.sleep(2)
                    
                    # Wait for content selector
                    try:
                        page.wait_for_selector('.cha-words, .chapter-words, .chapter-content', timeout=10000)
                    except:
                        pass
                    
                    html = page.content()
                    title = page.title()
                    
                    # Check if Cloudflare blocked
                    if 'Just a moment' in title:
                        time.sleep(3)
                        html = page.content()
                    
                    return html
                finally:
                    page.close()
                    
            except Exception as e:
                logger.error(f"WebNovel chapter fetch failed for {url}: {e}")
                # Reset browser on error
                try:
                    if self._browser:
                        self._browser.close()
                except:
                    pass
                self._browser = None
                self._context = None
                return None
    
    def close(self):
        """Close the browser."""
        with self._lock:
            try:
                if self._browser:
                    self._browser.close()
                if self._playwright:
                    self._playwright.stop()
            except:
                pass
            self._browser = None
            self._context = None
            self._playwright = None


# Global singleton browser manager
_webnovel_browser_manager = WebNovelBrowserManager()


async def fetch_webnovel_chapters_parallel(chapter_urls: List[Tuple[int, str]], concurrency: int = 3) -> List[Tuple[int, str, Optional[str]]]:
    """Fetch multiple WebNovel chapters using a SINGLE shared browser context.
    
    Uses one browser with one authenticated context to avoid session invalidation.
    
    Args:
        chapter_urls: List of (chapter_num, url) tuples
        concurrency: Max concurrent pages (default 3)
    
    Returns:
        List of (chapter_num, url, html_or_none) tuples
    """
    results = []
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-site-isolation-trials',
                    '--disable-setuid-sandbox',
                    '--ignore-certificate-errors',
                    '--window-size=1920,1080',
                ]
            )
            
            # Create ONE shared context with cookies and proper headers
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Referer': 'https://www.webnovel.com/',
                    'Origin': 'https://www.webnovel.com',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0',
                }
            )
            
            # TEMPORARILY DISABLED - cookies may trigger Cloudflare suspicion
            # cookies = load_cookies_from_file()
            # if cookies:
            #     await context.add_cookies(cookies)
            #     logger.info(f"Added {len(cookies)} cookies to shared WebNovel context")
            logger.info("Cookies disabled for WebNovel - using fresh session")
            
            # First, visit the catalog page to establish session
            if chapter_urls:
                first_url = chapter_urls[0][1]
                # Extract book URL from chapter URL
                import re
                book_match = re.search(r'(https://www\.webnovel\.com/book/[^/]+_\d+)', first_url)
                if book_match:
                    catalog_url = book_match.group(1) + '/catalog'
                    warmup_page = await context.new_page()
                    try:
                        await apply_stealth(warmup_page)
                        logger.info(f"Warming up session with catalog: {catalog_url}")
                        await warmup_page.goto(catalog_url, timeout=30000, wait_until='domcontentloaded')
                        await asyncio.sleep(3)  # Let cookies/session settle
                    except Exception as e:
                        logger.warning(f"Catalog warmup failed: {e}")
                    finally:
                        await warmup_page.close()
            
            # Use passed concurrency for WebNovel
            semaphore = asyncio.Semaphore(concurrency)
            
            async def fetch_one(chap_num: int, url: str, delay: float = 0) -> Tuple[int, str, Optional[str]]:
                if delay > 0:
                    await asyncio.sleep(delay)
                    
                async with semaphore:
                    page = await context.new_page()
                    try:
                        await apply_stealth(page)
                        
                        # Navigate to page
                        await page.goto(url, timeout=60000, wait_until='domcontentloaded')
                        
                        # Check for Cloudflare and wait for it to pass
                        for wait_attempt in range(6):  # Wait up to 30 seconds
                            title = await page.title()
                            if 'Just a moment' not in title and 'Cloudflare' not in title:
                                break
                            logger.info(f"Chapter {chap_num}: Waiting for Cloudflare ({wait_attempt+1}/6)...")
                            await asyncio.sleep(5)
                        
                        await asyncio.sleep(2)  # Extra wait after Cloudflare
                        
                        try:
                            await page.wait_for_selector('.cha-words, .chapter-words, .chapter-content', timeout=15000)
                        except:
                            pass
                        
                        # Scroll to trigger lazy loading
                        await page.evaluate('''async () => {
                            const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
                            const scrollStep = 500;
                            let lastHeight = 0;
                            let currentHeight = document.body.scrollHeight;
                            
                            while (lastHeight < currentHeight) {
                                lastHeight = currentHeight;
                                window.scrollBy(0, scrollStep);
                                await delay(150);
                                currentHeight = document.body.scrollHeight;
                            }
                            window.scrollTo(0, 0);
                        }''')
                        
                        await asyncio.sleep(1)
                        
                        html = await page.content()
                        title = await page.title()
                        
                        # Detect Cloudflare challenge
                        if 'Just a moment' in title or 'Cloudflare' in title:
                            logger.warning(f"Chapter {chap_num}: Cloudflare challenge detected")
                            return (chap_num, url, "CLOUDFLARE_BLOCKED")
                        
                        # Also check HTML content for Cloudflare markers
                        if 'challenge-platform' in html or 'cf-challenge' in html:
                            logger.warning(f"Chapter {chap_num}: Cloudflare challenge in content")
                            return (chap_num, url, "CLOUDFLARE_BLOCKED")
                        
                        logger.info(f"Chapter {chap_num}: Fetched {len(html)} bytes")
                        return (chap_num, url, html)
                    except Exception as e:
                        logger.error(f"Error fetching chapter {chap_num}: {e}")
                        return (chap_num, url, None)
                    finally:
                        await page.close()
            
            # Run all chapter fetches in parallel
            tasks = [fetch_one(num, url, 0) for num, url in chapter_urls]
            results = await asyncio.gather(*tasks)
            
            await browser.close()
            
    except Exception as e:
        logger.error(f"Browser setup failed: {e}")
        return [(num, url, None) for num, url in chapter_urls]
    
    return list(results)


def fetch_webnovel_chapters_parallel_sync(chapter_urls: List[Tuple[int, str]], concurrency: int = 3) -> List[Tuple[int, str, Optional[str]]]:
    """Sync wrapper for parallel WebNovel chapter fetching.
    
    Args:
        chapter_urls: List of (chapter_num, url) tuples
        concurrency: Max concurrent browser contexts (default 3)
    
    Returns:
        List of (chapter_num, url, html_or_none) tuples
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(fetch_webnovel_chapters_parallel(chapter_urls, concurrency))
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"Parallel fetch failed: {e}")
        return [(num, url, None) for num, url in chapter_urls]

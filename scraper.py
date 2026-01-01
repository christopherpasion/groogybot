import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse, quote_plus
import time
import logging
import random

import os

import asyncio
from typing import List, Optional, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
from threading import Thread, Event

# Import cache system for reducing requests
try:
    from cache import get_cache, NovelCache
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False
    def get_cache():
        return None


# Patching async_scraper import for Replit environment
async def run_async_scrape(*args, **kwargs):
    logger.warning("run_async_scrape is a placeholder")
    return []


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROTECTED_SITES = [
    'webnovel.com',
    'qidian.com',  # Cloudflare protected
    'novelfire.net',  # Blocks aggressively
    # Note: Ranobes uses embedded JS data in script tags, not Playwright
]


def is_protected_site(url: str) -> bool:
    """Check if URL is from a Cloudflare-protected site.

    Uses proper domain matching to avoid false positives like
    'freewebnovel.com' matching 'webnovel.com'.
    """
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url.lower())
        domain = parsed.netloc
        # Remove 'www.' prefix for comparison
        if domain.startswith('www.'):
            domain = domain[4:]
        # Check if domain matches or ends with protected site
        for site in PROTECTED_SITES:
            if domain == site or domain.endswith('.' + site):
                return True
        return False
    except Exception:
        return False


# Rotating User Agents for anti-detection (updated 2025)
USER_AGENTS = [
    # Chrome on Windows (most common)
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    # Chrome on Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    # Safari on Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    # Firefox
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:133.0) Gecko/20100101 Firefox/133.0',
    # Edge
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
    # Mobile Chrome
    'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/131.0.6778.73 Mobile/15E148 Safari/604.1',
    # Mobile Safari
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1',
]

# Alternative domain mirrors for sites that get blocked
SITE_MIRRORS = {
    'novelbin': [
        'novelbin.me', 'novelbin.com', 'novelbin.cfd', 'novelbin.net',
        'novelbin.org', 'novelbin.cc', 'novelbin.tv'
    ],
    'lightnovelworld': [
        'lightnovelworld.com', 'lightnovelworld.co', 'lnworld.com'
    ],
    'freewebnovel': [
        'freewebnovel.com', 'freewebnovel.me', 'freewebnovel.net'
    ],
    'novelfire': [
        'novelfire.net', 'novelfire.com', 'novelfire.org'
    ],
}

# Site health status cache (avoid repeated checks)
_site_health_cache = {}
_site_health_cache_ttl = 300  # 5 minutes


def check_site_health(site_url: str, timeout: int = 5) -> bool:
    """Check if a site is accessible. Returns True if healthy, False if dead/blocked.
    
    Results are cached for 5 minutes to avoid repeated checks.
    """
    import time as time_module
    
    # Check cache first
    cache_key = site_url.split('/')[2] if '://' in site_url else site_url
    if cache_key in _site_health_cache:
        cached_time, cached_result = _site_health_cache[cache_key]
        if time_module.time() - cached_time < _site_health_cache_ttl:
            return cached_result
    
    try:
        resp = requests.head(site_url, timeout=timeout, allow_redirects=True,
                            headers={'User-Agent': random.choice(USER_AGENTS)})
        is_healthy = resp.status_code < 400
        _site_health_cache[cache_key] = (time_module.time(), is_healthy)
        logger.debug(f"[Health] {cache_key}: {'OK' if is_healthy else 'DEAD'} (HTTP {resp.status_code})")
        return is_healthy
    except Exception as e:
        _site_health_cache[cache_key] = (time_module.time(), False)
        logger.debug(f"[Health] {cache_key}: DEAD ({e})")
        return False


class Scraper:
    def search_all_sites_with_choices(self, query: str, use_cache: bool = True) -> List[Dict[str, str]]:
        """
        Search all supported novel sites for the given query and return a list of results with site/source info.
        Each result is a dict: {'title': ..., 'url': ..., 'source': ...}
        Now runs all site searches in parallel for speed. If no results, fallback to DuckDuckGo search.
        
        Args:
            query: Search query
            use_cache: If True, check cache first and cache new results (reduces IP blocks)
        """
        import requests
        from bs4 import BeautifulSoup
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Check cache first
        if use_cache and CACHE_AVAILABLE:
            cache = get_cache()
            cached_results = cache.get_search_results(query)
            if cached_results:
                logger.info(f"[Search] Using cached results for '{query}' ({len(cached_results)} results)")
                return cached_results

        # Site health URLs for pre-checking
        site_health_urls = {
            'NovelBin': 'https://novelbin.me/',
            'RoyalRoad': 'https://www.royalroad.com/',
            'Ranobes': 'https://ranobes.net/',
            'NovelFire': 'https://novelfire.net/',
            'FreeWebNovel': 'https://freewebnovel.com/',
            'CreativeNovels': 'https://creativenovels.com/',
            'LightNovelWorld': 'https://lightnovelworld.com/',
            'LNMTL': 'https://lnmtl.com/',
            'ReaderNovel': 'https://readernovel.com/',
            'NovelBuddy': 'https://novelbuddy.com/',
            'LibRead': 'https://libread.com/',
            'YongLibrary': 'https://yonglibrary.com/',
        }
        
        # Only include working sites (tested Dec 2025)
        # Dead/blocked sites removed: BoxNovel, LightNovelCave, EmpireNovel, WTR-Lab, 
        # FullNovels, NiceNovel, BedNovel, AllNovelBook, EnglishNovelsFree, ReadNovelFull
        all_site_funcs = [
            ('NovelBin', lambda: self.search_novelbin_multiple(query)),
            ('RoyalRoad', lambda: self.search_royalroad_multiple(query)),
            ('Ranobes', lambda: self._search_ranobes(query)),
            ('NovelFire', lambda: self._search_novelfire(query)),
            ('FreeWebNovel', lambda: self._search_freewebnovel(query)),
            ('CreativeNovels', lambda: self._search_creativenovels(query)),
            ('LightNovelWorld', lambda: self._search_lightnovelworld(query)),
            ('LNMTL', lambda: self._search_lnmtl(query)),
            ('ReaderNovel', lambda: self._search_readernovel(query)),
            ('NovelBuddy', lambda: self._search_novelbuddy(query)),
            ('LibRead', lambda: self._search_libread(query)),
            ('YongLibrary', lambda: self._search_yonglibrary(query)),
            ('DuckDuckGo', lambda: self._search_duckduckgo_novels(query)),
        ]
        
        # Quick health check - skip sites that are down (with 2s timeout for speed)
        dead_sites = []
        for site_name, health_url in site_health_urls.items():
            if not check_site_health(health_url, timeout=2):
                dead_sites.append(site_name)
                logger.info(f"[Search] Skipping {site_name} - site appears down")
        
        # Filter out dead sites
        site_funcs = [(name, func) for name, func in all_site_funcs if name not in dead_sites]
        
        if dead_sites:
            logger.info(f"[Search] Skipped {len(dead_sites)} dead sites: {', '.join(dead_sites)}")

        results = []
        site_errors = []  # Track which sites failed
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_site = {executor.submit(func): name for name, func in site_funcs}
            for future in as_completed(future_to_site):
                site_name = future_to_site[future]
                try:
                    site_results = future.result()
                    if site_results:
                        results.extend(site_results)
                        # Debug: log sample results from each site
                        if site_results and len(site_results) > 0:
                            sample = site_results[0]
                            logger.debug(f"[Search] {site_name} sample result: {sample.get('title', 'No title')[:50]} -> {sample.get('url', 'No URL')[:80]}")
                    logger.info(f"[Search] {site_name} search complete, found {len(site_results) if site_results else 0} results.")
                except Exception as e:
                    site_errors.append(f"{site_name}: {str(e)[:50]}")
                    logger.error(f"{site_name} search error: {e}")
                    # Debug: add full error details
                    import traceback
                    logger.debug(f"{site_name} full error traceback: {traceback.format_exc()[:500]}")

        # Clean and deduplicate results
        unique_results = []
        seen_urls = set()
        seen_titles = set()  # Also dedupe by normalized title
        
        # Normalize query for relevance matching
        query_words = set(re.sub(r'[^a-z0-9\s]', '', query.lower()).split())
        
        for r in results:
            url = r.get('url', '')
            title = r.get('title', '')
            
            # Skip empty results
            if not url or not title:
                continue
                
            # Skip generic/bad titles
            title_lower = title.lower().strip()
            if title_lower in ['novel', 'manga', 'read online', '', 'untitled']:
                continue
            
            # Clean title: remove trailing numbers (IDs), MTL prefix
            clean_title = re.sub(r'\s+\d{4,}$', '', title)  # Remove trailing IDs like " 118044"
            clean_title = re.sub(r'^Mtl\s+', '', clean_title, flags=re.IGNORECASE)  # Remove "Mtl " prefix
            clean_title = clean_title.strip()
            r['title'] = clean_title if clean_title else title
            
            # Relevance check: title must contain at least one query word
            title_words = set(re.sub(r'[^a-z0-9\s]', '', clean_title.lower()).split())
            matching_words = query_words & title_words
            if not matching_words:
                # No word match - skip this result as irrelevant
                continue
            
            # Calculate relevance score (for sorting later)
            r['_relevance'] = len(matching_words) / len(query_words) if query_words else 0
            
            # Normalize for deduplication (lowercase, no special chars)
            normalized_title = re.sub(r'[^a-z0-9]', '', clean_title.lower())
            
            # Skip if URL already seen
            if url in seen_urls:
                continue
            
            # Skip if same title from same domain type (avoid duplicates)
            domain = urlparse(url).netloc.replace('www.', '')
            title_key = f"{normalized_title}_{domain.split('.')[0]}"
            if title_key in seen_titles:
                continue
            
            seen_urls.add(url)
            seen_titles.add(title_key)
            unique_results.append(r)
        
        # Sort by relevance (best matches first)
        unique_results.sort(key=lambda x: x.get('_relevance', 0), reverse=True)
        
        # Remove internal relevance field
        for r in unique_results:
            r.pop('_relevance', None)

        # Cache results for future use (reduces requests and IP blocking)
        if use_cache and CACHE_AVAILABLE and unique_results:
            cache = get_cache()
            cache.set_search_results(query, unique_results)

        # Log site errors summary
        if site_errors:
            logger.warning(f"[Search] {len(site_errors)} sites had errors: {'; '.join(site_errors)}")

        logger.info(f"[Search] All site searches complete. Returning {len(unique_results)} unique results.")
        return unique_results

    def _search_duckduckgo_novels(self, query: str) -> List[Dict[str, str]]:
        """
        Search DuckDuckGo for the query and return a list of likely novel links.
        Only returns results from known novel sites.
        Uses the HTML version of DuckDuckGo for reliable scraping.
        """
        try:
            from urllib.parse import urljoin, urlparse, parse_qs, unquote
            
            # Use the lite/html version for better scraping
            search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query + ' novel read online')}"
            
            resp = self._search_fetch(search_url, 'DuckDuckGo')
            if not resp:
                # Try alternative URL
                search_url = f"https://duckduckgo.com/html/?q={quote_plus(query + ' novel')}"
                resp = self._search_fetch(search_url, 'DuckDuckGo')
            
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            results = []
            
            # DuckDuckGo results are in .result__a or .result-link
            links = soup.select('.result__a, .result-link a, a.result__url')
            if not links:
                links = soup.select('a[href*="uddg="]')
            
            for a in links:
                href = a.get('href')
                title = a.get_text(strip=True)
                if not href or not title:
                    continue

                # Normalize DuckDuckGo redirect links to their real target URL
                url = href
                if href.startswith('/'):
                    url = urljoin('https://duckduckgo.com', href)

                parsed = urlparse(url)
                if 'duckduckgo.com' in parsed.netloc:
                    qs = parse_qs(parsed.query)
                    target = qs.get('uddg', [None])[0]
                    if target:
                        url = unquote(target)

                # Only accept links from known novel sites
                known_sites = [
                    'novelbin', 'royalroad', 'novelfire', 'freewebnovel', 'creativenovels',
                    'boxnovel', 'lightnovelworld', 'lnmtl', 'readernovel', 'novelbuddy',
                    'lightnovelcave', 'libread', 'wtr-lab', 'fullnovels',
                    'nicenovel', 'bednovel', 'allnovelbook', 'yonglibrary', 'englishnovelsfree',
                    'ranobes', 'readnovelfull', 'novellive', 'webnovel', 'scribblehub'
                ]
                if any(site in url.lower() for site in known_sites):
                    # Clean up title
                    title = re.sub(r'\s*[-|]\s*(Read|Novel|Online|Free).*', '', title, flags=re.I)
                    if title and not any(r['url'] == url for r in results):
                        results.append({'title': title.strip(), 'url': url, 'source': 'DuckDuckGo'})
                        
                if len(results) >= 15:
                    break
                    
            logger.info(f"[DuckDuckGo] Found {len(results)} novel results for '{query}'")
            return results
        except Exception as e:
            logger.error(f"DuckDuckGo search error: {e}")
            return []

    # --- Helper methods for each site for parallel search ---
    def _search_ranobes(self, query):
        """Search Ranobes for novels by title.
        
        Ranobes uses a specific search page structure. The results are in
        .short-cont or .shortstory blocks with links to /novels/ID-slug.html
        
        Only uses ranobes.net (English site). ranobes.com is Russian-only.
        Uses cloudscraper to bypass JavaScript challenges when available.
        """
        try:
            # Only use ranobes.net - the English site
            # NOTE: ranobes.com is the Russian version with Russian translations, not English!
            search_configs = [
                # (base_url, search_path, novel_url_pattern)
                ('https://ranobes.net', f'/search/{quote_plus(query)}/', r'/novels/\d+.*\.html'),
                ('https://ranobes.net', f'/?do=search&subaction=search&story={quote_plus(query)}', r'/novels/\d+.*\.html'),
            ]
            
            # Try cloudscraper first if available (better at bypassing JS challenges)
            try:
                import cloudscraper
                scraper = cloudscraper.create_scraper()
                for base_url, search_path, url_pattern in search_configs:
                    search_url = base_url + search_path
                    try:
                        resp = scraper.get(search_url, timeout=15)
                        if resp.status_code == 200 and 'just a moment' not in resp.text.lower()[:500]:
                            soup = BeautifulSoup(resp.content, 'html.parser')
                            results = self._parse_ranobes_results(soup, query, base_url, url_pattern)
                            if results:
                                return results
                        elif 'just a moment' in resp.text.lower()[:500]:
                            logger.warning(f"[Ranobes] Temporarily blocked by Cloudflare. Your IP may be rate-limited. Try again later or use a VPN.")
                    except Exception as e:
                        logger.debug(f"[Ranobes] cloudscraper failed for {base_url}: {e}")
            except ImportError:
                pass  # cloudscraper not installed
            
            # Fallback to regular session
            blocked = False
            for base_url, search_path, url_pattern in search_configs:
                search_url = base_url + search_path
                resp = self._search_fetch(search_url, 'Ranobes', rotate_on_fail=True)
                if not resp:
                    continue
                
                # Check if blocked by Cloudflare
                if 'just a moment' in resp.text.lower()[:500]:
                    blocked = True
                    continue
                    
                soup = BeautifulSoup(resp.content, 'html.parser')
                results = self._parse_ranobes_results(soup, query, base_url, url_pattern)
                if results:
                    return results
            
            if blocked:
                logger.warning("[Ranobes] Temporarily blocked by Cloudflare. Your IP may be rate-limited. Try again later or use a VPN.")
                    
            return []
        except Exception as e:
            logger.error(f"Ranobes search error: {e}")
            return []
    
    def _parse_ranobes_results(self, soup, query, base_url='https://ranobes.net', url_pattern=r'/novels/\d+.*\.html'):
        """Parse Ranobes search results from soup."""
        results = []
        
        # Ranobes search results are in .short-cont or article.short blocks
        # ranobes.com uses /ranobe/ while ranobes.net uses /novels/
        novel_path = '/ranobe/' if 'ranobes.com' in base_url else '/novels/'
        
        items = soup.select('.short-cont .short-title a, article.short .short-title a')
        if not items:
            items = soup.select('.shortstory .short-title a, .shortstory h2 a')
        if not items:
            items = soup.select(f'a[href*="{novel_path}"][href$=".html"]')
        if not items:
            # Try finding any links to novel pages
            items = soup.find_all('a', href=lambda h: h and novel_path in h and '.html' in h)

        for a in items[:10]:
            href = a.get('href', '')
            if not href:
                continue
            # Must be a novel page URL with ID
            if not re.search(url_pattern, href):
                continue
            if not href.startswith('http'):
                href = urljoin(base_url, href)
            title = a.get('title') or a.get_text(strip=True)
            # Clean up title
            title = re.sub(r'\s*-\s*Ranobes.*', '', title, flags=re.I)
            if title and href and not any(r['url'] == href for r in results):
                results.append({'title': title, 'url': href, 'source': 'Ranobes'})
        
        if results:
            logger.info(f"[Ranobes] Found {len(results)} results for '{query}'")
        return results

    def _search_novelfire(self, query):
        """Search NovelFire."""
        try:
            search_url = f"https://novelfire.net/search?keyword={quote_plus(query)}"
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=15)
            
            if resp.status_code != 200:
                logger.debug(f"[NovelFire] HTTP {resp.status_code}")
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # NovelFire uses a[href*="/book/"] for results
            items = soup.select('a[href*="/book/"]')
                
            results = []
            seen_urls = set()
            for item in items:
                href = item.get('href', '')
                if not href or '/chapters' in href or href in seen_urls:
                    continue
                if not href.startswith('http'):
                    href = 'https://novelfire.net' + href
                title = item.get('title') or item.get_text(strip=True)
                # Clean up title - remove rank info
                if title:
                    title = title.split('Rank')[0].strip()
                if title and len(title) > 2:
                    seen_urls.add(href)
                    results.append({'title': title, 'url': href, 'source': 'NovelFire'})
                if len(results) >= 5:
                    break
            
            logger.info(f"[NovelFire] Found {len(results)} results for '{query}'")
            return results
        except Exception as e:
            logger.error(f"NovelFire search error: {e}")
            return []

    def _search_freewebnovel(self, query):
        """Search FreeWebNovel with correct URL and selectors."""
        try:
            # freewebnovel.com uses searchkey parameter
            search_url = f"https://freewebnovel.com/search?searchkey={quote_plus(query)}"
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=15)
            
            if resp.status_code != 200:
                logger.debug(f"[FreeWebNovel] HTTP {resp.status_code}")
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # FreeWebNovel uses .tit a for novel links
            items = soup.select('.tit a')
            if not items:
                items = soup.select('.book-img-text li a, .novel-item a')
                
            results = []
            seen_urls = set()
            for item in items:
                href = item.get('href', '')
                if not href or href in seen_urls:
                    continue
                if not href.startswith('http'):
                    href = 'https://freewebnovel.com' + href
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    seen_urls.add(href)
                    results.append({'title': title, 'url': href, 'source': 'FreeWebNovel'})
                if len(results) >= 5:
                    break
            
            logger.info(f"[FreeWebNovel] Found {len(results)} results for '{query}'")
            return results
        except Exception as e:
            logger.error(f"FreeWebNovel search error: {e}")
            return []

    def _search_creativenovels(self, query):
        """Search CreativeNovels with proper selectors."""
        try:
            search_url = f"https://creativenovels.com/?s={quote_plus(query)}"
            resp = self._search_fetch(search_url, 'CreativeNovels')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # CreativeNovels uses links with /novel/ in the URL
            items = soup.select('a[href*="creativenovels.com/novel/"]')
            if not items:
                items = soup.select('.post-title a, article.post h2 a, .entry-title a')
            
            results = []
            seen_urls = set()
            for item in items:
                href = item.get('href', '')
                if not href or '/novel/' not in href:
                    continue
                if href in seen_urls:
                    continue
                seen_urls.add(href)
                if not href.startswith('http'):
                    href = 'https://creativenovels.com' + href
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'CreativeNovels'})
                if len(results) >= 5:
                    break
            return results
        except Exception as e:
            logger.error(f"CreativeNovels search error: {e}")
            return []

    def _search_boxnovel(self, query):
        """Search BoxNovel with proper selectors."""
        try:
            search_url = f"https://boxnovel.com/?s={quote_plus(query)}&post_type=wp-manga"
            resp = self._search_fetch(search_url, 'BoxNovel')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # BoxNovel uses WordPress manga theme
            items = soup.select('.post-title h3 a, .post-title h4 a, .c-tabs-item__content .post-title a')
            if not items:
                items = soup.select('a[href*="/novel/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'BoxNovel'})
            return results
        except Exception as e:
            logger.error(f"BoxNovel search error: {e}")
            return []

    def _search_lightnovelworld(self, query):
        """Search LightNovelWorld with Cloudflare bypass."""
        try:
            # Try multiple domains - lightnovelworld.org works best
            domains = ['lightnovelworld.org', 'www.lightnovelworld.com', 'lightnovelworld.co', 'lnworld.com']
            
            for domain in domains:
                search_url = f"https://{domain}/search?keyword={quote_plus(query)}"
                resp = self._search_fetch(search_url, 'LightNovelWorld')
                
                if not resp:
                    continue
                    
                soup = BeautifulSoup(resp.content, 'html.parser')
                
                # LightNovelWorld uses h3.card-title for novel names
                # The link is in a parent div container
                results = []
                seen_urls = set()
                
                for card in soup.select('h3.card-title'):
                    title = card.get_text(strip=True)
                    if not title or title in ['Navigation', 'Actions', 'Settings']:
                        continue
                    
                    # Find the link in parent divs
                    parent = card.parent
                    link = None
                    for _ in range(5):
                        if parent is None:
                            break
                        link = parent.find('a', href=lambda h: h and '/novel/' in h)
                        if link:
                            break
                        parent = parent.parent
                    
                    if not link:
                        continue
                        
                    href = link.get('href', '')
                    if not href or href in seen_urls:
                        continue
                    seen_urls.add(href)
                    
                    if not href.startswith('http'):
                        href = f'https://{domain}' + href
                    
                    results.append({'title': title, 'url': href, 'source': 'LightNovelWorld'})
                    if len(results) >= 5:
                        break
                
                if results:
                    logger.info(f"[LightNovelWorld] Found {len(results)} results for '{query}'")
                    return results
                    
            return []
        except Exception as e:
            logger.error(f"LightNovelWorld search error: {e}")
            return []

    def _search_lnmtl(self, query):
        """Search LNMTL (Machine Translation) with proper selectors."""
        try:
            # LNMTL uses /novel?q= for search
            search_url = f"https://lnmtl.com/novel?q={quote_plus(query)}"
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=15)
            
            if resp.status_code != 200:
                logger.debug(f"[LNMTL] HTTP {resp.status_code}")
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # LNMTL uses .media-title a for novel links
            items = soup.select('.media-title a')
            if not items:
                items = soup.select('a[href*="/novel/"]')
                
            results = []
            seen = set()
            for item in items:
                href = item.get('href', '')
                if href in seen or not href:
                    continue
                if not href.startswith('http'):
                    href = 'https://lnmtl.com' + href
                title = item.get_text(strip=True)
                if title and href and '/novel/' in href:
                    seen.add(href)
                    results.append({'title': title, 'url': href, 'source': 'LNMTL'})
                if len(results) >= 5:
                    break
                    
            logger.info(f"[LNMTL] Found {len(results)} results for '{query}'")
            return results
        except Exception as e:
            logger.error(f"LNMTL search error: {e}")
            return []

    def _search_readernovel(self, query):
        """Search ReaderNovel with proper selectors."""
        try:
            # readernovel.net works, readernovel.com is broken
            search_url = f"https://readernovel.net/?s={quote_plus(query)}&post_type=wp-manga"
            resp = self._search_fetch(search_url, 'ReaderNovel')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # WordPress manga theme
            items = soup.select('.post-title h3 a, .post-title h4 a, .c-tabs-item__content a')
            if not items:
                items = soup.select('a[href*="/novel/"], a[href*="/manga/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'ReaderNovel'})
            return results
        except Exception as e:
            logger.error(f"ReaderNovel search error: {e}")
            return []

    def _search_novelbuddy(self, query):
        """Search NovelBuddy with proper selectors."""
        try:
            # NovelBuddy uses /search endpoint
            search_url = f"https://novelbuddy.com/search?q={quote_plus(query)}"
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=15)
            
            if resp.status_code != 200:
                logger.debug(f"[NovelBuddy] HTTP {resp.status_code}")
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # NovelBuddy uses book-item or book-detailed-item
            items = soup.select('.book-item a, .book-detailed-item a')
            if not items:
                items = soup.select('a[href*="/novel/"]')
                
            results = []
            seen = set()
            for item in items:
                href = item.get('href', '')
                if not href or href in seen:
                    continue
                if not href.startswith('http'):
                    href = 'https://novelbuddy.com' + href
                # Only include novel pages, not chapter pages
                if '/chapter-' in href:
                    continue
                title = item.get('title') or item.get_text(strip=True)
                if title and href and '/novel/' in href:
                    seen.add(href)
                    results.append({'title': title, 'url': href, 'source': 'NovelBuddy'})
                if len(results) >= 5:
                    break
                    
            logger.info(f"[NovelBuddy] Found {len(results)} results for '{query}'")
            return results
        except Exception as e:
            logger.error(f"NovelBuddy search error: {e}")
            return []

    def _search_lightnovelcave(self, query):
        """Search LightNovelCave with proper selectors."""
        try:
            # Try multiple domains
            domains = ['lightnovelcave.com', 'lightnovelcave.co', 'lncave.com']
            
            for domain in domains:
                search_url = f"https://{domain}/search?keyword={quote_plus(query)}"
                resp = self._search_fetch(search_url, 'LightNovelCave')
                if not resp:
                    continue
                    
                soup = BeautifulSoup(resp.content, 'html.parser')
                
                # Similar to LightNovelWorld
                items = soup.select('.novel-item .novel-title a, .novel-list .novel-title a')
                if not items:
                    items = soup.select('a[href*="/novel/"]')
                    
                results = []
                for item in items[:5]:
                    href = item.get('href', '')
                    if href and not href.startswith('http'):
                        href = f'https://{domain}' + href
                    title = item.get('title') or item.get_text(strip=True)
                    if title and href:
                        results.append({'title': title, 'url': href, 'source': 'LightNovelCave'})
                        
                if results:
                    return results
                    
            return []
        except Exception as e:
            logger.error(f"LightNovelCave search error: {e}")
            return []

    def _search_libread(self, query):
        """Search LibRead with proper selectors."""
        try:
            # libread.com works, libread.org is broken
            search_url = f"https://libread.com/?s={quote_plus(query)}"
            resp = self._search_fetch(search_url, 'LibRead')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # LibRead uses links with /libread/ in href
            items = soup.select('a[href*="/libread/"]')
            if not items:
                items = soup.select('.book-list .bookname a, .novel-list .novel-title a')
                
            results = []
            seen_urls = set()
            for item in items:
                href = item.get('href', '')
                if not href or '/libread/' not in href:
                    continue
                # Skip chapter links, only get novel pages
                if '/chapter-' in href:
                    continue
                if href in seen_urls:
                    continue
                seen_urls.add(href)
                if not href.startswith('http'):
                    href = 'https://libread.com' + href
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'LibRead'})
                if len(results) >= 5:
                    break
                    
            return results
        except Exception as e:
            logger.error(f"LibRead search error: {e}")
            return []

    def _search_empirenovel(self, query):
        """Search EmpireNovel with proper selectors."""
        try:
            search_url = f"https://empirenovel.com/?s={quote_plus(query)}&post_type=wp-manga"
            resp = self._search_fetch(search_url, 'EmpireNovel')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # WordPress manga theme
            items = soup.select('.post-title h3 a, .post-title h4 a')
            if not items:
                items = soup.select('a[href*="/manga/"], a[href*="/novel/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'EmpireNovel'})
            return results
        except Exception as e:
            logger.error(f"EmpireNovel search error: {e}")
            return []

    def _search_wtrlab(self, query):
        """Search WTR-Lab with proper selectors."""
        try:
            search_url = f"https://wtr-lab.com/en/search?q={quote_plus(query)}"
            resp = self._search_fetch(search_url, 'WTR-Lab')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # WTR-Lab uses novel cards
            items = soup.select('.novel-item a, .search-result a[href*="/serie-"]')
            if not items:
                items = soup.select('a[href*="/serie-"], a[href*="/novel/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                if href and not href.startswith('http'):
                    href = 'https://wtr-lab.com' + href
                title = item.get('title') or item.get_text(strip=True)
                if title and href and len(title) > 2:
                    results.append({'title': title, 'url': href, 'source': 'WTR-Lab'})
            return results
        except Exception as e:
            logger.error(f"WTR-Lab search error: {e}")
            return []

    def _search_fullnovels(self, query):
        """Search FullNovels with proper selectors."""
        try:
            search_url = f"https://fullnovels.com/?s={quote_plus(query)}&post_type=wp-manga"
            resp = self._search_fetch(search_url, 'FullNovels')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # WordPress manga theme
            items = soup.select('.post-title h3 a, .post-title h4 a')
            if not items:
                items = soup.select('a[href*="/manga/"], a[href*="/novel/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'FullNovels'})
            return results
        except Exception as e:
            logger.error(f"FullNovels search error: {e}")
            return []

    def _search_nicenovel(self, query):
        """Search NiceNovel with proper selectors."""
        try:
            search_url = f"https://nicenovel.net/search?keyword={quote_plus(query)}"
            resp = self._search_fetch(search_url, 'NiceNovel')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            items = soup.select('.novel-item a, .book-item a, .search-item a')
            if not items:
                items = soup.select('a[href*="/novel/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                if href and not href.startswith('http'):
                    href = 'https://nicenovel.net' + href
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'NiceNovel'})
            return results
        except Exception as e:
            logger.error(f"NiceNovel search error: {e}")
            return []

    def _search_bednovel(self, query):
        """Search BedNovel with proper selectors."""
        try:
            search_url = f"https://bednovel.com/?s={quote_plus(query)}&post_type=wp-manga"
            resp = self._search_fetch(search_url, 'BedNovel')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # WordPress manga theme
            items = soup.select('.post-title h3 a, .post-title h4 a')
            if not items:
                items = soup.select('a[href*="/manga/"], a[href*="/novel/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'BedNovel'})
            return results
        except Exception as e:
            logger.error(f"BedNovel search error: {e}")
            return []

    def _search_allnovelbook(self, query):
        """Search AllNovelBook with proper selectors."""
        try:
            search_url = f"https://allnovelbook.com/?s={quote_plus(query)}&post_type=wp-manga"
            resp = self._search_fetch(search_url, 'AllNovelBook')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # WordPress manga theme
            items = soup.select('.post-title h3 a, .post-title h4 a')
            if not items:
                items = soup.select('a[href*="/manga/"], a[href*="/novel/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'AllNovelBook'})
            return results
        except Exception as e:
            logger.error(f"AllNovelBook search error: {e}")
            return []

    def _search_yonglibrary(self, query):
        """Search YongLibrary with proper selectors."""
        try:
            search_url = f"https://yonglibrary.com/?s={quote_plus(query)}&post_type=wp-manga"
            resp = self._search_fetch(search_url, 'YongLibrary')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # WordPress manga theme
            items = soup.select('.post-title h3 a, .post-title h4 a')
            if not items:
                items = soup.select('a[href*="/manga/"], a[href*="/novel/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                title = item.get('title') or item.get_text(strip=True)
                # Skip empty/generic titles
                if not title or title.lower() in ['novel', 'manga', 'read', '']:
                    continue
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'YongLibrary'})
            return results
        except Exception as e:
            logger.error(f"YongLibrary search error: {e}")
            return []

    def _search_englishnovelsfree(self, query):
        """Search EnglishNovelsFree with proper selectors."""
        try:
            search_url = f"https://englishnovelsfree.com/?s={quote_plus(query)}&post_type=wp-manga"
            resp = self._search_fetch(search_url, 'EnglishNovelsFree')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # WordPress manga theme
            items = soup.select('.post-title h3 a, .post-title h4 a')
            if not items:
                items = soup.select('a[href*="/manga/"], a[href*="/novel/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'EnglishNovelsFree'})
            return results
        except Exception as e:
            logger.error(f"EnglishNovelsFree search error: {e}")
            return []

    def _search_readnovelfull(self, query):
        """Search ReadNovelFull with proper selectors."""
        try:
            # readnovelfull.net works, .com and .me are broken
            search_url = f"https://readnovelfull.net/search?keyword={quote_plus(query)}"
            resp = self._search_fetch(search_url, 'ReadNovelFull')
            if not resp:
                return []
                
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # ReadNovelFull uses novel-title class
            items = soup.select('.novel-title a, .book-name a, h3.title a')
            if not items:
                items = soup.select('a[href*="/novel-book/"], a[href*="/novel/"]')
                
            results = []
            for item in items[:5]:
                href = item.get('href', '')
                if href and not href.startswith('http'):
                    href = 'https://readnovelfull.net' + href
                title = item.get('title') or item.get_text(strip=True)
                if title and href:
                    results.append({'title': title, 'url': href, 'source': 'ReadNovelFull'})
            return results
        except Exception as e:
            logger.error(f"ReadNovelFull search error: {e}")
            return []

    # Free sites (no paywalls)
    FREE_SITES = [
        'novelbin', 'novelfire', 'royalroad', 'creativenovels', 'lnmtl',
        'readernovel', 'boxnovel', 'freewebnovel', 'lightnovelworld',
        'readnovelfull', 'novellive', 'wtr-lab', 'ranobes'
    ]
    # Paid sites (have paywalls/premium chapters)
    PAID_SITES = ['webnovel', 'qidian', 'wuxiaworld']

    def __init__(self,
                 parallel_workers: int = 3,
                 use_playwright: bool = None):
        # Use a single persistent User-Agent and HTTP session per Scraper
        # instance so cookies and identity are reused across requests.
        # NOTE: Keep parallel_workers low (3-5) to avoid anti-bot detection
        self.headers = {'User-Agent': random.choice(USER_AGENTS)}
        self.session = requests.Session()
        
        # Debug mode (enable rich logging + HTML dumps for diagnostics)
        self.debug_mode = os.getenv('SCRAPER_DEBUG', '0') == '1'
        
        # Configure proxy from environment variable (format: http://user:pass@host:port)
        # For Webshare rotating proxy, use: http://user-rotate:pass@p.webshare.io:80
        self.proxy_url = os.getenv('PROXY_URL', '')
        self.rotating_proxy_url = os.getenv('ROTATING_PROXY_URL', '')  # Optional separate rotating proxy
        if self.proxy_url:
            self.session.proxies = {
                'http': self.proxy_url,
                'https': self.proxy_url
            }
            logger.info(f"Scraper using proxy: {self.proxy_url.split('@')[-1] if '@' in self.proxy_url else self.proxy_url}")
        
        # Configure session for better anti-detection
        self.session.headers.update(self._get_random_headers())

        # Lightweight bandwidth accounting (in-memory, resets on restart)
        self.bandwidth_month = None
        self.bandwidth_bytes = 0
        self.bandwidth_budget_bytes = 10 * 1024 * 1024 * 1024  # 10GB proxy budget
        self.bandwidth_warn70 = False
        self.bandwidth_warn90 = False
        
        # Pre-set cookies for sites that require them (e.g., Ranobes browser check)
        self.session.cookies.set('browser_check', '1', domain='ranobes.net')
        
        # Set up connection pooling for better performance
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        # Configure automatic retries for connection issues
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.throttle_delay = 0  # No delay - maximum speed
        self.max_retries = 4  # Increased retries for Cloudflare bypass attempts
        self.parallel_workers = parallel_workers  # Use tier-based worker count from bot
        self.progress_callback = None  # Callback for progress updates
        self.cancel_check = None  # Callback to check if scraping should be cancelled
        self.link_progress_callback = None  # Callback for link collection progress
        self.request_count = 0  # Track requests for rate limiting
        # Optional browser-based fallback (Playwright/Selenium). Disabled by default
        # unless explicitly enabled via argument or USE_PLAYWRIGHT=1.
        if use_playwright is None:
            use_playwright = os.getenv('USE_PLAYWRIGHT', '0') == '1'
        self.use_playwright = use_playwright
        self._playwright_scraper = None
        
        # FlareSolverr configuration for Cloudflare bypass
        self.flaresolverr_url = os.getenv('FLARESOLVERR_URL', 'http://localhost:8191/v1')
        self.flaresolverr_enabled = bool(os.getenv('FLARESOLVERR_URL', ''))
        if self.flaresolverr_enabled:
            logger.info(f"[SCRAPER] FlareSolverr enabled at {self.flaresolverr_url}")
        
        logger.info(f"[SCRAPER] Initialized with {parallel_workers} workers, playwright={use_playwright}")
        if self.debug_mode:
            logger.info("[SCRAPER] Debug mode enabled (SCRAPER_DEBUG=1)")

    def _debug_dump_html(self, url: str, html: str, tag: str) -> None:
        """Dump HTML to debug/<domain>/<timestamp>_<tag>.html when debug mode is on."""
        try:
            if not self.debug_mode or not html:
                return
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc or 'unknown-domain'
            ts = int(time.time())
            out_dir = os.path.join(os.getcwd(), 'debug', domain)
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, f"{ts}_{tag}.html")
            # Limit file size to avoid huge dumps
            content = html if len(html) < 2_000_000 else html[:2_000_000]
            with open(out_path, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info(f"[DEBUG] HTML dumped: {out_path}")
        except Exception as e:
            logger.debug(f"[DEBUG] Failed to dump HTML: {e}")

    def _track_bandwidth(self, resp: Optional[requests.Response]) -> None:
        """Track response size and warn as we approach the proxy budget."""
        try:
            if not resp:
                return

            from datetime import datetime

            current_month = datetime.utcnow().strftime("%Y-%m")
            if self.bandwidth_month != current_month:
                self.bandwidth_month = current_month
                self.bandwidth_bytes = 0
                self.bandwidth_warn70 = False
                self.bandwidth_warn90 = False

            content_length = resp.headers.get('Content-Length') if resp.headers else None
            if content_length and str(content_length).isdigit():
                size_bytes = int(content_length)
            else:
                size_bytes = len(resp.content or b"")

            self.bandwidth_bytes += size_bytes

            if self.bandwidth_budget_bytes <= 0:
                return

            usage_pct = self.bandwidth_bytes / self.bandwidth_budget_bytes
            if not self.bandwidth_warn70 and usage_pct >= 0.7:
                self.bandwidth_warn70 = True
                used_mb = self.bandwidth_bytes / 1_048_576
                budget_mb = self.bandwidth_budget_bytes / 1_048_576
                logger.warning(f"[BANDWIDTH] ~70% used ({used_mb:.1f} MB of {budget_mb:.0f} MB). Consider pausing heavy scraping.")
            if not self.bandwidth_warn90 and usage_pct >= 0.9:
                self.bandwidth_warn90 = True
                used_mb = self.bandwidth_bytes / 1_048_576
                budget_mb = self.bandwidth_budget_bytes / 1_048_576
                logger.error(f"[BANDWIDTH] ~90% used ({used_mb:.1f} MB of {budget_mb:.0f} MB). Reduce requests or rotate proxies.")
        except Exception as e:
            logger.debug(f"[BANDWIDTH] Tracking error: {e}")

    def _get_fresh_session(self, rotate_ip: bool = False) -> requests.Session:
        """Create a fresh session with new headers and optionally rotated IP.
        
        Use this for sites with heavy anti-bot protection to avoid fingerprinting.
        If rotate_ip=True and ROTATING_PROXY_URL is set, uses the rotating proxy.
        """
        session = requests.Session()
        session.headers.update(self._get_random_headers())
        
        # Use rotating proxy if available and requested
        proxy_url = self.rotating_proxy_url if (rotate_ip and self.rotating_proxy_url) else self.proxy_url
        if proxy_url:
            session.proxies = {'http': proxy_url, 'https': proxy_url}
        
        # Set common cookies
        session.cookies.set('browser_check', '1', domain='ranobes.net')
        
        return session

    def _search_fetch(self, url: str, site_name: str, timeout: int = 10, rotate_on_fail: bool = False) -> Optional[requests.Response]:
        """Fetch a URL for search purposes with proper headers and logging.
        
        Uses fresh session for heavy-security sites to avoid fingerprinting.
        Returns None if the request fails or returns a Cloudflare challenge.
        """
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc
            
            def fetch_once(sess: requests.Session) -> Optional[requests.Response]:
                headers = self._get_cloudflare_bypass_headers(url)
                resp_inner = sess.get(url, headers=headers, timeout=timeout)
                self._track_bandwidth(resp_inner)
                
                if resp_inner.status_code == 403:
                    logger.debug(f"[{site_name}] 403 Forbidden - likely Cloudflare blocked")
                    return None
                elif resp_inner.status_code == 503:
                    logger.debug(f"[{site_name}] 503 - Cloudflare challenge or maintenance")
                    return None
                elif resp_inner.status_code != 200:
                    logger.debug(f"[{site_name}] HTTP {resp_inner.status_code}")
                    return None
                
                # Check for Cloudflare challenge page
                content_sample_local = resp_inner.text[:500].lower() if resp_inner.text else ''
                if 'checking your browser' in content_sample_local:
                    logger.debug(f"[{site_name}] Cloudflare challenge detected")
                    self._debug_dump_html(url, resp_inner.text, f"cloudflare_challenge_{site_name}")
                    return None
                return resp_inner

            # Use fresh session with rotating IP for heavy-security sites
            heavy_security = any(site in domain.lower() for site in ['ranobes', 'novelbin', 'lightnovelworld'])
            if heavy_security:
                session = self._get_fresh_session(rotate_ip=True)
                logger.debug(f"[{site_name}] Using fresh session for heavy-security site")
            else:
                session = self.session
            
            resp = fetch_once(session)

            if not resp and rotate_on_fail:
                alt_session = self._get_fresh_session(rotate_ip=True)
                logger.debug(f"[{site_name}] Rotating proxy and retrying search fetch")
                resp = fetch_once(alt_session)

            if resp and resp.text and self.debug_mode:
                soup_debug = BeautifulSoup(resp.text, 'html.parser')
                title_tag = soup_debug.find('title')
                page_title = title_tag.get_text()[:80] if title_tag else 'No title'
                main_divs = soup_debug.find_all('div', class_=True)[:8]
                div_classes = [' '.join(div.get('class', [])) for div in main_divs]
                logger.debug(f"[{site_name}] Page title: {page_title}")
                logger.debug(f"[{site_name}] Top div classes: {div_classes}")
            
            return resp
            
        except requests.exceptions.Timeout:
            logger.debug(f"[{site_name}] Timeout")
            return None
        except Exception as e:
            logger.debug(f"[{site_name}] Error: {type(e).__name__}")
            return None

    def _is_paid_site(self, url: str) -> bool:
        """Check if URL is from a site with paywalls"""
        domain = urlparse(url).netloc.lower()
        return any(site in domain for site in self.PAID_SITES)

    def get_novel_metadata(self, url: str) -> Dict:
        """
        Quickly fetch novel metadata (title, total chapters) without downloading content.
        Used for limit enforcement before full scraping.

        Returns: {'title': str, 'total_chapters': int, 'error': str (if failed)}
        """
        try:
            logger.info(f"Fetching metadata for {url}")

            # WebNovel.com scraping disabled: Playwright not supported on Termux/Android
            # Be careful: 'freewebnovel.com' contains 'webnovel.com' so use exact domain check
            domain = urlparse(url).netloc.lower()
            if domain == 'webnovel.com' or domain == 'www.webnovel.com':
                return {
                    'title':
                    'Unknown',
                    'total_chapters':
                    0,
                    'error':
                    'WebNovel.com scraping is not supported on this platform (Playwright required)'
                }

            # Special handling for Ranobes - count actual chapter links
            if 'ranobes' in url:
                return self._get_ranobes_metadata(url)

            resp = self._get_with_retry(url, rotate_on_block=True, rotate_per_attempt=True)
            if not resp:
                # Browser-based fallback for sites that block plain requests
                if self.use_playwright and 'novelbin' in url:
                    html = self._browser_fetch_html(url)
                    if not html:
                        return {
                            'title': 'Unknown',
                            'total_chapters': 0,
                            'error': 'Failed to fetch URL'
                        }
                    soup = BeautifulSoup(html, 'html.parser')
                else:
                    return {
                        'title': 'Unknown',
                        'total_chapters': 0,
                        'error': 'Failed to fetch URL'
                    }
            else:
                soup = BeautifulSoup(resp.content, 'html.parser')
            title = self._extract_title(soup, url)
            
            # Extract full metadata (author, cover, translator, etc.)
            metadata = self._extract_full_metadata(soup, url)

            # Site-specific chapter counting
            if 'novelbin' in url:
                nb_count = self._get_novelbin_chapter_count(soup, url)
                if nb_count:
                    logger.info(
                        f"Metadata: {title} - {nb_count} chapters (NovelBin chapter list)"
                    )
                    return {'title': title, 'total_chapters': nb_count, 'metadata': metadata}

            # Use the same method as get_chapter_count for accuracy
            total_chapters = 0

            # Method 1: Extract chapter links from HTML (most accurate)
            chapter_urls = self._get_chapter_links_from_html(soup, url)
            if chapter_urls:
                # Extract max chapter number from URLs
                chapter_nums = set()
                for ch_url in chapter_urls:
                    chap_num = self._extract_chapter_number(ch_url)
                    if chap_num and chap_num != 999999:
                        chapter_nums.add(chap_num)
                if chapter_nums:
                    total_chapters = max(chapter_nums)
                    logger.info(
                        f"Metadata: {title} - {total_chapters} chapters (from HTML links)"
                    )
                    return {'title': title, 'total_chapters': total_chapters, 'metadata': metadata}

            # Method 2: Look for "X chapters" text in page (fallback)
            import re
            page_text = soup.get_text()
            match = re.search(r'(\d{2,})\s*(?:chapters?|ch\.)', page_text,
                              re.IGNORECASE)
            if match:
                total_chapters = int(match.group(1))
                logger.info(
                    f"Metadata: {title} - {total_chapters} chapters (from page text)"
                )
                return {'title': title, 'total_chapters': total_chapters, 'metadata': metadata}

            # Method 3: Count raw chapter links (last resort)
            chapter_links = soup.find_all(
                'a',
                href=lambda h: h and
                ('/chapter' in h.lower() or 'chapter-' in h.lower()))
            if chapter_links:
                total_chapters = len(chapter_links)

            # Default if nothing found
            if total_chapters == 0:
                total_chapters = 500  # Conservative default

            logger.info(f"Metadata: {title} - {total_chapters} chapters")
            return {'title': title, 'total_chapters': total_chapters, 'metadata': metadata}

        except Exception as e:
            logger.error(f"Error fetching metadata: {e}")
            return {'title': 'Unknown', 'total_chapters': 500, 'error': str(e)}

    def get_chapter_count(self, url: str) -> int:
        """Compatibility wrapper used by the bot to get total chapters.

        Delegates to get_novel_metadata so all sites share the same logic.
        Returns an integer chapter count (0 if unknown).
        """
        try:
            meta = self.get_novel_metadata(url)
            count = int(meta.get('total_chapters', 0) or 0)
            logger.info(f"get_chapter_count({url}) -> {count}")
            return count
        except Exception as e:
            logger.error(f"get_chapter_count error for {url}: {e}")
            return 0

    def _get_novelbin_chapter_count(self, soup: BeautifulSoup,
                                     url: str) -> Optional[int]:
        """Try to determine total chapters on NovelBin from chapter links.

        Prefers the highest chapter number found in chapter URLs/text such as
        "Chapter 156: ..." to avoid outdated "X chapters" counters.
        """
        try:
            chapter_nums = set()

            # Collect numbers from chapter links in the chapter list
            links = soup.find_all(
                'a',
                href=lambda h: h and 'chapter' in h.lower())
            for a in links:
                href = a.get('href', '')
                if not href:
                    continue
                full_url = urljoin(url, href)
                num = self._extract_chapter_number(full_url)
                if num and num != 999999:
                    chapter_nums.add(num)

            # Fallback: parse from "Chapter 156:" text if present
            if not chapter_nums:
                text = soup.get_text(" \n", strip=True)
                m = re.findall(r'Chapter\s*(\d+)\s*:', text, re.I)
                if m:
                    for s in m:
                        try:
                            chapter_nums.add(int(s))
                        except ValueError:
                            continue

            if chapter_nums:
                return max(chapter_nums)
        except Exception as e:
            logger.warning(f"NovelBin chapter count error for {url}: {e}")
        return None

    def _count_ranobes_chapters(self, url: str) -> Optional[int]:
        """Count total chapters for Ranobes by extracting from JavaScript data.

        Uses the same method as the scraper - gets count_all from embedded JSON.
        This ensures we get TRANSLATED chapters, not original chapters.
        """
        try:
            logger.info(f"Counting Ranobes chapters for {url}")

            # Extract novel ID from URL like https://ranobes.net/novels/164915-a-monster-who-levels-up.html
            match = re.search(r'/novels/(\d+)', url)
            if not match:
                logger.warning(f"Could not extract novel ID from {url}")
                return None

            novel_id = match.group(1)
            slug_match = re.search(r'/novels/\d+-([^./]+)', url)
            slug = slug_match.group(1).lower() if slug_match else None
            base_chapters_url = f"https://ranobes.net/chapters/{novel_id}/"
            
            html_content = None
            
            # Try FlareSolverr first (best for Cloudflare bypass)
            if self.flaresolverr_enabled:
                html_content = self._fetch_with_flaresolverr(base_chapters_url)
                if html_content:
                    logger.info(f"[RANOBES] FlareSolverr succeeded for chapter count")
            
            # Fallback to regular requests
            if not html_content:
                resp = self._get_with_retry(base_chapters_url, rotate_on_block=True, rotate_per_attempt=True)
                if resp:
                    html_content = resp.text
            
            # Fallback to Playwright
            if not html_content:
                logger.info(f"[RANOBES] Trying Playwright fallback for chapter count: {base_chapters_url}")
                try:
                    from playwright_scraper import get_scraper_instance, run_in_pw_loop
                    pw_scraper = get_scraper_instance()
                    result = run_in_pw_loop(pw_scraper.get_page_content(base_chapters_url, wait_for_js=True))
                    html_content = result[0] if result else None
                    if html_content:
                        logger.info(f"[RANOBES] Playwright succeeded for chapter count page")
                except Exception as pw_err:
                    logger.error(f"[RANOBES] Playwright error for chapter count: {pw_err}")
            
            if not html_content:
                logger.warning(f"[RANOBES] All fetch methods failed for chapter count")
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            # Log first 500 chars to see what we're getting
            logger.info(f"[RANOBES] HTML preview: {html_content[:500]}...")
            
            dle_content = soup.find("div", id="dle-content")
            if not dle_content:
                # Debug: check what divs actually exist
                all_divs = soup.find_all("div", id=True)[:10]  # First 10 divs with IDs
                div_ids = [div.get('id') for div in all_divs]
                logger.warning(f"No div#dle-content found. Available div IDs: {div_ids}")
                
                # Try alternative selectors
                dle_content = (soup.find("div", id="content") or  # New Ranobes structure
                             soup.find("div", class_="dle-content") or 
                             soup.find("main") or 
                             soup.find("article") or
                             soup.find("div", class_="content"))
                
                if not dle_content:
                    logger.error(f"No content container found at all")
                    return None
                else:
                    logger.info(f"Using alternative content container: {dle_content.name}.{dle_content.get('class', [])} #{dle_content.get('id', '')}")

            # Try to locate __DATA__ scripts anywhere on the page
            script_tag = None
            all_scripts = soup.find_all("script")
            logger.info(f"[RANOBES] Found {len(all_scripts)} script tags on page")
            scripts_with_content = 0
            for i, script in enumerate(all_scripts):
                txt = (script.string or script.get_text() or "").strip()
                if not txt:
                    continue
                scripts_with_content += 1
                # Log first 200 chars of each script for debugging
                if 'DATA' in txt or 'chapters' in txt.lower() or 'count' in txt.lower():
                    logger.info(f"[RANOBES] Script {i} contains potential data: {txt[:200]}...")
                if 'window.__DATA__' in txt:
                    script_tag = script
                    logger.info(f"[RANOBES] Found window.__DATA__ in script {i}")
                    break
            logger.info(f"[RANOBES] Scripts with content: {scripts_with_content}/{len(all_scripts)}")

            if script_tag:
                import json
                script_text = script_tag.string or script_tag.get_text() or ""
                # Use brace balancing for robust JSON extraction
                json_start = script_text.find('window.__DATA__')
                if json_start != -1:
                    brace_start = script_text.find('{', json_start)
                    if brace_start != -1:
                        brace_count = 0
                        json_end = brace_start
                        for i, char in enumerate(script_text[brace_start:]):
                            if char == '{':
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    json_end = brace_start + i + 1
                                    break
                        json_str = script_text[brace_start:json_end]
                        try:
                            data = json.loads(json_str)
                            total_chapters = int(data.get('count_all', 0))
                            if total_chapters > 0:
                                logger.info(
                                    f"Ranobes: Got chapter count from JavaScript data: {total_chapters}"
                                )
                                return total_chapters
                        except json.JSONDecodeError as e:
                            logger.warning(f"JSON parse error: {e}")

            # If no JSON count, fall back to HTML link parsing (chapters page then novel page)
            links = self._extract_ranobes_links_from_soup(soup)
            if not links:
                html_links = self._extract_ranobes_links_html_fallback(soup, novel_id, slug)
                if html_links:
                    links.extend(html_links)
            if not links:
                latest = self._extract_ranobes_latest_block(soup, base_chapters_url)
                if latest:
                    links.extend(latest)

            if not links:
                novel_html = None
                novel_soup = None
                
                # Try FlareSolverr first
                if self.flaresolverr_enabled:
                    novel_html = self._fetch_with_flaresolverr(url)
                    if novel_html:
                        novel_soup = BeautifulSoup(novel_html, 'html.parser')
                
                # Fallback to regular request
                if not novel_soup:
                    novel_resp = self._get_with_retry(url, rotate_on_block=True, rotate_per_attempt=True)
                    if novel_resp:
                        novel_soup = BeautifulSoup(novel_resp.content, 'html.parser')
                
                # Fallback to Playwright
                if not novel_soup:
                    try:
                        from playwright_scraper import get_scraper_instance, run_in_pw_loop
                        pw_scraper = get_scraper_instance()
                        result = run_in_pw_loop(pw_scraper.get_page_content(url, wait_for_js=True))
                        novel_html = result[0] if result else None
                        if novel_html:
                            novel_soup = BeautifulSoup(novel_html, 'html.parser')
                    except Exception as pw_err:
                        logger.warning(f"Playwright fallback failed for novel page: {pw_err}")
                
                if novel_soup:
                    links = self._extract_ranobes_latest_block(novel_soup, url)
                    if not links:
                        links = self._extract_ranobes_links_html_fallback(novel_soup, novel_id, slug)

            if links:
                logger.info(f"Ranobes: Chapter count derived from HTML links: {len(links)}")
                return len(list(dict.fromkeys(links)))

            logger.warning(
                "Ranobes: Could not determine chapter count from JavaScript or HTML"
            )
            return None
        except Exception as e:
            logger.error(f"Error counting Ranobes chapters: {e}")
            return None

    def _count_ranobes_chapters_html_fallback(self, url: str, slug: Optional[str] = None) -> Optional[int]:
        """Fallback method to count chapters by scraping HTML directly"""
        try:
            # Extract novel ID
            match = re.search(r'/novels/(\d+)', url)
            if not match:
                return None
            
            novel_id = match.group(1)
            if not slug:
                slug_match = re.search(r'/novels/\d+-([^./]+)', url)
                slug = slug_match.group(1).lower() if slug_match else None
            chapters_url = f"https://ranobes.net/chapters/{novel_id}/"
            
            resp = self._get_with_retry(chapters_url, rotate_on_block=True, rotate_per_attempt=True)
            if not resp:
                return None
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Look for chapter links in common patterns
            chapter_links = []
            
            # Try different selectors for chapter links
            selectors = []
            if slug:
                selectors.extend([
                    f'a[href*="/{slug}-"]',
                    f'a[href*="/{slug}/"]'
                ])
            selectors.extend([
                f'a[href*="-{novel_id}/"]',  # Novel ID pattern
                'a[href*="/chapter"]',  # Generic chapter pattern
                '.chapter-link',  # Class-based
                '[data-chapter]'  # Data attribute
            ])
            
            for selector in selectors:
                links = soup.select(selector)
                if links:
                    chapter_links = [a.get('href') for a in links if a.get('href')]
                    logger.info(f"Found {len(chapter_links)} chapter links using selector: {selector}")
                    break
            
            if not chapter_links:
                # Last resort: look for any links containing the novel name/ID
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href')
                    matches_slug = slug and slug in href.lower()
                    if href and (novel_id in href or matches_slug):
                        chapter_links.append(href)
                
                logger.info(f"Fallback found {len(chapter_links)} potential chapter links")
            
            return len(chapter_links) if chapter_links else None
            
        except Exception as e:
            logger.error(f"HTML fallback error: {e}")
            return None

    def _get_ranobes_metadata(self, url: str) -> Dict:
        """Get Ranobes metadata including cover, author, genre, translator, status, chapters."""
        try:
            logger.info(f"Fetching Ranobes metadata for {url}")
            
            html_content = None
            
            # Try FlareSolverr first (best for Cloudflare bypass)
            if self.flaresolverr_enabled:
                html_content = self._fetch_with_flaresolverr(url)
                if html_content:
                    logger.info(f"[RANOBES] FlareSolverr succeeded for metadata: {url}")
            
            # Fallback to regular requests
            if not html_content:
                resp = self._get_with_retry(url, rotate_on_block=True, rotate_per_attempt=True)
                if resp:
                    html_content = resp.text
            
            # Fallback to Playwright if still no content
            if not html_content:
                logger.info(f"[RANOBES] Trying Playwright fallback for metadata: {url}")
                try:
                    from playwright_scraper import get_scraper_instance, run_in_pw_loop
                    pw_scraper = get_scraper_instance()
                    result = run_in_pw_loop(pw_scraper.get_page_content(url, wait_for_js=True))
                    html_content = result[0] if result else None
                    if html_content:
                        logger.info(f"[RANOBES] Playwright succeeded for metadata: {url}")
                except Exception as pw_err:
                    logger.error(f"[RANOBES] Playwright error for metadata: {pw_err}")
            
            if not html_content:
                logger.error(f"[RANOBES] All fetch methods failed for metadata: {url}")
                return {
                    'title': 'Unknown',
                    'total_chapters': 500,
                    'error': 'Failed to fetch URL'
                }
            
            soup = BeautifulSoup(html_content, 'html.parser')
            title = self._extract_title(soup, url)
            count = self._count_ranobes_chapters(url) or 500

            # Initialize metadata
            metadata = {
                'cover_image': None,
                'author': None,
                'translator': None,
                'genre': None,
                'status': None,
                'status_coo': None,
                'language': None,
                'year': None,
                'original_chapters': None,
                'translated_chapters': None,
                'publishers': None,
                'description': None
            }

            # === COVER IMAGE ===
            # Try figure.cover background-image first
            cover_figure = soup.select_one('figure.cover')
            if cover_figure:
                style = cover_figure.get('style', '')
                url_match = re.search(r'url\(["\']?([^"\'()]+)["\']?\)', style)
                if url_match:
                    metadata['cover_image'] = url_match.group(1)

            # Fallback to img tag
            if not metadata['cover_image']:
                cover_img = soup.select_one(
                    'figure.cover img, .r-fullstory-poster img, .poster img, link[itemprop="image"]'
                )
                if cover_img:
                    src = cover_img.get('src') or cover_img.get('href')
                    if src:
                        metadata['cover_image'] = urljoin(url, src)

            # === SPEC SECTION METADATA (li elements) ===
            spec_section = soup.select_one('.r-fullstory-spec')
            if spec_section:
                li_elements = spec_section.find_all('li')
                for li in li_elements:
                    li_text = li.get_text(strip=True)

                    # Status in COO
                    if 'Status in COO:' in li_text:
                        link = li.find('a')
                        if link:
                            metadata['status_coo'] = link.get_text(strip=True)

                    # Translation status
                    elif li_text.startswith('Translation:'):
                        link = li.find('a')
                        if link:
                            metadata['status'] = link.get_text(strip=True)

                    # In original chapters
                    elif 'In original:' in li_text:
                        match = re.search(r'(\d+)\s*chapters?', li_text,
                                          re.IGNORECASE)
                        if match:
                            metadata['original_chapters'] = match.group(1)

                    # Translated chapters
                    elif li_text.startswith('Translated:'):
                        match = re.search(r'(\d+)\s*chapters?', li_text,
                                          re.IGNORECASE)
                        if match:
                            metadata['translated_chapters'] = match.group(1)

                    # Year of publishing
                    elif 'Year of publishing:' in li_text:
                        span = li.find('span', itemprop='copyrightYear')
                        if span:
                            metadata['year'] = span.get_text(strip=True)
                        else:
                            match = re.search(r'(\d{4})', li_text)
                            if match:
                                metadata['year'] = match.group(1)

                    # Language
                    elif li_text.startswith('Language:'):
                        span = li.find('span', itemprop='locationCreated')
                        if span:
                            link = span.find('a')
                            metadata['language'] = link.get_text(
                                strip=True) if link else span.get_text(
                                    strip=True)

                    # Authors
                    elif 'Authors:' in li_text:
                        author_span = li.find('span',
                                              itemprop='creator') or li.find(
                                                  'span', class_='tag_list')
                        if author_span:
                            links = author_span.find_all('a')
                            if links:
                                metadata['author'] = ', '.join(
                                    a.get_text(strip=True) for a in links)

                    # Translator
                    elif li_text.startswith('Translator:'):
                        trans_span = li.find('span', itemprop='translator')
                        if trans_span:
                            links = trans_span.find_all('a')
                            if links:
                                metadata['translator'] = ', '.join(
                                    a.get_text(strip=True) for a in links)

                    # Publishers
                    elif 'Publishers:' in li_text:
                        pub_span = li.find(
                            'span', itemprop='publisher') or li.find(
                                'span', class_='publishers_list')
                        if pub_span:
                            links = pub_span.find_all('a')
                            if links:
                                metadata['publishers'] = ', '.join(
                                    a.get_text(strip=True) for a in links)

            # === GENRE ===
            genre_elems = soup.select(
                '.r-fullstory-spec a[href*="/genre/"], .tag_list a[href*="/genre/"]'
            )
            if genre_elems:
                genres = [g.get_text(strip=True) for g in genre_elems[:5]]
                metadata['genre'] = ', '.join(genres)

            # === DESCRIPTION ===
            # Prioritize .moreless__full (complete text) over .moreless__short (truncated preview)
            # Must check separately because select_one returns first in DOM order
            desc_elem = soup.select_one('.moreless__full')
            if not desc_elem:
                desc_elem = soup.select_one('.moreless__short')
            if not desc_elem:
                # Fallback to other description selectors
                desc_elem = soup.select_one(
                    '.moreless_full, .r-description, .r-fullstory-desc, .description, '
                    '[itemprop="description"], .novel-description, .book-description'
                )

            if desc_elem:
                # Get all text content, preserving line breaks from <br> tags
                desc_text = desc_elem.get_text(separator='\n', strip=True)

                if desc_text:
                    # Clean up HTML entities and extra whitespace
                    desc_text = desc_text.replace('&nbsp;', ' ')
                    desc_text = desc_text.replace('&amp;', '&')
                    # Remove collapse/expand buttons
                    desc_text = desc_text.replace('Collapse',
                                                  '').replace('Read more', '')
                    # Split into lines and remove empty lines
                    lines = [
                        line.strip() for line in desc_text.split('\n')
                        if line.strip()
                    ]
                    # Rejoin with double newlines for paragraphs
                    desc_text = '\n\n'.join(lines)
                    # Keep full description (up to 5000 chars)
                    metadata['description'] = desc_text[:5000]

            # Log what was extracted
            logger.info(
                f"Ranobes metadata extracted: cover={bool(metadata.get('cover_image'))}, "
                f"author={metadata.get('author')}, status_coo={metadata.get('status_coo')}, "
                f"chapters={metadata.get('translated_chapters')}")

            result = {
                'title': title,
                'total_chapters': count,
                'metadata': metadata
            }
            return result

        except Exception as e:
            logger.error(f"Error fetching Ranobes metadata: {e}",
                         exc_info=True)
            return {'title': 'Unknown', 'total_chapters': 500, 'error': str(e)}

    def scrape(self,
               url: str,
               chapter_start: int = 1,
               chapter_end: Optional[int] = None,
               user_id: str = None) -> Dict:
        """
        Main entry point for scraping - coordinates metadata fetching and chapter downloading.
        
        Supports resume: if user_id is provided and a previous download was interrupted,
        only downloads missing chapters.
        """
        try:
            # Check for resumable progress
            resume_info = None
            if user_id and CACHE_AVAILABLE:
                cache = get_cache()
                resume_info = cache.get_download_progress(user_id, url)
                if resume_info and resume_info.get('status') in ['in_progress', 'failed', 'partial']:
                    completed_chapters = resume_info.get('completed_chapters', [])
                    if completed_chapters:
                        logger.info(f"[Resume] Found {len(completed_chapters)} previously downloaded chapters for user {user_id}")
            
            # 1. Fetch metadata first to get title and confirm site support
            metadata_result = self.get_novel_metadata(url)
            if 'error' in metadata_result and not metadata_result.get('title'):
                return {'error': metadata_result['error']}

            title = metadata_result.get('title', 'Unknown Novel')
            metadata = metadata_result.get('metadata', {})
            total_available = metadata_result.get('total_chapters', 0)


            # 2. Collect chapter links
            if self.link_progress_callback:
                self.link_progress_callback(0, 0, 'collecting')

            chapter_links = []
            
            # Ranobes has its own specialized chapter link extraction - use it directly
            if 'ranobes' in url:
                chapter_links = self._get_ranobes_chapter_links(url)
            else:
                # --- Protected site logic for other sites ---
                protected_sites = ['novelbin', 'lightnovelworld', 'qidian', 'webnovel']
                site = next((s for s in protected_sites if s in url), None)
                if site:
                    import time, random
                    # Lower parallelism for protected sites
                    self.parallel_workers = min(self.parallel_workers, 2)
                    logger.info(f"[SCRAPER] Using reduced parallelism ({self.parallel_workers}) for protected site: {site}")
                    # Add random delay before any protected site chapter list request
                    delay = random.uniform(1.5, 3.5)
                    logger.info(f"[SCRAPER] Sleeping for {delay:.2f}s before fetching chapter list for {site}")
                    time.sleep(delay)
                    # Try normal requests first
                    resp = self._get_with_retry(url, rotate_on_block=True)
                    if resp:
                        logger.info(f"[SCRAPER] {site}: Got chapter list page with requests. User-Agent: {self.session.headers.get('User-Agent')}, Cookies: {self.session.cookies.get_dict()}")
                        soup = BeautifulSoup(resp.content, 'html.parser')
                        chapter_links = self._get_chapter_links_from_html(soup, url)
                    # If blocked or no links, try Playwright
                    if (not chapter_links or len(chapter_links) < 5) and self.use_playwright:
                        logger.info(f"[SCRAPER] {site}: Falling back to Playwright for chapter list page.")
                        try:
                            from playwright_scraper import get_scraper_instance, run_in_pw_loop
                            pw_scraper = get_scraper_instance()
                            logger.info(f"[Playwright] Launching browser for {url}")
                            html, _ = run_in_pw_loop(pw_scraper.get_page_content(url))
                            logger.info(f"[Playwright] Got HTML for {url}")
                            soup = BeautifulSoup(html, 'html.parser')
                            chapter_links = self._get_chapter_links_from_html(soup, url)
                        except Exception as e:
                            logger.warning(f"[Playwright] Failed to fetch chapter list for {url}: {e}")
                    # If still not enough links, enumerate paginated chapter lists with Playwright
                    if (not chapter_links or len(chapter_links) < 5) and self.use_playwright:
                        logger.info(f"[SCRAPER] {site}: Attempting paginated chapter list enumeration with Playwright.")
                        try:
                            from playwright_scraper import get_scraper_instance, run_in_pw_loop
                            pw_scraper = get_scraper_instance()
                            paginated_links = []
                            page_num = 1
                            while True:
                                page_url = url.rstrip('/') + f'/page/{page_num}/'
                                delay = random.uniform(1.5, 3.5)
                                logger.info(f"[Playwright] Sleeping for {delay:.2f}s before fetching {page_url}")
                                time.sleep(delay)
                                html, _ = run_in_pw_loop(pw_scraper.get_page_content(page_url))
                                if not html or 'Cloudflare' in html or 'checking your browser' in html:
                                    logger.info(f"[Playwright] Cloudflare or empty page at {page_url}, stopping pagination.")
                                    break
                                soup = BeautifulSoup(html, 'html.parser')
                                links = self._get_chapter_links_from_html(soup, url)
                                if not links:
                                    logger.info(f"[Playwright] No chapter links found at {page_url}, stopping pagination.")
                                    break
                                paginated_links.extend(links)
                                logger.info(f"[Playwright] Found {len(links)} chapter links on page {page_num}.")
                                page_num += 1
                            if paginated_links:
                                chapter_links = paginated_links
                        except Exception as e:
                            logger.warning(f"[Playwright] Pagination failed for {url}: {e}")
                else:
                    # Non-protected sites - use simple requests
                    resp = self._get_with_retry(url, rotate_on_block=True)
                    if resp:
                        soup = BeautifulSoup(resp.content, 'html.parser')
                        chapter_links = self._get_chapter_links_from_html(soup, url)
                    elif self.use_playwright and 'novelbin' in url:
                        html = self._browser_fetch_html(url)
                        if html:
                            soup = BeautifulSoup(html, 'html.parser')
                            chapter_links = self._get_chapter_links_from_html(soup, url)

            # Log what we collected so far
            logger.info(
                f"Collected {len(chapter_links)} chapter links for {url}")
            if chapter_links:
                logger.info(
                    f"First few chapter links: {chapter_links[:5]}")

            if not chapter_links:
                return {
                    'title': title,
                    'chapters': [],
                    'error': 'No chapter links found'
                }

            # 3. Filter chapters by range
            # Sort by chapter number if possible
            try:
                chapter_links = sorted(
                    chapter_links,
                    key=lambda x: self._extract_chapter_number(x) or 0)
            except Exception as e:
                logger.warning(f"Sorting chapter links failed: {e}")

            include_prologue = 'ranobes' in url and chapter_start == 1

            # Apply range
            start_idx = max(0, chapter_start - 1)
            end_idx = chapter_end if chapter_end else len(chapter_links)
            # When starting from chapter 1 on Ranobes, pull one extra to include prologue
            if include_prologue and chapter_end:
                end_idx = min(len(chapter_links), end_idx + 1)

            target_links = chapter_links[start_idx:end_idx]

            if not target_links:
                return {
                    'title':
                    title,
                    'chapters': [],
                    'error':
                    f'No chapters found in range {chapter_start}-{chapter_end}'
                }

            logger.info(
                f"Downloading {len(target_links)} chapters for {title}")

            total_to_download = len(target_links)
            if self.progress_callback:
                self.progress_callback(0, total_to_download)

            # 4. Download chapters in parallel (with resume support)
            chapters = []
            failed_urls = []
            completed_urls = []
            
            # If resuming, get already-cached chapters first
            if resume_info and CACHE_AVAILABLE:
                cache = get_cache()
                for i, link_url in enumerate(target_links):
                    cached = cache.get_chapter(link_url, url)
                    if cached:
                        chapters.append(cached)
                        completed_urls.append(link_url)
                if completed_urls:
                    logger.info(f"[Resume] Loaded {len(completed_urls)} chapters from cache")
                    # Remove already-cached from target_links
                    target_links = [u for u in target_links if u not in completed_urls]
                    total_to_download = len(target_links) + len(completed_urls)
            
            # Save initial progress
            if user_id and CACHE_AVAILABLE:
                cache = get_cache()
                cache.save_download_progress(user_id, url, {
                    'title': title,
                    'chapter_start': chapter_start,
                    'chapter_end': chapter_end or len(chapter_links),
                    'completed_chapters': completed_urls,
                    'failed_chapters': [],
                    'status': 'in_progress'
                })
            
            # Reduce parallelism for Ranobes to avoid Cloudflare rate limits
            chapter_workers = self.parallel_workers
            if 'ranobes' in url:
                chapter_workers = 1  # Sequential for Ranobes
                logger.info(f"[Ranobes] Using sequential chapter downloads to avoid rate limits")
            
            with ThreadPoolExecutor(
                    max_workers=chapter_workers) as executor:
                future_to_url = {
                    executor.submit(self._download_chapter, link_url, i + chapter_start + len(completed_urls), url):
                    link_url
                    for i, link_url in enumerate(target_links)
                }

                completed = len(completed_urls)
                for future in as_completed(future_to_url):
                    if self.cancel_check and self.cancel_check():
                        executor.shutdown(wait=False, cancel_futures=True)
                        # Save partial progress for resume
                        if user_id and CACHE_AVAILABLE:
                            cache.save_download_progress(user_id, url, {
                                'title': title,
                                'chapter_start': chapter_start,
                                'chapter_end': chapter_end or len(chapter_links),
                                'completed_chapters': completed_urls,
                                'failed_chapters': failed_urls,
                                'status': 'partial'
                            })
                        break

                    chapter_url = future_to_url[future]
                    try:
                        chapter = future.result()
                        if chapter:
                            chapters.append(chapter)
                            completed_urls.append(chapter_url)
                        else:
                            failed_urls.append(chapter_url)
                    except Exception as e:
                        logger.error(f"Chapter download failed: {e}")
                        failed_urls.append(chapter_url)

                    completed += 1
                    if self.progress_callback:
                        self.progress_callback(completed, total_to_download)
                    
                    # Periodically save progress (every 10 chapters)
                    if user_id and CACHE_AVAILABLE and completed % 10 == 0:
                        cache.save_download_progress(user_id, url, {
                            'title': title,
                            'chapter_start': chapter_start,
                            'chapter_end': chapter_end or len(chapter_links),
                            'completed_chapters': completed_urls,
                            'failed_chapters': failed_urls,
                            'status': 'in_progress'
                        })

            # Sort final chapters by number
            chapters.sort(key=lambda x: x.get('chapter_num', 0))

            # Normalize ordering/titles and ensure prologue inclusion for Ranobes
            chapters = self._normalize_chapters(
                chapters,
                include_prologue=include_prologue,
                chapter_start=chapter_start,
                chapter_end=chapter_end,
                url=url,
            )
            
            # Clear progress on success (or mark as complete with failures)
            if user_id and CACHE_AVAILABLE:
                if failed_urls:
                    cache.save_download_progress(user_id, url, {
                        'title': title,
                        'chapter_start': chapter_start,
                        'chapter_end': chapter_end or len(chapter_links),
                        'completed_chapters': completed_urls,
                        'failed_chapters': failed_urls,
                        'status': 'partial'
                    })
                else:
                    cache.clear_download_progress(user_id, url)

            return {
                'title': title,
                'metadata': metadata,
                'chapters': chapters,
                'novel_url': url,
                'total_available': total_available,
                'failed_chapters': len(failed_urls) if failed_urls else 0
            }
        except Exception as e:
            logger.error(f"Scrape error: {e}", exc_info=True)
            # Save error progress
            if user_id and CACHE_AVAILABLE:
                try:
                    cache = get_cache()
                    cache.save_download_progress(user_id, url, {
                        'title': title if 'title' in dir() else 'Unknown',
                        'chapter_start': chapter_start,
                        'chapter_end': chapter_end,
                        'completed_chapters': completed_urls if 'completed_urls' in dir() else [],
                        'failed_chapters': [],
                        'status': 'failed',
                        'error': str(e)
                    })
                except:
                    pass
            return {'title': 'Unknown', 'chapters': [], 'error': str(e)}

    def _normalize_chapters(self, chapters: List[Dict], include_prologue: bool, chapter_start: int, chapter_end: Optional[int], url: str) -> List[Dict]:
        """Clean titles and enforce prologue + range semantics (Ranobes-focused)."""
        if not chapters:
            return chapters

        is_ranobes = 'ranobes' in url

        # Extract prologue if requested
        prologue_chapter = None
        remaining = list(chapters)
        if include_prologue and remaining:
            for ch in remaining:
                title_raw = (ch.get('title') or '').lower()
                if title_raw.startswith('prologue'):
                    prologue_chapter = ch
                    remaining = [c for c in remaining if c is not ch]
                    break
            if prologue_chapter is None:
                prologue_chapter = remaining[0]
                remaining = remaining[1:]

        # Trim to requested count (main chapters only)
        if chapter_end:
            desired_main = chapter_end - chapter_start + 1
            remaining = remaining[:desired_main]

        def clean_title(raw: str) -> str:
            if not raw:
                return ''
            # Drop novel suffix after '|'
            return raw.split('|', 1)[0].strip()

        normalized = []
        main_start_num = 1 if include_prologue else chapter_start
        for idx, ch in enumerate(remaining, start=main_start_num):
            title_raw = ch.get('title') or ''
            final_title = title_raw
            if is_ranobes:
                base = clean_title(title_raw)
                if base.lower().startswith('chapter'):
                    final_title = base
                elif base:
                    final_title = f"Chapter {idx} | {base}"
                else:
                    final_title = f"Chapter {idx}"
            elif not title_raw:
                final_title = f"Chapter {idx}"

            ch_copy = dict(ch)
            ch_copy['title'] = final_title
            ch_copy['chapter_num'] = idx
            normalized.append(ch_copy)

        output = []
        if prologue_chapter:
            prologue_copy = dict(prologue_chapter)
            prologue_copy['title'] = 'Prologue'
            prologue_copy['chapter_num'] = 0
            output.append(prologue_copy)

        output.extend(normalized)
        return output

    def _get_ranobes_chapter_links(self, url: str) -> List[str]:
        """Extract all chapter links for Ranobes using mathematical page calculation"""
        try:
            import math

            # 1. Setup ID and Base URL
            match = re.search(r'/novels/(\d+)', url)
            if not match: match = re.search(r'/chapters/(\d+)', url)
            if not match: return []

            novel_id = match.group(1)
            slug_match = re.search(r'/novels/\d+-([^./]+)', url)
            slug = slug_match.group(1).lower() if slug_match else None
            base_chapters_url = f"https://ranobes.net/chapters/{novel_id}/"

            # 2. Fetch Page 1 to get the Data - try FlareSolverr first
            logger.info(f"Fetching Page 1 data from {base_chapters_url}")
            html_content = None
            
            # Try FlareSolverr first
            if self.flaresolverr_enabled:
                html_content = self._fetch_with_flaresolverr(base_chapters_url)
                if html_content:
                    logger.info("[Ranobes] FlareSolverr succeeded for page 1")
            
            # Fallback to regular requests
            if not html_content:
                resp = self._get_with_retry(
                    base_chapters_url,
                    rotate_on_block=True,
                    rotate_per_attempt=True,
                )
                if resp:
                    html_content = resp.text
            
            soup = BeautifulSoup(html_content, 'html.parser') if html_content else None

            # 3. Extract JSON Data for precise calculation
            total_chapters = 0
            page_size = 0
            all_links = []
            if soup:
                dle_content = soup.find("div", id="dle-content")
                if dle_content:
                    script_tag = dle_content.find("script")
                    if script_tag and script_tag.string:
                        import json
                        # Use a more robust JSON extraction - find the start and balance braces
                        script_text = script_tag.string
                        json_start = script_text.find('window.__DATA__')
                        if json_start != -1:
                            # Find the opening brace
                            brace_start = script_text.find('{', json_start)
                            if brace_start != -1:
                                # Balance braces to find the complete JSON object
                                brace_count = 0
                                json_end = brace_start
                                for i, char in enumerate(script_text[brace_start:]):
                                    if char == '{':
                                        brace_count += 1
                                    elif char == '}':
                                        brace_count -= 1
                                        if brace_count == 0:
                                            json_end = brace_start + i + 1
                                            break
                                json_str = script_text[brace_start:json_end]
                                try:
                                    data = json.loads(json_str)
                                    total_chapters = int(data.get('count_all', 0))
                                    current_page_chapters = data.get('chapters', [])
                                    page_size = len(current_page_chapters)
                                    logger.info(f"[Ranobes] JSON parsed: {total_chapters} total chapters, {page_size} on page 1")
                                except json.JSONDecodeError as e:
                                    logger.warning(f"JSON parse error: {e}")

                # 4. Get links from Page 1
                all_links = self._extract_ranobes_links_from_soup(soup)
                # If no links found via JSON method, try HTML fallback
                if not all_links:
                    logger.info("No links from JSON method, trying HTML fallback...")
                    all_links = self._extract_ranobes_links_html_fallback(soup, novel_id, slug)
                # Try parsing the inline latest-chapters block (e.g., the novel page's "Last 25 chapters")
                if not all_links:
                    latest = self._extract_ranobes_latest_block(soup, base_chapters_url)
                    if latest:
                        logger.info(f"Ranobes latest-chapters block yielded {len(latest)} links")
                        all_links.extend(latest)
                # As a final attempt, pull the novel page itself and parse its latest-chapters block
                if not all_links:
                    logger.info("Ranobes: Trying novel page latest-chapters block")
                    novel_resp = self._get_with_retry(
                        url,
                        rotate_on_block=True,
                        rotate_per_attempt=True,
                    )
                    if novel_resp:
                        novel_soup = BeautifulSoup(novel_resp.content, 'html.parser')
                        latest = self._extract_ranobes_latest_block(novel_soup, url)
                        if latest:
                            logger.info(f"Ranobes novel page latest-chapters block yielded {len(latest)} links")
                            all_links.extend(latest)
                        # Also try HTML fallback selectors against the novel page
                        if not all_links:
                            logger.info("Ranobes: Trying novel page HTML fallback selectors")
                            novel_html_links = self._extract_ranobes_links_html_fallback(novel_soup, novel_id, slug)
                            if novel_html_links:
                                logger.info(f"Ranobes novel page HTML fallback yielded {len(novel_html_links)} links")
                                all_links.extend(novel_html_links)

            # If we couldn't read the page size, assume standard 10 or just use what we found
            if page_size == 0: page_size = max(len(all_links), 10)

            # 5. Calculate Total Pages
            if total_chapters > 0:
                max_page = math.ceil(total_chapters / page_size)
                logger.info(
                    f"Math Calculation: {total_chapters} total / {page_size} per page = {max_page} pages"
                )
            else:
                # Fallback to visual check if JSON failed
                max_page = 1
                if soup:
                    pagination = soup.select(
                        '.pages a, .navigation a, .pagination a')
                    for link in pagination:
                        try:
                            num = int(link.get_text(strip=True))
                            if num > max_page: max_page = num
                        except:
                            pass
                    logger.info(f"Visual Calculation: Found {max_page} pages")

            # If we have enough links, return them
            if max_page <= 1 and all_links:
                return all_links

            # 6. Sequential Fetch Pages 2 to Max with delays to avoid rate limiting
            import time, random
            failed_pages = []
            logger.info(f"Fetching {max_page - 1} additional pages sequentially with delays...")
            
            for p in range(2, max_page + 1):
                page_url = f"https://ranobes.net/chapters/{novel_id}/page/{p}/"
                # Add random delay between requests to avoid Cloudflare
                delay = random.uniform(1.0, 2.5)
                time.sleep(delay)
                
                links = self._fetch_ranobes_page_links(page_url)
                if links:
                    all_links.extend(links)
                    logger.info(f"[Ranobes] Page {p}/{max_page}: Got {len(links)} links")
                else:
                    failed_pages.append(page_url)
                    logger.warning(f"[Ranobes] Page {p}/{max_page}: Failed to fetch")
            
            # Try Playwright for failed pages
            if failed_pages and self.use_playwright:
                logger.info(f"[Ranobes] Retrying {len(failed_pages)} failed pages with Playwright...")
                try:
                    from playwright_scraper import get_scraper_instance, run_in_pw_loop
                    pw_scraper = get_scraper_instance()
                    for page_url in failed_pages:
                        delay = random.uniform(2.0, 4.0)
                        time.sleep(delay)
                        try:
                            html, _ = run_in_pw_loop(pw_scraper.get_page_content(page_url))
                            if html and 'Cloudflare' not in html:
                                soup = BeautifulSoup(html, 'html.parser')
                                links = self._extract_ranobes_links_from_soup(soup)
                                if not links:
                                    links = self._extract_ranobes_links_html_fallback(soup, novel_id, slug)
                                if links:
                                    all_links.extend(links)
                                    logger.info(f"[Playwright] Recovered {len(links)} links from {page_url}")
                        except Exception as e:
                            logger.warning(f"[Playwright] Failed to fetch {page_url}: {e}")
                except Exception as e:
                    logger.warning(f"[Playwright] Fallback initialization failed: {e}")

            logger.info(f"Total chapters collected: {len(all_links)}")

            # 7. Deduplicate and filter
            deduped = list(dict.fromkeys(all_links))
            filtered = []
            base_url = url.rstrip('/')
            for link in deduped:
                if not link:
                    continue
                norm = link.rstrip('/')
                if norm == base_url:
                    continue
                if norm.endswith('.html'):
                    filtered.append(link)
                    continue
                if novel_id in norm:
                    filtered.append(link)
            if filtered:
                return filtered

            # --- Playwright fallback if all else fails ---
            if self.use_playwright:
                logger.info("[Ranobes] All requests-based methods failed, trying Playwright fallback for chapter links.")
                try:
                    from playwright_scraper import get_scraper_instance, run_in_pw_loop
                    import time, random
                    pw_scraper = get_scraper_instance()
                    html, _ = run_in_pw_loop(pw_scraper.get_page_content(base_chapters_url))
                    if html:
                        soup = BeautifulSoup(html, 'html.parser')
                        links = self._extract_ranobes_links_from_soup(soup)
                        if not links:
                            links = self._extract_ranobes_links_html_fallback(soup, novel_id, slug)
                        if links:
                            logger.info(f"[Ranobes] Playwright fallback found {len(links)} chapter links on main chapters page.")
                            return links
                        # Try paginated Playwright fetches
                        paginated_links = []
                        for page_num in range(2, max_page + 1):
                            page_url = f"https://ranobes.net/chapters/{novel_id}/page/{page_num}/"
                            delay = random.uniform(1.5, 3.5)
                            logger.info(f"[Playwright] Sleeping for {delay:.2f}s before fetching {page_url}")
                            time.sleep(delay)
                            html, _ = run_in_pw_loop(pw_scraper.get_page_content(page_url))
                            if not html or 'Cloudflare' in html or 'checking your browser' in html:
                                logger.info(f"[Playwright] Cloudflare or empty page at {page_url}, stopping pagination.")
                                break
                            soup = BeautifulSoup(html, 'html.parser')
                            links = self._extract_ranobes_links_from_soup(soup)
                            if not links:
                                logger.info(f"[Playwright] No chapter links found at {page_url}, stopping pagination.")
                                break
                            paginated_links.extend(links)
                            logger.info(f"[Playwright] Found {len(links)} chapter links on page {page_num}.")
                        if paginated_links:
                            logger.info(f"[Ranobes] Playwright fallback found {len(paginated_links)} chapter links from paginated pages.")
                            return paginated_links
                except Exception as e:
                    logger.warning(f"[Ranobes] Playwright fallback failed: {e}")

            logger.error("Ranobes: No chapter links found after all fallback methods; failing scrape")
            return []
        except Exception as e:
            logger.error(f"Error getting Ranobes links: {e}")
            return []

    def _extract_ranobes_links_html_fallback(self, soup, novel_id: str, slug: Optional[str] = None) -> List[str]:
        """Fallback method to extract chapter links directly from HTML"""
        try:
            links = []
            
            # Try multiple selectors for chapter links
            selectors = []
            if slug:
                selectors.extend([
                    f'a[href*="/{slug}-"]',
                    f'a[href*="/{slug}/"]'
                ])
            selectors.extend([
                f'a[href*="-{novel_id}/"]',  # Links containing novel ID
                f'a[href*="{novel_id}-"]',   # ID followed by chapter
                f'a[href*="{novel_id}"][href$=".html"]',
                'a[href*="/chapter"]',  # Generic chapter links
                '.chapter-link a',  # Class-based
                'a[data-chapter]',  # Data attribute
                'a[data-chapter-id]',
                'a[data-number]',
                'a[data-id]',
                'a[data-url]'
            ])
            
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    chapter_links = [a.get('href') for a in elements if a.get('href')]
                    full_links = []
                    for link in chapter_links:
                        # Strip fragment anchors (e.g., #comment-id-XXX)
                        link = link.split('#')[0]
                        if not link:
                            continue
                        if link.startswith('http'):
                            full_links.append(link)
                        elif link.startswith('/'):
                            full_links.append(f"https://ranobes.net{link}")

                    if full_links:
                        logger.info(f"HTML fallback found {len(full_links)} links using: {selector}")
                        links.extend(full_links)
                        # Keep collecting if we found very few (broken layout); otherwise stop
                        if len(links) >= 5:
                            break
            
            if not links:
                # Last resort: search all links for patterns
                all_links = soup.find_all('a', href=True)
                for a in all_links:
                    href = a.get('href', '')
                    # Look for links that contain novel ID or novel name patterns
                    matches_slug = slug and slug in href.lower()
                    chapter_like = re.search(r'/\d+-\d+\.html$', href) or href.endswith('.html')
                    if (novel_id in href or matches_slug or chapter_like):
                        # Strip fragment anchors (e.g., #comment-id-XXX)
                        href = href.split('#')[0]
                        if not href:
                            continue
                        if href.startswith('/'):
                            href = f"https://ranobes.net{href}"
                        elif href.startswith('http'):
                            pass  # Already full URL
                        else:
                            continue
                        links.append(href)
                
                if links:
                    logger.info(f"HTML fallback last resort found {len(links)} potential chapter links")
            
            return list(dict.fromkeys(links))  # Remove duplicates
            
        except Exception as e:
            logger.error(f"HTML fallback extraction error: {e}")
            return []

    def _extract_ranobes_latest_block(self, soup, base_url: str) -> List[str]:
        """Parse the inline "Last chapters" block Ranobes renders on novel pages."""
        try:
            anchors = soup.select(
                'div.r-fullstory-chapters ul.chapters-scroll-list a.chapter-item[rel="chapter"], '
                'div.r-fullstory-chapters ul.chapters-scroll-list a.chapter-item'
            )
            if not anchors:
                anchors = soup.select('a.chapter-item[rel="chapter"], .chapters-scroll-list a[rel="chapter"]')

            links: List[str] = []
            for a in anchors:
                href = a.get('href')
                if not href:
                    continue
                # Strip fragment anchors (e.g., #comment-id-XXX)
                href = href.split('#')[0]
                if not href:
                    continue
                links.append(urljoin(base_url, href))

            # Deduplicate while preserving order
            return list(dict.fromkeys(links))
        except Exception as e:
            logger.debug(f"Ranobes latest block parse error: {e}")
            return []

    def _fetch_ranobes_page_links(self, url: str) -> List[str]:
        """Helper for worker threads to fetch a single page"""
        html_content = None
        
        # Try FlareSolverr first
        if self.flaresolverr_enabled:
            html_content = self._fetch_with_flaresolverr(url)
        
        # Fallback to regular requests
        if not html_content:
            resp = self._get_with_retry(url, rotate_on_block=True, rotate_per_attempt=True)
            if resp:
                html_content = resp.text
        
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        return self._extract_ranobes_links_from_soup(soup)

    def _extract_ranobes_links_from_soup(self, soup) -> List[str]:
        """Extract chapter links from Ranobes page using __DATA__ JSON if present."""
        try:
            dle_content = soup.find("div", id="dle-content")
            if not dle_content:
                # Try alternative selectors
                dle_content = (soup.find("div", id="content") or  # New Ranobes structure
                               soup.find("div", class_="dle-content") or 
                               soup.find("main") or 
                               soup.find("article") or
                               soup.find("div", class_="content"))
                if not dle_content:
                    return []

            def iter_scripts(scope) -> List[str]:
                texts = []
                for s in scope.find_all("script"):
                    # .string misses scripts with whitespace/children; get_text handles both
                    txt = s.string or s.get_text() or ""
                    if txt:
                        texts.append(txt)
                return texts

            # Collect candidate script texts (container first, then full document)
            script_texts = iter_scripts(dle_content)
            if not script_texts:
                script_texts = iter_scripts(soup)

            target_json = None
            for txt in script_texts:
                # Use brace balancing for robust JSON extraction
                json_start = txt.find('window.__DATA__')
                if json_start != -1:
                    brace_start = txt.find('{', json_start)
                    if brace_start != -1:
                        brace_count = 0
                        json_end = brace_start
                        for i, char in enumerate(txt[brace_start:]):
                            if char == '{':
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    json_end = brace_start + i + 1
                                    break
                        target_json = txt[brace_start:json_end]
                        break

            if not target_json:
                return []

            import json
            data = json.loads(target_json)
            chapters_data = data.get('chapters', [])
            
            logger.debug(f"Ranobes JSON data keys: {list(data.keys())}")
            logger.debug(f"Ranobes chapters count in JSON: {len(chapters_data)}")

            links = []
            for chap in chapters_data:
                link = chap.get('link')
                if link:
                    # Strip fragment anchors (e.g., #comment-id-XXX)
                    link = link.split('#')[0]
                    if link:
                        links.append(urljoin("https://ranobes.net", link))
            
            if links:
                logger.debug(f"Extracted {len(links)} links from JSON. Sample: {links[0] if links else 'None'}")
            
            return links
        except Exception as e:
            logger.debug(f"Ranobes JSON extraction error: {e}")
            return []

    def _download_chapter(self, url: str, chapter_num: int, novel_url: str = None) -> Optional[Dict]:
        """Download and parse a single chapter with aggressive cleaning.
        
        Uses cache to avoid re-downloading chapters that haven't changed.
        """
        try:
            # Add random delay between chapter downloads to avoid rate limiting
            # Use longer delay for ranobes which has aggressive rate limiting
            if 'ranobes' in url:
                self._random_delay(1.5, 3.0)  # Longer delay for ranobes
            else:
                self._random_delay(0.5, 1.5)  # Standard delay
            
            # Check cache first - chapter content doesn't change
            if CACHE_AVAILABLE:
                cache = get_cache()
                cached = cache.get_chapter(url, novel_url)
                if cached:
                    # Validate cached content is not rate-limited junk
                    cached_content = cached.get('content', '')
                    if cached_content and len(cached_content) > 200 and 'Rate limited' not in cached_content:
                        logger.info(f"[Chapter] ✓ Cache hit for chapter {chapter_num}")
                        return cached
                    else:
                        logger.info(f"[Chapter] Ignoring bad cached content for chapter {chapter_num}")
            
            logger.info(
                f"[Chapter] Starting download for chapter {chapter_num} from {url}"
            )

            # For some sites (e.g., NovelBin), chapter pages may expect a Referer
            referer = None
            if 'novelbin' in url:
                try:
                    m = re.match(r'(https?://[^/]+/novel-book/[^/]+)', url)
                    if m:
                        referer = m.group(1)
                except Exception:
                    referer = None

            rotate_ranobes = 'ranobes' in url
            rotate_per_attempt = rotate_ranobes
            
            html_content = None
            soup = None
            
            # Try FlareSolverr first for Ranobes
            if 'ranobes' in url and self.flaresolverr_enabled:
                html_content = self._fetch_with_flaresolverr(url)
                if html_content:
                    soup = BeautifulSoup(html_content, 'html.parser')
                    logger.info(f"[Chapter] FlareSolverr succeeded for chapter {chapter_num}")
            
            # Fallback to regular requests
            if not soup:
                resp = self._get_with_retry(
                    url,
                    referer=referer,
                    rotate_on_block=rotate_ranobes,
                    rotate_per_attempt=rotate_per_attempt,
                )
                if resp:
                    soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Try browser-based fallback for protected sites
            if not soup:
                if self.use_playwright and ('novelbin' in url or 'ranobes' in url):
                    logger.info(f"[Chapter] Trying Playwright fallback for chapter {chapter_num}")
                    try:
                        from playwright_scraper import get_scraper_instance, run_in_pw_loop
                        pw_scraper = get_scraper_instance()
                        html, _ = run_in_pw_loop(pw_scraper.get_page_content(url))
                        if html and 'Cloudflare' not in html and 'checking your browser' not in html.lower():
                            soup = BeautifulSoup(html, 'html.parser')
                            logger.info(f"[Playwright] Successfully fetched chapter {chapter_num}")
                        else:
                            logger.warning(f"[Chapter] Playwright also blocked for chapter {chapter_num}")
                    except Exception as e:
                        logger.warning(f"[Playwright] Failed to fetch chapter {chapter_num}: {e}")
            
            if not soup:
                logger.warning(
                    f"[Chapter] Failed to fetch chapter URL after all methods: {url}"
                )
                return None
            
            # Check for anti-bot/rate limit page (common on ranobes)
            page_text = soup.get_text()[:500].lower()
            page_title = soup.title.string.lower() if soup.title and soup.title.string else ''
            if 'abnormal activity' in page_text or 'detected abnormal' in page_text or 'dear visitor' in page_text or 'just a moment' in page_title or 'just a moment' in page_text:
                logger.warning(f"[Chapter] Rate limited at chapter {chapter_num}, waiting 10s...")
                logger.debug(f"[Chapter] Anti-bot text sample: {page_text[:200]}")
                # Dump HTML for debugging
                try:
                    html_text = resp.text if resp and getattr(resp, 'text', None) else soup.prettify()
                except Exception:
                    html_text = soup.prettify()
                self._debug_dump_html(url, html_text, f"rate_limited_chapter_{chapter_num}")
                time.sleep(10)  # Wait before retry
                # Retry once with fresh session (force rotate per attempt for ranobes)
                delay = random.uniform(20, 35) if rotate_ranobes else 10
                time.sleep(delay)
                resp = self._get_with_retry(
                    url,
                    referer=referer,
                    rotate_on_block=rotate_ranobes,
                    rotate_per_attempt=rotate_per_attempt,
                )
                if resp:
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    page_text = soup.get_text()[:500].lower()
                    if 'abnormal activity' in page_text or 'dear visitor' in page_text:
                        logger.error(f"[Chapter] Still rate limited for chapter {chapter_num}")
                        # Dump the retried HTML as well
                        self._debug_dump_html(url, resp.text, f"rate_limited_retry_{chapter_num}")
                        return {'title': f'Chapter {chapter_num}', 'content': '[Rate limited - please try again later]', 'chapter_num': chapter_num}
                else:
                    self._debug_dump_html(url, None, f"rate_limited_no_resp_{chapter_num}")
                    return {'title': f'Chapter {chapter_num}', 'content': '[Rate limited - please try again later]', 'chapter_num': chapter_num}
            
            content = ""

            # --- Ranobes Specific Cleaning ---
            if 'ranobes' in url:
                # Remove unwanted UI elements
                for junk in soup.select(
                    "script, style, .ads, .social-buttons, .navigation, .btn-group, .mechanic-buttons, "
                    "#u_o, #u_b, #click_y, #bookmark, .bookmark, .footer, .bottom-menu, .stats, .report, "
                    "a[href*='chapter-list'], a[href*='next-chapter'], .nav-buttons, .prev-next, .next-chapter, .prev-chapter, img"
                ):
                    junk.decompose()

                # Try all possible containers for chapter text
                # Tested selectors: #arrticle and .story work on current Ranobes
                content_div = (
                    soup.select_one('#arrticle') or  # Main container (not #arrticle-text!)
                    soup.select_one('.story') or  # Alternative story container
                    soup.select_one('#dle-content') or
                    soup.select_one('.text.story-text') or
                    soup.select_one('[id^="post-message"]')
                )

                if content_div:
                    for tag in content_div.select('.ads, .nav-buttons, .bookmark, .report, div[align="center"]'):
                        tag.decompose()
                    
                    # Extract content preserving italics/emphasis
                    # Convert <em>, <i> to markdown-style *text* for EPUB
                    for em_tag in content_div.find_all(['em', 'i']):
                        em_text = em_tag.get_text()
                        em_tag.replace_with(f'*{em_text}*')
                    
                    raw_content = content_div.get_text(separator='\n\n', strip=True)
                    
                    # Remove Ranobes watermarks (various Unicode obfuscated versions)
                    # Pattern matches: ŖᴀNÕḂĒŚ, RANOBES, ᖇᗩᑎOᗷᗴᔕ, etc.
                    watermark_patterns = [
                        r'ŖᴀNÕḂĒŚ',
                        r'RANOBES',
                        r'Ranobes',
                        r'ranobes',
                        r'ᖇᗩᑎOᗷᗴᔕ',
                        r'[ŖR][ᴀAa][NÑ][OÕ][BḂ][EĒ][SŚ]',  # Mixed Unicode variants
                        r'R\s*A\s*N\s*O\s*B\s*E\s*S',  # Spaced out
                    ]
                    for pattern in watermark_patterns:
                        raw_content = re.sub(pattern, '', raw_content, flags=re.IGNORECASE)
                    
                    lines = raw_content.split('\n')
                    cleaned_lines = []
                    for line in lines:
                        l = line.strip()
                        if l.upper() in [
                            'OPTIONS', 'BOOKMARK', 'CHAPTERS LIST', 'NEXT >>', 'PREVIOUS',
                            'REPORT', 'BACK', '<< BACK', 'NEXT', 'NEXT >>'
                        ]:
                            continue
                        if re.match(r'^\d+\s*Report$', l, re.I):
                            continue
                        if l:
                            cleaned_lines.append(l)
                    content = '\n\n'.join(cleaned_lines)
                else:
                    # Fallback: extract all visible <p> tags from the main content area (include all non-empty lines)
                    paragraphs = soup.find_all('p')
                    cleaned_paragraphs = []
                    for p in paragraphs:
                        text = p.get_text(strip=True)
                        if text:  # include all non-empty lines, even short ones
                            cleaned_paragraphs.append(text)
                    if cleaned_paragraphs:
                        content = '\n\n'.join(cleaned_paragraphs)
                        logger.warning(f"Used fallback <p> extraction for {url}")
                    else:
                        logger.warning(f"Could not find content container or fallback for {url}")
                        content = "Error: Could not extract chapter text."
            
            # --- NovelFire Specific ---
            elif 'novelfire' in url:
                content_div = soup.select_one('#content, article, .chapter-content')
                if content_div:
                    for junk in content_div.select('script, style, .ads, .navigation, .chapter-nav, .prev-next'):
                        junk.decompose()
                    content = content_div.get_text(separator='\n\n', strip=True)
            
            # --- NovelBuddy Specific ---
            elif 'novelbuddy' in url:
                content_div = soup.select_one(
                    '.chapter__content, .content-inner, .viewer-content, '
                    '#chapter-content, .reading-content, .chapter-content'
                )
                if content_div:
                    for junk in content_div.select(
                        'script, style, .ads, .navigation, .chapter-nav, '
                        '.chapter-title, .chapter__title, .player, .audio, audio, source, '
                        '.tts, .text-to-speech, .voice, .video, .mejs-container, .chapter-player'
                    ):
                        junk.decompose()

                    raw_text = content_div.get_text(separator='\n\n', strip=True)
                    cleaned_lines = []
                    for line in raw_text.splitlines():
                        stripped = line.strip()
                        if not stripped:
                            continue
                        lower = stripped.lower()
                        if 'audio player' in lower or 'microsoft david' in lower or 'press reset' in lower:
                            continue  # drop TTS/player helper text
                        cleaned_lines.append(stripped)
                    content = '\n\n'.join(cleaned_lines)
            
            # --- LNMTL Specific ---
            elif 'lnmtl' in url:
                content_div = soup.select_one('.chapter-body, .translated, .text-content')
                if content_div:
                    for junk in content_div.select('script, style, .ads'):
                        junk.decompose()
                    content = content_div.get_text(separator='\n\n', strip=True)
            
            # --- FreeWebNovel Specific ---
            elif 'freewebnovel' in url:
                content_div = soup.select_one('.txt, .chapter-content, #chapter-content')
                if content_div:
                    for junk in content_div.select('script, style, .ads'):
                        junk.decompose()
                    content = content_div.get_text(separator='\n\n', strip=True)
            
            else:
                # --- Generic Site Handling ---
                # Try a rich set of common selectors; some sites (like NovelBin)
                # use specific containers such as .chapter-content or .reading-content
                content_div = soup.select_one(
                    '#chapter-content, #chr-content, .chapter-content, .reading-content, '
                    '.chr-c, .chapter__content, .content-inner, article')
                if content_div:
                    for junk in content_div.select('script, style, .ads, .navigation, img'):
                        junk.decompose()
                    content = content_div.get_text(separator='\n\n', strip=True)

            if not content:
                logger.warning(
                    f"[Chapter] Extracted EMPTY content for chapter {chapter_num} from {url}"
                )
                # Dump HTML to investigate selector issues or anti-bot pages
                try:
                    html_text = resp.text if resp and getattr(resp, 'text', None) else soup.prettify()
                except Exception:
                    html_text = soup.prettify()
                self._debug_dump_html(url, html_text, f"empty_content_chapter_{chapter_num}")
            else:
                logger.info(
                    f"[Chapter] Extracted content length for chapter {chapter_num} from {url}: {len(content)} chars"
                )

            # Extract and clean title
            title_tag = soup.find('h1')
            title_raw = title_tag.get_text(strip=True) if title_tag else f"Chapter {chapter_num}"

            # Remove novel name prefix if it matches the novel slug
            novel_slug = None
            novel_base_path = None
            if novel_url:
                slug_match = re.search(r'/novel/([^/?#]+)', novel_url)
                if slug_match:
                    novel_slug = slug_match.group(1).replace('-', ' ').lower()
                    # Base path for slug-based sites like NovelBuddy
                    parsed_novel = urlparse(novel_url)
                    novel_base_path = f"/novel/{slug_match.group(1).lower()}"

            title_match = re.search(r'(Chapter\s*\d+[^\n]*)', title_raw, re.IGNORECASE)
            title_candidate = title_match.group(1).strip() if title_match else title_raw.strip()
            if novel_slug and title_candidate.lower().startswith(novel_slug):
                # Drop leading novel name
                trimmed = title_candidate[len(novel_slug):].lstrip(' -:\u2013')
                title_candidate = trimmed if trimmed else title_candidate
            title = title_candidate

            # Detect chapter number mismatches to avoid caching wrong chapters
            expected_num = chapter_num
            title_num = self._extract_chapter_number(title) if title else None
            url_num = self._extract_chapter_number(url)
            number_mismatch = False
            if expected_num and title_num and title_num != expected_num:
                number_mismatch = True
            if expected_num and url_num and url_num != expected_num:
                number_mismatch = True

            # Slug/base-path mismatch guard for slugged sites (NovelBuddy)
            slug_path_mismatch = False
            if novel_base_path and 'novelbuddy' in url:
                path_full = urlparse(url).path.lower()
                if not path_full.startswith(novel_base_path):
                    slug_path_mismatch = True
            # Normalize content spacing and drop duplicate heading lines
            if content:
                content = content.replace('\xa0', ' ')
                lines = content.splitlines()
                normalized = []
                for ln in lines:
                    stripped = ln.strip()
                    if stripped and title and stripped.lower() == title.lower():
                        # Drop duplicated inline title lines
                        continue
                    if novel_slug and stripped.lower().startswith(novel_slug) and 'chapter' in stripped.lower():
                        # Drop lines prefixed with novel name + chapter title
                        continue
                    normalized.append(stripped)

                # Collapse multiple blank lines and remove leading indentation
                cleaned_lines = []
                for ln in normalized:
                    if not ln:
                        if cleaned_lines and cleaned_lines[-1] == '':
                            continue
                        cleaned_lines.append('')
                    else:
                        cleaned_lines.append(ln.lstrip())

                content = '\n'.join(cleaned_lines)
                content = re.sub(r'\n{3,}', '\n\n', content)

            # If the numbers or slug path don't match, treat as suspect and do not cache (slugged sites)
            if (number_mismatch or slug_path_mismatch) and 'novelbuddy' in url:
                logger.warning(
                    f"[Chapter] NovelBuddy mismatch: expected={expected_num}, title_num={title_num}, url_num={url_num}, slug_path_mismatch={slug_path_mismatch}; skipping cache and marking as failed"
                )
                return None

            chapter_data = {
                'title': title,
                'content': content,
                'chapter_num': chapter_num,
                'url': url
            }
            
            # Cache the chapter content ONLY if it's valid (not rate-limited or too short)
            # Short content (<200 chars) is likely anti-bot message
            if CACHE_AVAILABLE and content and len(content) > 200:
                if 'Rate limited' not in content and 'abnormal activity' not in content.lower():
                    cache = get_cache()
                    cache.set_chapter(url, chapter_data, novel_url)
                    logger.debug(f"[Chapter] Cached chapter {chapter_num} ({len(content)} chars)")
                else:
                    logger.warning(f"[Chapter] Not caching rate-limited content for chapter {chapter_num}")
            elif content and len(content) <= 200:
                logger.warning(f"[Chapter] Not caching short content ({len(content)} chars) for chapter {chapter_num} - likely anti-bot")
                # Dump short content HTML for diagnostics
                try:
                    html_text = resp.text if resp and getattr(resp, 'text', None) else soup.prettify()
                except Exception:
                    html_text = soup.prettify()
                self._debug_dump_html(url, html_text, f"short_content_{chapter_num}")
            
            return chapter_data
        except Exception as e:
            logger.error(f"Error downloading chapter {url}: {e}")
            return None

    def _fetch_with_flaresolverr(self, url: str, max_timeout: int = 60000) -> Optional[str]:
        """Fetch URL using FlareSolverr to bypass Cloudflare protection.
        
        Returns the HTML content as a string, or None if failed.
        """
        if not self.flaresolverr_enabled:
            return None
        
        try:
            payload = {
                "cmd": "request.get",
                "url": url,
                "maxTimeout": max_timeout
            }
            
            logger.info(f"[FlareSolverr] Requesting: {url}")
            resp = requests.post(
                self.flaresolverr_url,
                json=payload,
                timeout=max_timeout // 1000 + 10  # Add buffer to timeout
            )
            
            if resp.status_code != 200:
                logger.warning(f"[FlareSolverr] HTTP error: {resp.status_code}")
                return None
            
            data = resp.json()
            
            if data.get("status") != "ok":
                logger.warning(f"[FlareSolverr] Request failed: {data.get('message', 'Unknown error')}")
                return None
            
            solution = data.get("solution", {})
            html = solution.get("response", "")
            status_code = solution.get("status", 0)
            
            if status_code >= 400:
                logger.warning(f"[FlareSolverr] Got status {status_code} for {url}")
                return None
            
            if not html or len(html) < 500:
                logger.warning(f"[FlareSolverr] Empty or too short response for {url}")
                return None
            
            # Check for Cloudflare challenge still present
            if 'Just a moment' in html or 'cf-browser-verification' in html:
                logger.warning(f"[FlareSolverr] Cloudflare challenge still present")
                return None
            
            logger.info(f"[FlareSolverr] Success for {url} ({len(html)} bytes)")
            return html
            
        except requests.exceptions.Timeout:
            logger.warning(f"[FlareSolverr] Timeout for {url}")
            return None
        except requests.exceptions.ConnectionError:
            logger.warning(f"[FlareSolverr] Connection error - is FlareSolverr running?")
            return None
        except Exception as e:
            logger.error(f"[FlareSolverr] Error: {e}")
            return None

    def _get_with_retry(self, url: str, referer: Optional[str] = None, max_retries: int = None, rotate_per_attempt: bool = False, rotate_on_block: bool = False) -> Optional[requests.Response]:
        """Fetch URL with retries, rotating headers, and Cloudflare bypass attempts.

        Strategy:
        1. First try with normal headers
        2. If 403/503, try with Cloudflare bypass headers
        3. If still blocked, try alternative domain mirrors (if available)
        4. Add small delays between retries to avoid rate limiting
        
        Logs detailed info to help debug site-specific blocking.
        """
        from urllib.parse import urlparse
        
        retries = max_retries if max_retries else self.max_retries
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Use fresh session with IP rotation for heavy-security sites
        heavy_security = any(site in domain for site in ['ranobes', 'novelbin', 'lightnovelworld'])
        base_session = self._get_fresh_session(rotate_ip=True) if heavy_security else self.session
        if heavy_security:
            logger.debug(f"[FETCH] Using fresh session + IP rotation for {domain}")
        
        # Identify which site we're hitting
        site_key = None
        for key in SITE_MIRRORS.keys():
            if key in domain:
                site_key = key
                break
        
        logger.debug(f"[FETCH] Starting fetch for {url} (site: {site_key or 'unknown'})")
        
        last_blocked = False

        for attempt in range(retries):
            try:
                # Strategy 1: Normal headers (attempt 0)
                # Strategy 2: Cloudflare bypass headers (attempt 1+)
                if attempt == 0:
                    headers = self._get_random_headers(for_site=domain)
                    logger.debug(f"[FETCH] Attempt {attempt + 1}: Using normal headers")
                else:
                    headers = self._get_cloudflare_bypass_headers(url)
                    # Also create a fresh session for Cloudflare bypass
                    logger.debug(f"[FETCH] Attempt {attempt + 1}: Using Cloudflare bypass headers")
                
                if referer:
                    headers['Referer'] = referer
                
                session = base_session
                if rotate_per_attempt and heavy_security:
                    logger.info(f"[FETCH] Rotating session/IP for attempt {attempt + 1} on {domain}")
                    session = self._get_fresh_session(rotate_ip=True)
                elif rotate_on_block and heavy_security and last_blocked:
                    logger.info(f"[FETCH] Rotating session/IP after block on {domain} (attempt {attempt + 1})")
                    session = self._get_fresh_session(rotate_ip=True)

                resp = session.get(url, headers=headers, timeout=15, allow_redirects=True)
                self._track_bandwidth(resp)
                
                # Log response details for debugging
                logger.debug(f"[FETCH] Response: status={resp.status_code}, url={resp.url}, cookies={len(resp.cookies)}")
                
                blocked = False
                if resp.status_code == 200:
                    # Check for Cloudflare challenge or bot page
                    content_sample = resp.text[:800].lower() if resp.text else ''
                    if 'just a moment' in content_sample or 'checking your browser' in content_sample or ('cloudflare' in content_sample and 'challenge' in content_sample):
                        logger.warning(f"[CLOUDFLARE] Detected Cloudflare challenge on {url}")
                        blocked = True
                    elif 'dear visitor' in content_sample or 'abnormal activity' in content_sample:
                        logger.warning(f"[FETCH] Bot block page detected on {url}")
                        blocked = True
                    else:
                        logger.info(f"[FETCH] ✓ Success: {url}")
                        return resp
                elif resp.status_code == 403:
                    logger.warning(f"[FETCH] Attempt {attempt + 1}: 403 Forbidden for {url}")
                    blocked = True
                elif resp.status_code == 503:
                    logger.warning(f"[FETCH] Attempt {attempt + 1}: 503 Service Unavailable (likely Cloudflare) for {url}")
                    blocked = True
                elif resp.status_code == 429:
                    logger.warning(f"[FETCH] Attempt {attempt + 1}: 429 Rate Limited for {url}")
                    blocked = True
                    time.sleep(3)  # Longer delay for rate limiting
                else:
                    logger.warning(f"[FETCH] Attempt {attempt + 1}: HTTP {resp.status_code} for {url}")
                    blocked = False

                last_blocked = blocked

                # Small delay before retry
                time.sleep(1 + attempt * 0.5)
                
            except requests.exceptions.Timeout:
                logger.warning(f"[FETCH] Attempt {attempt + 1}: Timeout for {url}")
                last_blocked = True
                time.sleep(2)
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"[FETCH] Attempt {attempt + 1}: Connection error for {url}: {e}")
                last_blocked = True
                time.sleep(2)
            except Exception as e:
                logger.warning(f"[FETCH] Attempt {attempt + 1}: Error for {url}: {type(e).__name__}: {e}")
                last_blocked = True
                time.sleep(1)
        
        # All retries failed - try alternative domain mirrors if available
        if site_key and site_key in SITE_MIRRORS:
            logger.info(f"[FETCH] Trying alternative mirrors for {site_key}...")
            for mirror_domain in SITE_MIRRORS[site_key]:
                if mirror_domain in url:
                    continue  # Skip the domain we already tried
                
                # Replace domain in URL
                alt_url = url.replace(domain, mirror_domain)
                logger.info(f"[FETCH] Trying mirror: {alt_url}")
                
                try:
                    headers = self._get_cloudflare_bypass_headers(alt_url)
                    mirror_session = self._get_fresh_session(rotate_ip=True) if (rotate_per_attempt and heavy_security) else self.session
                    resp = mirror_session.get(alt_url, headers=headers, timeout=15, allow_redirects=True)
                    self._track_bandwidth(resp)
                    
                    if resp.status_code == 200:
                        content_sample = resp.text[:500].lower() if resp.text else ''
                        if 'checking your browser' not in content_sample:
                            logger.info(f"[FETCH] ✓ Mirror success: {alt_url}")
                            return resp
                    else:
                        logger.debug(f"[FETCH] Mirror {mirror_domain} returned {resp.status_code}")
                except Exception as e:
                    logger.debug(f"[FETCH] Mirror {mirror_domain} failed: {e}")
                
                time.sleep(0.5)
        
        logger.error(f"[FETCH] ✗ All attempts failed for {url}")
        return None

    def _browser_fetch_html(self, url: str) -> Optional[str]:
        """Fetch page HTML using Playwright or Selenium as a last resort.

        This is only used for hard-blocked sites like NovelBin when
        ``use_playwright`` is True and the necessary dependencies are
        installed in the environment.
        
        Uses playwright-stealth to avoid detection by anti-bot systems.
        """
        # --- Try Playwright with stealth first ---
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
            
            # Try to import stealth
            try:
                from playwright_stealth import stealth_sync  # type: ignore
                use_stealth = True
            except ImportError:
                use_stealth = False
                logger.debug("playwright-stealth not available")

            try:
                with sync_playwright() as p:
                    # Use Chromium with stealth (better anti-detection)
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                        viewport={'width': 1920, 'height': 1080},
                        locale='en-US',
                    )
                    page = context.new_page()
                    
                    # Apply stealth if available
                    if use_stealth:
                        stealth_sync(page)
                    
                    page.goto(url, wait_until='networkidle', timeout=30000)
                    
                    # Wait for browser check to complete if present
                    title = page.title()
                    if 'just a moment' in title.lower() or 'checking' in title.lower():
                        logger.debug(f"Browser check detected, waiting for redirect...")
                        # Wait up to 15 seconds for the page to change
                        try:
                            page.wait_for_function(
                                "() => !document.title.toLowerCase().includes('moment') && !document.title.toLowerCase().includes('checking')",
                                timeout=15000
                            )
                            page.wait_for_load_state('networkidle', timeout=10000)
                        except Exception:
                            logger.debug("Timeout waiting for browser check to complete")
                    
                    html = page.content()
                    browser.close()
                    logger.info(f"Playwright fetched HTML for {url}")
                    return html
            except Exception as e:
                logger.warning(f"Playwright fetch failed for {url}: {e}")
        except Exception:
            logger.debug("Playwright not available; skipping")

        # --- Fallback: Selenium (requires driver installed) ---
        try:
            from selenium import webdriver  # type: ignore
            from selenium.webdriver.chrome.options import Options  # type: ignore

            try:
                options = Options()
                options.add_argument("--headless=new")
                options.add_argument("--disable-gpu")
                options.add_argument("--no-sandbox")
                driver = webdriver.Chrome(options=options)
                driver.get(url)
                time.sleep(5)
                html = driver.page_source
                driver.quit()
                logger.info(f"Selenium fetched HTML for {url}")
                return html
            except Exception as e:
                logger.warning(f"Selenium fetch failed for {url}: {e}")
        except Exception:
            logger.debug("Selenium not available; skipping")

        return None

    def _extract_title(self, soup: BeautifulSoup, url: str) -> str:
        """Extract novel title from soup, cleaning site-specific suffixes"""
        domain = urlparse(url).netloc.lower()
        
        # Try site-specific title selectors first
        title = None
        
        if 'freewebnovel' in domain:
            # FreeWebNovel: title in h1.tit or .book-name
            title_tag = soup.select_one('h1.tit, .book-name, .novel-title')
            if title_tag:
                title = title_tag.get_text(strip=True)
        
        elif 'novelfire' in domain:
            title_tag = soup.select_one('.novel-title, h1.name')
            if title_tag:
                title = title_tag.get_text(strip=True)
        
        elif 'novelbuddy' in domain:
            title_tag = soup.select_one('.novel-title, h1')
            if title_tag:
                title = title_tag.get_text(strip=True)
        
        elif 'royalroad' in domain:
            title_tag = soup.select_one('h1')
            if title_tag:
                title = title_tag.get_text(strip=True)
        
        elif 'lnmtl' in domain:
            title_tag = soup.select_one('.novel-name, h1.title')
            if title_tag:
                title = title_tag.get_text(strip=True)
        
        # Fallback to generic h1 or title
        if not title:
            title_tag = soup.find('h1') or soup.find('title')
            if title_tag:
                title = title_tag.get_text(strip=True)
        
        if not title:
            return "Unknown Novel"

        # Clean up title
        if 'ranobes' in url:
            title = re.split(r'[•|]', title)[0].strip()
            title = re.sub(r'\s*online\s*-\s*RANOBES\.NET.*', '', title, flags=re.I)
            title = re.sub(r'\s*-\s*Read.*', '', title, flags=re.I)
        
        # Remove common suffixes
        title = re.sub(r'\s*-\s*Free Web Novel.*', '', title, flags=re.I)
        title = re.sub(r'\s*-\s*Read Free.*', '', title, flags=re.I)
        title = re.sub(r'\s*\|\s*Novel.*', '', title, flags=re.I)
        title = re.sub(r'\s*-\s*Novel\s*$', '', title, flags=re.I)
        
        return title.strip()

    def _extract_full_metadata(self, soup: BeautifulSoup, url: str) -> Dict:
        """Extract comprehensive metadata (author, cover, translator, genre, status) from any site"""
        domain = urlparse(url).netloc.lower()
        metadata = {
            'author': None,
            'cover_image': None,
            'translator': None,
            'genre': None,
            'status': None,
            'description': None,
        }
        
        # === AUTHOR ===
        author_selectors = [
            # FreeWebNovel
            '.author a', '.info a[href*="/author/"]', 
            # NovelFire / NovelBin
            '.author-name', 'a[href*="/author/"]',
            # NovelBuddy - check author-content
            '.author-content a', '.author-content',
            # RoyalRoad
            '.author a', '.fiction-info a[href*="/profile/"]',
            # LNMTL
            '.novel-author', '.panel-body a[href*="/author/"]',
            # Generic
            '[itemprop="author"]', '.author', 'span.author',
        ]
        for sel in author_selectors:
            try:
                el = soup.select_one(sel)
                if el:
                    author = el.get_text(strip=True)
                    if author and author.lower() not in ['author', 'authors', 'n/a', 'unknown', '']:
                        metadata['author'] = author
                        break
            except:
                pass
        
        # Fallback 1: LNMTL style - <dt>Authors</dt><dd>Name</dd>
        if not metadata['author']:
            try:
                dt_author = soup.find('dt', string=re.compile(r'^Authors?$', re.I))
                if dt_author:
                    dd = dt_author.find_next_sibling('dd')
                    if dd:
                        author = dd.get_text(strip=True)
                        if author and len(author) < 100:
                            metadata['author'] = author
            except:
                pass
        
        # Fallback 2: regex search for "Author: X" pattern in page text
        if not metadata['author']:
            try:
                page_text = soup.get_text()
                match = re.search(r'Author[s]?[:\s]+([A-Za-z0-9_\-\s]+?)(?:\n|Status|Genre|Chapter|Current)', page_text)
                if match:
                    author = match.group(1).strip()
                    if author and len(author) < 50:
                        metadata['author'] = author
            except:
                pass
        
        # === COVER IMAGE ===
        cover_selectors = [
            # FreeWebNovel
            '.pic img', '.book-img img', '.cover img',
            # NovelFire / NovelBin
            '.novel-cover img', '.book-cover img', '.cover-detail img',
            # NovelBuddy
            '.cover img', '.novel-cover img',
            # RoyalRoad
            '.cover-art-container img', '.fiction-cover img',
            # LNMTL
            '.novel-cover img', '.novel img',
            # Generic
            'img[alt*="cover"]', '.novel-img img', '.thumbnail img',
        ]
        for sel in cover_selectors:
            try:
                el = soup.select_one(sel)
                if el:
                    src = el.get('src') or el.get('data-src') or el.get('data-lazy-src')
                    if src and 'placeholder' not in src.lower() and 'nocover' not in src.lower():
                        if not src.startswith('http'):
                            src = urljoin(url, src)
                        metadata['cover_image'] = src
                        break
            except:
                pass
        
        # === TRANSLATOR ===
        trans_selectors = [
            'a[href*="/translator/"]', '.translator a', '[itemprop="translator"]',
            '.info li:contains("Translator") a',
        ]
        for sel in trans_selectors:
            try:
                el = soup.select_one(sel)
                if el:
                    trans = el.get_text(strip=True)
                    if trans and trans.lower() not in ['translator', 'n/a']:
                        metadata['translator'] = trans
                        break
            except:
                pass
        
        # === GENRE ===
        genre_selectors = [
            '.genres a', '.genre a', '.tags a', '[itemprop="genre"]',
            '.book-info .tag', '.novel-tags a',
        ]
        genres = []
        for sel in genre_selectors:
            try:
                elements = soup.select(sel)
                for el in elements[:10]:
                    g = el.get_text(strip=True)
                    if g and g.lower() not in ['genres', 'tags'] and len(g) < 50:
                        genres.append(g)
            except:
                pass
        if genres:
            metadata['genre'] = ', '.join(genres[:5])
        
        # === STATUS ===
        status_selectors = [
            '.status', '.novel-status', '[itemprop="status"]',
            '.info li:contains("Status")',
        ]
        for sel in status_selectors:
            try:
                el = soup.select_one(sel)
                if el:
                    status = el.get_text(strip=True)
                    if 'ongoing' in status.lower():
                        metadata['status'] = 'Ongoing'
                        break
                    elif 'complet' in status.lower():
                        metadata['status'] = 'Completed'
                        break
            except:
                pass
        
        # === DESCRIPTION ===
        desc_selectors = [
            '.summary', '.description', '.synopsis', '[itemprop="description"]',
            '.book-intro', '.novel-description', '.desc-text',
        ]
        for sel in desc_selectors:
            try:
                el = soup.select_one(sel)
                if el:
                    desc = el.get_text(strip=True)
                    if desc and len(desc) > 50:
                        metadata['description'] = desc[:2000]
                        break
            except:
                pass
        
        return metadata

    def _get_chapter_links_from_html(self, soup: BeautifulSoup,
                                     url: str) -> List[str]:
        """Extract chapter links from HTML with site-specific handling.

        Uses site-specific selectors where needed and otherwise falls back
        to common chapter list patterns.
        """
        links: List[str] = []
        parsed_url = urlparse(url)
        base = url
        domain = parsed_url.netloc.lower()
        base_path = parsed_url.path.rstrip('/')
        
        logger.debug(f"[CHAPTERS] Extracting chapter links from {domain}")

        # === NovelBin ===
        if 'novelbin' in domain:
            containers = soup.select('#chapter-list, .chapter-list, .list-chapter, #list-chapter')
            if not containers:
                containers = [soup]
            for cont in containers:
                for a in cont.find_all('a', href=True):
                    href = a['href']
                    if 'chapter' in href.lower() or '/c' in href.lower():
                        full = urljoin(base, href)
                        if full not in links:
                            links.append(full)
        
        # === NovelFire ===
        elif 'novelfire' in domain:
            # NovelFire uses .chapter-list for chapter links
            containers = soup.select('.chapter-list a')
            for a in containers:
                href = a.get('href', '')
                if href and '/chapter-' in href.lower():
                    full = urljoin(base, href)
                    if full not in links:
                        links.append(full)
        
        # === Ranobes ===
        elif 'ranobes' in domain:
            # Ranobes has chapters in .chapters-list or script data
            containers = soup.select('.chapters-list a, .chapter-item a, #chapters-list a')
            for a in containers:
                href = a.get('href', '')
                if href and '/read-' in href or '/chapters/' in href:
                    full = urljoin(base, href)
                    if full not in links:
                        links.append(full)
        
        # === FreeWebNovel ===
        elif 'freewebnovel' in domain:
            containers = soup.select('.chapter-list a, .m-newest2 a, .chapter-item a')
            for a in containers:
                href = a.get('href', '')
                if href and 'chapter' in href.lower():
                    full = urljoin(base, href)
                    if full not in links:
                        links.append(full)
        
        # === LightNovelWorld / LightNovelCave ===
        elif 'lightnovelworld' in domain or 'lightnovelcave' in domain or 'lnworld' in domain:
            containers = soup.select('.chapter-list a, .chapter-item a, ul.chapter-list a')
            for a in containers:
                href = a.get('href', '')
                if href and '/chapter-' in href.lower():
                    full = urljoin(base, href)
                    if full not in links:
                        links.append(full)
        
        # === BoxNovel / WordPress Manga Theme ===
        elif 'boxnovel' in domain or 'bednovel' in domain or 'fullnovels' in domain:
            containers = soup.select('.wp-manga-chapter a, .chapter-link a, .version-chap a')
            for a in containers:
                href = a.get('href', '')
                if href and 'chapter' in href.lower():
                    full = urljoin(base, href)
                    if full not in links:
                        links.append(full)
        
        # === RoyalRoad ===
        elif 'royalroad' in domain:
            containers = soup.select('#chapters tbody tr a, .chapter-row a')
            for a in containers:
                href = a.get('href', '')
                if href and '/chapter/' in href:
                    full = urljoin(base, href)
                    if full not in links:
                        links.append(full)
        
        # === NovelBuddy ===
        elif 'novelbuddy' in domain:
            # NovelBuddy: restrict to the novel's own chapter list to avoid site-wide recent updates
            slug_match = re.search(r'/novel/([^/?#]+)', url)
            slug = slug_match.group(1).lower() if slug_match else None

            chapter_containers = soup.select(
                '#chapter-list, .chapter-list, .list-chapter, .chapters, '
                '.chapter__list, #chapters, .list-chapter-book, .chapter-wrapper'
            )

            anchors = []
            for cont in chapter_containers:
                anchors.extend(cont.find_all('a', href=True))

            # Fallback: only anchors containing /chapter- and (if available) the slug
            if not anchors:
                anchors = soup.select('a[href*="/chapter-"]')

            for a in anchors:
                href = a.get('href', '')
                if '/chapter-' not in href.lower():
                    continue
                full = urljoin(base, href)
                parsed_full = urlparse(full)
                path_full = parsed_full.path.rstrip('/')
                # Require path to start with the novel path (strong filter)
                if base_path and not path_full.startswith(base_path):
                    continue
                if slug and slug not in path_full.lower():
                    continue  # skip cross-novel links
                if full not in links:
                    links.append(full)
        
        # === LibRead ===
        elif 'libread' in domain:
            # LibRead uses various chapter list containers
            for sel in ['.chapter-list a', '.chapters a', 'ul.list-chapter a', '.ul-list3 a']:
                containers = soup.select(sel)
                for a in containers:
                    href = a.get('href', '')
                    if href and 'chapter' in href.lower():
                        full = urljoin(base, href)
                        if full not in links:
                            links.append(full)
                if links:
                    break
        
        # === ReadNovelFull ===
        elif 'readnovelfull' in domain:
            containers = soup.select('.list-chapter a, #list-chapter a, .chapter-list a')
            for a in containers:
                href = a.get('href', '')
                if href and 'chapter' in href.lower():
                    full = urljoin(base, href)
                    if full not in links:
                        links.append(full)

        # === Generic fallback: any anchor with "chapter" in href ===
        if not links:
            logger.debug(f"[CHAPTERS] No site-specific links found, using generic fallback")
            for a in soup.find_all('a', href=True):
                href = a['href']
                # Match various chapter URL patterns
                if re.search(r'chapter[-_]?\d+|/ch[-_]?\d+|/c\d+', href.lower()):
                    full = urljoin(base, href)
                    if full not in links:
                        links.append(full)
            
            # If still no links, try broader pattern
            if not links:
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if 'chapter' in href.lower():
                        full = urljoin(base, href)
                        if full not in links:
                            links.append(full)

        logger.info(f"[CHAPTERS] Found {len(links)} chapter links for {url}")
        if links and len(links) <= 5:
            logger.debug(f"[CHAPTERS] Sample links: {links[:5]}")

        return links

    def _extract_chapter_number(self, url: str) -> Optional[int]:
        """Extract chapter number from URL for correct sorting"""
        try:
            # 1. Look for 'chapter-123' pattern (common in most URLs)
            match = re.search(r'chapter[-_]?(\d+)', url, re.IGNORECASE)
            if match:
                return int(match.group(1))

            # 2. Look for number at the end of URL (e.g., .../123.html)
            match = re.search(r'/(\d+)(?:\.html)?$', url)
            if match:
                return int(match.group(1))

            # 3. Look for 'c123' pattern
            match = re.search(r'/c(\d+)', url, re.IGNORECASE)
            if match:
                return int(match.group(1))

            return 999999  # If no number found, push to end of list
        except Exception:
            return 999999

    def _get_random_headers(self, for_site: str = None) -> Dict[str, str]:
        """Get headers for HTTP requests with anti-Cloudflare evasion.

        We keep a stable User-Agent per Scraper instance to look more like a
        real browser session, but randomize it once when the Scraper is
        created. Other headers stay consistent across requests.
        
        Args:
            for_site: Optional domain hint (e.g., 'novelbin.com') to set proper Referer/Origin
        """
        ua = self.headers.get('User-Agent') or random.choice(USER_AGENTS)
        
        # Base headers that look like a real browser
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        
        # If we know the site, add proper Referer for cross-origin requests
        if for_site:
            headers['Referer'] = f'https://{for_site}/'
            headers['Origin'] = f'https://{for_site}'
        
        return headers

    def _get_cloudflare_bypass_headers(self, url: str) -> Dict[str, str]:
        """Get specialized headers for Cloudflare-protected sites."""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Use a fresh random UA for Cloudflare bypass attempts
        ua = random.choice(USER_AGENTS)
        
        return {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Referer': f'https://{domain}/',
            'Origin': f'https://{domain}',
            'sec-ch-ua': '"Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'DNT': '1',
        }

    def _random_delay(self, min_delay: float = 0.5, max_delay: float = 1.5):
        """Add random delay between requests to avoid rate limiting"""
        self.request_count += 1
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def search_novelbin(self, query: str) -> Optional[str]:
        """Search NovelBin for a novel by title"""
        try:
            search_url = f"https://novelbin.me/?s={query.replace(' ', '+')}"
            resp = self.session.get(search_url,
                                headers=self._get_random_headers(for_site='novelbin.me'),
                                timeout=10)
            if resp.status_code != 200:
                logger.warning(f"[NovelBin] Search returned status {resp.status_code}")
                return None
            soup = BeautifulSoup(resp.content, 'html.parser')
            # Look for first result link
            result = soup.select_one('.list-items .item-info .novel-name a')
            if result and result.get('href'):
                return result['href']
        except Exception as e:
            logger.error(f"NovelBin search error: {e}")
        return None

    def search_novelbin_multiple(self, query: str) -> List[Dict[str, str]]:
        """Search NovelBin and return multiple results with titles.
        
        Tries multiple domains with Cloudflare bypass headers.
        """
        results = []

        # Try multiple NovelBin domains - order by reliability
        domains = [
            'https://novelbin.me', 'https://novelbin.com',
            'https://novelbin.cfd', 'https://novelbin.net',
            'https://novelbin.org', 'https://novelbin.cc'
        ]

        for domain in domains:
            if len(results) >= 10:
                break
            try:
                # NovelBin uses /search?keyword= endpoint
                search_url = f"{domain}/search?keyword={quote_plus(query)}"
                
                # Use Cloudflare bypass headers
                domain_name = domain.replace('https://', '')
                headers = self._get_cloudflare_bypass_headers(search_url)
                
                logger.debug(f"[NovelBin] Trying {domain} for query: {query}")
                resp = self.session.get(search_url, headers=headers, timeout=10)
                
                if resp.status_code == 403:
                    logger.debug(f"[NovelBin] {domain} returned 403, trying next...")
                    continue
                elif resp.status_code != 200:
                    logger.debug(f"[NovelBin] {domain} returned {resp.status_code}")
                    continue
                
                # Check for Cloudflare challenge
                content = resp.text[:500].lower()
                if 'checking your browser' in content or ('cloudflare' in content and 'challenge' in content):
                    logger.debug(f"[NovelBin] {domain} returned Cloudflare challenge")
                    continue
                    
                soup = BeautifulSoup(resp.content, 'html.parser')

                # NovelBin uses h3.novel-title a for results
                items = soup.select('h3.novel-title a')
                if not items:
                    items = soup.select('.novel-title a')
                if not items:
                    items = soup.select('.list-novel a[href*="/novel-book/"]')
                if not items:
                    items = soup.select('a[href*="/novel-book/"], a[href*="/b/"]')
                if not items:
                    items = soup.find_all('a', href=lambda h: h and ('/novel-book/' in h or '/b/' in h))

                for item in items[:5]:
                    href = item.get('href', '')
                    if href and ('/novel-book/' in href or '/novel/' in href or '/b/' in href):
                        # Make absolute URL
                        if not href.startswith('http'):
                            href = domain + href
                        title = item.get('title') or item.get_text(strip=True)
                        if title and len(title) > 2 and not any(r['url'] == href for r in results):
                            results.append({
                                'title': title,
                                'url': href,
                                'source': 'NovelBin'
                            })
                            
                if results:
                    logger.info(f"[NovelBin] Found {len(results)} results from {domain}")
                    break  # Got results from this domain
            except requests.exceptions.Timeout:
                logger.debug(f"[NovelBin] Timeout on {domain}")
                continue
            except Exception as e:
                logger.debug(f"[NovelBin] Error on {domain}: {type(e).__name__}: {e}")
                continue

        return results

    def search_royalroad(self, query: str) -> Optional[str]:
        """Search RoyalRoad for a novel by title"""
        try:
            search_url = f"https://www.royalroad.com/fictions/search?title={quote_plus(query)}"
            resp = requests.get(search_url,
                                headers=self._get_random_headers(),
                                timeout=10)
            soup = BeautifulSoup(resp.content, 'html.parser')
            result = soup.select_one('.fiction-list-item .fiction-title a')
            if result and result.get('href'):
                return urljoin("https://www.royalroad.com", result['href'])
        except Exception as e:
            logger.error(f"RoyalRoad search error: {e}")
        return None

    def search_royalroad_multiple(self, query: str) -> List[Dict[str, str]]:
        """Search RoyalRoad and return multiple results with titles"""
        results = []
        try:
            search_url = f"https://www.royalroad.com/fictions/search?title={quote_plus(query)}"
            resp = self.session.get(search_url,
                                headers=self._get_random_headers(),
                                timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')

            # RoyalRoad uses .fiction-title a for results
            items = soup.select('.fiction-title a')
            
            for item in items[:10]:
                href = item.get('href', '')
                title = item.get_text(strip=True)
                if title and href:  # Just need both to exist
                    full_url = urljoin("https://www.royalroad.com", href)
                    results.append({
                        'title': title,
                        'url': full_url,
                        'source': 'RoyalRoad'
                    })
                if len(results) >= 5:
                    break
                    
            logger.info(f"[RoyalRoad] Found {len(results)} results for '{query}'")
        except Exception as e:
            logger.error(f"RoyalRoad search error: {e}")
        return results

"""
Manga Scraper Module - Downloads manga chapters and images from various sites
Supported sites: MangaDex, MangaPark, MangaHere, MangaFox, Mangago, Mangamya, AsuraComic, MangaPill, MangaBuddy, MangaDass, and more
Uses Playwright Stealth for anti-bot bypass when needed
"""

import os
import re
import time
import random
import logging
import asyncio
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, quote_plus
from typing import Dict, List, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

PROTECTED_MANGA_SITES = [
    'viz.com', 'asuracomic.net', 'asurascans.com',
    'mangakakalot.com', 'chapmanganato.to'
]

def is_protected_manga_site(url: str) -> bool:
    url_lower = url.lower()
    return any(site in url_lower for site in PROTECTED_MANGA_SITES)

def run_pw_async(coro, timeout: float = 120.0):
    """Run coroutine in the dedicated Playwright event loop thread"""
    try:
        from playwright_scraper import run_in_pw_loop
        return run_in_pw_loop(coro, timeout)
    except Exception as e:
        logger.error(f"Playwright async execution failed: {e}")
        raise

class MangaScraper:
    """Scraper for manga sites - downloads chapter images"""
    
    SUPPORTED_DOMAINS = [
        'mangadex.org',
        'manganato.com', 'chapmanganato.com', 'chapmanganato.to',
        'mangakakalot.com', 'mangakakalot.tv',
        'mangago.me',
        'mangapark.net', 'mangapark.io', 'mangapark.com',
        'mangahere.cc',
        'fanfox.net', 'mangafox.me',
        'mangamya.com',
        # New sites
        'viz.com',
        'magimanga.net',
        'mangaread.org',
        'mangapill.com',
        'mangatoto.com',
        'mangabuddy.com',
        # AsuraComic
        'asuracomic.net', 'asurascans.com', 'asura.gg',
        # Additional sites (December 2025)
        'manhuaus.com',
        'infinite-mage.org',
        'mangadass.com',
        'novamanga.com',
        'mangapaw.com',
        'daotranslate.com',
    ]
    
    # Sites to search via DuckDuckGo
    DUCKDUCKGO_SEARCH_SITES = [
        'mangadex.org',
        'manganato.com',
        'mangakakalot.com',
        'mangapark.net',
        'mangapill.com',
        'mangatoto.com',
        'mangabuddy.com',
        'mangaread.org',
        'asuracomic.net',
        'manhuaus.com',
        'mangadass.com',
        'novamanga.com',
        'mangapaw.com',
    ]
    
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Edge/120.0.0.0',
    ]
    
    def __init__(self, use_playwright: bool = True):
        self.session = requests.Session()
        self.cancelled = False
        self.use_playwright = use_playwright
    
    def _get_random_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.google.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
        }
    
    def _random_delay(self, min_delay: float = 0.2, max_delay: float = 0.6):
        time.sleep(random.uniform(min_delay, max_delay))
    
    def cancel(self):
        """Cancel ongoing operations"""
        self.cancelled = True
    
    def reset(self):
        """Reset cancellation flag"""
        self.cancelled = False
    
    @staticmethod
    def is_manga_url(url: str) -> bool:
        """Check if URL is from a supported manga site"""
        try:
            domain = urlparse(url).netloc.lower()
            return any(supported in domain for supported in MangaScraper.SUPPORTED_DOMAINS)
        except:
            return False
    
    def _detect_site_type(self, url: str) -> str:
        """Detect which manga site we're scraping"""
        domain = urlparse(url).netloc.lower()
        
        if 'mangadex' in domain:
            return 'mangadex'
        elif 'manganato' in domain or 'chapmanganato' in domain:
            return 'manganato'
        elif 'mangakakalot' in domain:
            return 'mangakakalot'
        elif 'mangago' in domain:
            return 'mangago'
        elif 'mangapark' in domain:
            return 'mangapark'
        elif 'mangahere' in domain:
            return 'mangahere'
        elif 'fanfox' in domain or 'mangafox' in domain:
            return 'mangafox'
        elif 'mangamya' in domain:
            return 'mangamya'
        elif 'mangapill' in domain:
            return 'mangapill'
        elif 'mangatoto' in domain:
            return 'mangatoto'
        elif 'mangabuddy' in domain:
            return 'mangabuddy'
        elif 'mangaread' in domain:
            return 'mangaread'
        elif 'viz.com' in domain:
            return 'viz'
        elif 'magimanga' in domain:
            return 'magimanga'
        elif 'asura' in domain:
            return 'asura'
        # New sites (December 2025) - use generic parser
        elif 'manhuaus' in domain:
            return 'madara'  # Madara WordPress theme
        elif 'infinite-mage' in domain:
            return 'generic'  # Single manga site
        elif 'mangadass' in domain:
            return 'generic'  # Generic manga site
        elif 'novamanga' in domain:
            return 'generic'  # Generic manga site
        elif 'mangapaw' in domain:
            return 'generic'  # Generic manga site
        elif 'daotranslate' in domain:
            return 'madara'  # Madara WordPress theme
        else:
            return 'generic'
    
    def _get_site_name(self, url: str) -> str:
        """Get friendly site name from URL"""
        domain = urlparse(url).netloc.lower()
        site_names = {
            'mangadex': 'MangaDex',
            'manganato': 'Manganato',
            'chapmanganato': 'Manganato',
            'mangakakalot': 'Mangakakalot',
            'mangago': 'Mangago',
            'mangapark': 'MangaPark',
            'mangahere': 'MangaHere',
            'fanfox': 'MangaFox',
            'mangamya': 'Mangamya',
            'mangapill': 'MangaPill',
            'mangatoto': 'MangaToto',
            'mangabuddy': 'MangaBuddy',
            'mangaread': 'MangaRead',
            'viz': 'VIZ',
            'magimanga': 'MagiManga',
            'asura': 'AsuraComic',
            # New sites
            'manhuaus': 'ManhuaUS',
            'infinite-mage': 'InfiniteMage',
            'mangadass': 'MangaDass',
            'novamanga': 'NovaManga',
            'mangapaw': 'MangaPaw',
            'daotranslate': 'DaoTranslate',
        }
        for key, name in site_names.items():
            if key in domain:
                return name
        return 'Unknown'
    
    def search_manga(self, query: str, progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """Search for manga across multiple sites using DuckDuckGo + site-specific searches"""
        results = []
        
        if progress_callback:
            progress_callback("Searching manga sites...")
        
        logger.info(f"Searching manga: {query}")
        
        # First try DuckDuckGo search (covers more sites)
        try:
            ddg_results = self._search_with_duckduckgo(query)
            results.extend(ddg_results)
            logger.info(f"DuckDuckGo found {len(ddg_results)} results")
        except Exception as e:
            logger.warning(f"DuckDuckGo search error: {e}")
        
        # Also try site-specific searches for reliability
        # Search Manganato
        try:
            manganato_results = self._search_manganato(query)
            results.extend(manganato_results[:3])
            logger.info(f"Manganato found {len(manganato_results)} results")
        except Exception as e:
            logger.warning(f"Manganato search error: {e}")
        
        # Search Mangakakalot
        try:
            mangakakalot_results = self._search_mangakakalot(query)
            results.extend(mangakakalot_results[:3])
            logger.info(f"Mangakakalot found {len(mangakakalot_results)} results")
        except Exception as e:
            logger.warning(f"Mangakakalot search error: {e}")
        
        # Search MangaPark
        try:
            mangapark_results = self._search_mangapark(query)
            results.extend(mangapark_results[:2])
        except Exception as e:
            logger.warning(f"MangaPark search error: {e}")
        
        # Search MangaDex API directly
        try:
            mangadex_results = self._search_mangadex(query)
            results.extend(mangadex_results[:3])
            logger.info(f"MangaDex found {len(mangadex_results)} results")
        except Exception as e:
            logger.warning(f"MangaDex search error: {e}")
        
        # Search MangaPill
        try:
            mangapill_results = self._search_mangapill(query)
            results.extend(mangapill_results[:3])
            logger.info(f"MangaPill found {len(mangapill_results)} results")
        except Exception as e:
            logger.warning(f"MangaPill search error: {e}")
        
        # Search MangaBuddy
        try:
            mangabuddy_results = self._search_mangabuddy(query)
            results.extend(mangabuddy_results[:3])
            logger.info(f"MangaBuddy found {len(mangabuddy_results)} results")
        except Exception as e:
            logger.warning(f"MangaBuddy search error: {e}")
        
        # Search AsuraComic
        try:
            asura_results = self._search_asura(query)
            results.extend(asura_results[:3])
            logger.info(f"AsuraComic found {len(asura_results)} results")
        except Exception as e:
            logger.warning(f"AsuraComic search error: {e}")
        
        # Search MangaToto
        try:
            mangatoto_results = self._search_mangatoto(query)
            results.extend(mangatoto_results[:3])
            logger.info(f"MangaToto found {len(mangatoto_results)} results")
        except Exception as e:
            logger.warning(f"MangaToto search error: {e}")
        
        # Search MangaDass
        try:
            mangadass_results = self._search_mangadass(query)
            results.extend(mangadass_results[:3])
            logger.info(f"MangaDass found {len(mangadass_results)} results")
        except Exception as e:
            logger.warning(f"MangaDass search error: {e}")
        
        # Search MangaPaw
        try:
            mangapaw_results = self._search_mangapaw(query)
            results.extend(mangapaw_results[:3])
            logger.info(f"MangaPaw found {len(mangapaw_results)} results")
        except Exception as e:
            logger.warning(f"MangaPaw search error: {e}")
        
        # Score and sort all results by relevance to query
        query_lower = query.lower().strip()
        scored_results = []
        for r in results:
            title_lower = r['title'].lower()
            if title_lower == query_lower:
                score = 100
            elif title_lower.startswith(query_lower + ' ') or title_lower.startswith(query_lower + ':'):
                score = 90
            elif title_lower.startswith(query_lower):
                score = 80
            elif query_lower in title_lower.split():
                score = 70
            elif query_lower in title_lower:
                score = 50
            else:
                score = 10
            scored_results.append((score, r))
        
        # Sort by score (highest first)
        scored_results.sort(key=lambda x: x[0], reverse=True)
        
        # Deduplicate by title similarity while preserving score order
        unique_results = []
        seen_titles = set()
        for score, r in scored_results:
            title_key = re.sub(r'[^a-z0-9]', '', r['title'].lower())[:30]
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_results.append(r)
        
        logger.info(f"Total unique results: {len(unique_results)}")
        return unique_results[:10]
    
    def _search_with_duckduckgo(self, query: str) -> List[Dict[str, Any]]:
        """Search DuckDuckGo for manga across multiple sites"""
        from urllib.parse import unquote
        results = []
        
        # Build multi-site search query (include all configured sites)
        sites = ' OR '.join([f'site:{site}' for site in self.DUCKDUCKGO_SEARCH_SITES])
        search_query = f"{query} manga ({sites})"
        
        ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(search_query)}"
        
        try:
            self._random_delay(0.2, 0.5)
            resp = self.session.get(ddg_url, headers=self._get_random_headers(), timeout=15)
            
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'html.parser')
                
                for link in soup.select('.result__a'):
                    href = link.get('href', '')
                    
                    # DuckDuckGo wraps URLs in redirect
                    if 'uddg=' in href:
                        actual_url = unquote(href.split('uddg=')[1].split('&')[0])
                        
                        # Only include manga-related links
                        if any(site in actual_url for site in self.SUPPORTED_DOMAINS):
                            # Skip tag pages, category pages, chapter-specific pages - these aren't actual manga pages
                            skip_patterns = ['/manga-tags/', '/manga-tag/', '/tag/', '/category/', '/genre/', '/genres/', '-chapter-', '-online']
                            if any(pattern in actual_url for pattern in skip_patterns):
                                logger.debug(f"Skipping non-manga URL: {actual_url}")
                                continue
                            
                            title = link.get_text(strip=True)[:100] or query
                            title = title.replace('\\%', '%').replace('\\', '')
                            source = self._get_site_name(actual_url)
                            
                            # Avoid duplicates
                            if not any(r['url'] == actual_url for r in results):
                                results.append({
                                    'title': title,
                                    'url': actual_url,
                                    'source': source
                                })
                            
                            if len(results) >= 8:
                                break
        
        except Exception as e:
            logger.warning(f"DuckDuckGo manga search failed: {e}")
        
        return results
    
    def _search_mangadex(self, query: str) -> List[Dict[str, Any]]:
        """Search MangaDex using their API with better matching"""
        results = []
        query_lower = query.lower().strip()
        
        def score_and_get_results(search_query: str) -> List[Dict[str, Any]]:
            """Helper to search and score results"""
            scored_results = []
            try:
                api_url = (
                    f"https://api.mangadex.org/manga?title={quote_plus(search_query)}&limit=10&includes[]=cover_art"
                    f"&contentRating[]=safe&contentRating[]=suggestive&contentRating[]=erotica"
                )
                resp = self.session.get(api_url, headers=self._get_random_headers(), timeout=10)
                
                if resp.status_code == 200:
                    data = resp.json()
                    for manga in data.get('data', []):
                        manga_id = manga.get('id', '')
                        attrs = manga.get('attributes', {})
                        
                        titles = attrs.get('title', {})
                        alt_titles = attrs.get('altTitles', [])
                        
                        all_titles = list(titles.values())
                        for alt in alt_titles:
                            all_titles.extend(alt.values())
                        
                        title = titles.get('en', '') or (list(titles.values())[0] if titles else search_query)
                        
                        score = 0
                        for t in all_titles:
                            t_lower = t.lower().strip()
                            if t_lower == query_lower:
                                score = 100
                                break
                            elif t_lower.startswith(query_lower + ' ') or t_lower.startswith(query_lower + ':'):
                                score = max(score, 90)  # Title starts with exact query word
                            elif t_lower.startswith(query_lower):
                                score = max(score, 80)
                            elif query_lower in t_lower.split():
                                score = max(score, 70)  # Query is a complete word in title
                            elif query_lower in t_lower:
                                score = max(score, 50)
                        
                        if manga_id and score > 0:
                            scored_results.append({
                                'title': title,
                                'url': f"https://mangadex.org/title/{manga_id}",
                                'source': 'MangaDex',
                                'score': score,
                                'manga_id': manga_id
                            })
            except Exception as e:
                logger.debug(f"MangaDex search error: {e}")
            
            return scored_results
        
        # Try original query first
        all_scored = score_and_get_results(query)
        
        # Always try expanded queries to find exact matches that the original might miss
        # (MangaDex search often returns partial matches before exact matches)
        for expansion in ['labyrinth', 'adventure', 'sinbad', 'manga']:
            expanded = f"{query} {expansion}"
            extra = score_and_get_results(expanded)
            for r in extra:
                if not any(x['manga_id'] == r['manga_id'] for x in all_scored):
                    all_scored.append(r)
        
        # Sort by score and deduplicate
        all_scored.sort(key=lambda x: x['score'], reverse=True)
        seen_ids = set()
        for r in all_scored:
            if r['manga_id'] not in seen_ids and len(results) < 5:
                seen_ids.add(r['manga_id'])
                results.append({
                    'title': r['title'],
                    'url': r['url'],
                    'source': r['source']
                })
        
        return results
    
    def _search_manganato(self, query: str) -> List[Dict[str, Any]]:
        """Search Manganato - currently disabled due to site issues"""
        # mangakakalot.to has broken image rendering, disabled for now
        return []
    
    def _search_mangakakalot(self, query: str) -> List[Dict[str, Any]]:
        """Search Mangakakalot - currently disabled due to site issues"""
        # mangakakalot.to has broken image rendering, disabled for now
        return []
    
    def _search_mangapark(self, query: str) -> List[Dict[str, Any]]:
        """Search MangaPark"""
        results = []
        search_url = f"https://mangapark.net/search?word={quote_plus(query)}"
        
        try:
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=10)
            if resp.status_code != 200:
                return results
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            items = soup.select('.item, .pb-3')
            
            for item in items[:5]:
                link = item.select_one('a.fw-bold, a[href*="/title/"]')
                if not link:
                    continue
                
                title = link.get_text(strip=True)
                url = link.get('href', '')
                if url and not url.startswith('http'):
                    url = urljoin('https://mangapark.net', url)
                
                if title and url:
                    results.append({
                        'title': title,
                        'url': url,
                        'source': 'MangaPark'
                    })
        except Exception as e:
            logger.debug(f"MangaPark search error: {e}")
        
        return results
    
    def _search_mangapill(self, query: str) -> List[Dict[str, Any]]:
        """Search MangaPill directly"""
        results = []
        search_url = f"https://mangapill.com/search?q={quote_plus(query)}"
        
        try:
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=10)
            if resp.status_code != 200:
                return results
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            items = soup.select('.grid > div')
            
            for item in items[:5]:
                link = item.select_one('a[href*="/manga/"]')
                if not link:
                    continue
                
                title_elem = item.select_one('.font-bold, a div.mt-1')
                title = title_elem.get_text(strip=True) if title_elem else link.get_text(strip=True)
                url = link.get('href', '')
                if url and not url.startswith('http'):
                    url = urljoin('https://mangapill.com', url)
                
                if title and url and '/manga/' in url:
                    results.append({
                        'title': title,
                        'url': url,
                        'source': 'MangaPill'
                    })
        except Exception as e:
            logger.debug(f"MangaPill search error: {e}")
        
        return results
    
    def _search_mangabuddy(self, query: str) -> List[Dict[str, Any]]:
        """Search MangaBuddy directly"""
        results = []
        search_url = f"https://mangabuddy.com/search?q={quote_plus(query)}"
        
        try:
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=10)
            if resp.status_code != 200:
                return results
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            items = soup.select('.book-item, .list-item, .book-detailed-item')
            
            for item in items[:5]:
                # Get the main manga link - avoid navigation/tags links
                link = item.select_one('a.title, a[href]:not([href*="manga-tags"]):not([href*="genre"]):not([href*="author"])')
                if not link:
                    # Try first link but filter later
                    link = item.select_one('a[href]')
                if not link:
                    continue
                
                url = link.get('href', '')
                # Skip non-manga pages (tags, genres, authors, etc.)
                if any(skip in url for skip in ['/manga-tags/', '/genre/', '/author/', '/search', '/page/']):
                    continue
                    
                if url and not url.startswith('http'):
                    url = urljoin('https://mangabuddy.com', url)
                
                title_elem = item.select_one('.book-title, .title, h3, a.title')
                title = title_elem.get_text(strip=True) if title_elem else link.get('title', '')
                
                # Only include if it looks like a manga detail page (short path, no special segments)
                if title and url and 'mangabuddy.com/' in url:
                    # Validate URL structure - manga pages are typically /manga-name
                    path = url.replace('https://mangabuddy.com/', '').split('/')[0]
                    logger.debug(f"MangaBuddy URL check: {url} -> path={path}")
                    if path and not any(x in path for x in ['search', 'genre', 'author', 'manga-tags', 'page']):
                        logger.info(f"MangaBuddy adding result: {title} -> {url}")
                        results.append({
                            'title': title,
                            'url': url,
                            'source': 'MangaBuddy'
                        })
                    else:
                        logger.debug(f"MangaBuddy skipped (bad path): {url}")
        except Exception as e:
            logger.debug(f"MangaBuddy search error: {e}")
        
        logger.info(f"MangaBuddy final results: {len(results)} items")
        return results
    
    def _search_asura(self, query: str) -> List[Dict[str, Any]]:
        """Search AsuraComic directly"""
        results = []
        search_url = f"https://asuracomic.net/series?page=1&name={quote_plus(query)}"
        
        try:
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=10)
            if resp.status_code != 200:
                return results
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            items = soup.select('.grid a[href*="/series/"]')
            
            seen = set()
            for item in items[:10]:
                url = item.get('href', '')
                if not url.startswith('http'):
                    url = urljoin('https://asuracomic.net', url)
                
                if url in seen:
                    continue
                seen.add(url)
                
                title_elem = item.select_one('span.block, .text-sm')
                title = title_elem.get_text(strip=True) if title_elem else ''
                
                if title and url and '/series/' in url:
                    results.append({
                        'title': title,
                        'url': url,
                        'source': 'AsuraComic'
                    })
                    if len(results) >= 5:
                        break
        except Exception as e:
            logger.debug(f"AsuraComic search error: {e}")
        
        return results
    
    def _search_mangatoto(self, query: str) -> List[Dict[str, Any]]:
        """Search MangaToto directly"""
        results = []
        search_url = f"https://mangatoto.com/search?word={quote_plus(query)}"
        
        try:
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=10)
            if resp.status_code != 200:
                return results
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            items = soup.select('.item, .book-item, a[href*="/series/"]')
            
            seen = set()
            for item in items[:10]:
                if item.name == 'a':
                    link = item
                else:
                    link = item.select_one('a[href*="/series/"], a[href*="/manga/"]')
                
                if not link:
                    continue
                
                url = link.get('href', '')
                if not url.startswith('http'):
                    url = urljoin('https://mangatoto.com', url)
                
                if url in seen:
                    continue
                seen.add(url)
                
                title_elem = link.select_one('.title, .name, h3, h4') or link
                title = title_elem.get_text(strip=True) if title_elem else ''
                
                if title and url and ('/series/' in url or '/manga/' in url):
                    results.append({
                        'title': title,
                        'url': url,
                        'source': 'MangaToto'
                    })
                    if len(results) >= 5:
                        break
        except Exception as e:
            logger.debug(f"MangaToto search error: {e}")
        
        return results
    
    def _search_mangadass(self, query: str) -> List[Dict[str, Any]]:
        """Search MangaDass directly"""
        results = []
        search_url = f"https://mangadass.com/?s={quote_plus(query)}"
        
        try:
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=10)
            if resp.status_code != 200:
                return results
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            items = soup.select('.bsx, .bs, article, .listupd .bs')
            
            for item in items[:5]:
                link = item.select_one('a[href*="/manga/"]')
                if not link:
                    continue
                
                title_elem = item.select_one('.tt, .title, h2, h3')
                title = title_elem.get_text(strip=True) if title_elem else link.get('title', '')
                url = link.get('href', '')
                
                if title and url:
                    results.append({
                        'title': title,
                        'url': url,
                        'source': 'MangaDass'
                    })
        except Exception as e:
            logger.debug(f"MangaDass search error: {e}")
        
        return results
    
    def _search_mangapaw(self, query: str) -> List[Dict[str, Any]]:
        """Search MangaPaw directly"""
        results = []
        search_url = f"https://mangapaw.com/search/{quote_plus(query.replace(' ', '-'))}"
        
        try:
            resp = self.session.get(search_url, headers=self._get_random_headers(), timeout=10)
            if resp.status_code != 200:
                return results
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            items = soup.select('.novel-item, .search-item, a[href*="/manga/"]')
            
            seen = set()
            for item in items[:10]:
                if item.name == 'a':
                    link = item
                else:
                    link = item.select_one('a[href*="/manga/"]')
                
                if not link:
                    continue
                
                url = link.get('href', '')
                if not url.startswith('http'):
                    url = urljoin('https://mangapaw.com', url)
                
                if url in seen or '/chapter' in url:
                    continue
                seen.add(url)
                
                title_elem = link.select_one('.title, h3, h4') or link
                title = title_elem.get_text(strip=True) if title_elem else ''
                
                if title and url and '/manga/' in url:
                    results.append({
                        'title': title,
                        'url': url,
                        'source': 'MangaPaw'
                    })
                    if len(results) >= 5:
                        break
        except Exception as e:
            logger.debug(f"MangaPaw search error: {e}")
        
        return results
    
    def get_manga_info(self, url: str) -> Dict[str, Any]:
        """Get manga information and chapter list"""
        site_type = self._detect_site_type(url)
        logger.info(f"Fetching manga info from {url} (detected site: {site_type})")
        
        try:
            resp = self.session.get(url, headers=self._get_random_headers(), timeout=15)
            logger.info(f"Response status: {resp.status_code}")
            if resp.status_code != 200:
                return {'error': f'Failed to fetch manga page (status {resp.status_code})'}
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            if site_type == 'manganato':
                result = self._parse_manganato_info(soup, url)
            elif site_type == 'mangakakalot':
                result = self._parse_mangakakalot_info(soup, url)
            elif site_type == 'mangapark':
                result = self._parse_mangapark_info(soup, url)
            elif site_type == 'mangadex':
                result = self._parse_mangadex_info(url)
            elif site_type == 'mangapill':
                result = self._parse_mangapill_info(soup, url)
            elif site_type == 'asura':
                result = self._parse_asura_info(soup, url)
            elif site_type == 'madara':
                result = self._parse_madara_info(soup, url)
            elif site_type == 'mangabuddy':
                result = self._parse_mangabuddy_info(soup, url)
            else:
                result = self._parse_generic_manga_info(soup, url)
            
            # Log result summary
            chapters_count = len(result.get('chapters', []))
            title = result.get('title', 'Unknown')
            logger.info(f"Manga info result: title='{title}', chapters={chapters_count}, source={result.get('source', 'unknown')}")
            
            return result
                
        except Exception as e:
            logger.error(f"Error getting manga info: {e}", exc_info=True)
            return {'error': str(e)}
    
    def _parse_manganato_info(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Parse Manganato manga page"""
        info = {
            'title': '',
            'cover': '',
            'chapters': [],
            'source': 'Manganato'
        }
        
        # Title
        title_elem = soup.select_one('.story-info-right h1, .panel-story-info .story-info-right h1')
        if title_elem:
            info['title'] = title_elem.get_text(strip=True)
        
        # Cover image
        cover_elem = soup.select_one('.story-info-left img.img-loading, .info-image img')
        if cover_elem:
            info['cover'] = cover_elem.get('src', '')
        
        # Chapters
        chapter_list = soup.select('.row-content-chapter li a, .panel-story-chapter-list li a')
        for chap in reversed(chapter_list):  # Reverse to get oldest first
            chap_url = chap.get('href', '')
            chap_title = chap.get_text(strip=True)
            if chap_url:
                info['chapters'].append({
                    'title': chap_title,
                    'url': chap_url
                })
        
        return info
    
    def _parse_mangakakalot_info(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Parse Mangakakalot manga page - legacy domains only (mangakakalot.to removed)"""
        info = {
            'title': '',
            'cover': '',
            'chapters': [],
            'source': 'Mangakakalot'
        }
        
        # Title
        title_elem = soup.select_one('.manga-info-text h1, h1')
        if title_elem:
            info['title'] = title_elem.get_text(strip=True)
        
        # Cover image
        cover_elem = soup.select_one('.manga-info-pic img')
        if cover_elem:
            info['cover'] = cover_elem.get('src', '')
        
        # Chapters - static selectors for mangakakalot.com/tv
        chapter_list = soup.select('.chapter-list .row a, .manga-info-chapter .row a, .list-chapter a')
        for chap in reversed(chapter_list):
            chap_url = chap.get('href', '')
            chap_title = chap.get_text(strip=True)
            if chap_url:
                if not chap_url.startswith('http'):
                    chap_url = urljoin(url, chap_url)
                info['chapters'].append({
                    'title': chap_title,
                    'url': chap_url
                })
        
        return info
    
    def _parse_mangapark_info(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Parse MangaPark manga page using GraphQL API"""
        info = {
            'title': '',
            'cover': '',
            'chapters': [],
            'source': 'MangaPark'
        }
        
        try:
            # Extract comic ID from URL or page content
            # URL format: /title/123456-en-manga-name
            comic_id = None
            match = re.search(r'/title/(\d+)', url)
            if match:
                comic_id = match.group(1)
            
            if not comic_id:
                # Try to find in page HTML (embedded JSON)
                page_text = str(soup)
                id_match = re.search(r'"comicId"[:\s]*"?(\d+)"?', page_text)
                if id_match:
                    comic_id = id_match.group(1)
            
            if not comic_id:
                logger.error(f"Could not extract MangaPark comic ID from {url}")
                return info
            
            logger.info(f"MangaPark comic ID: {comic_id}")
            
            # Get title and cover from page
            title_elem = soup.select_one('h3.item-title, h1, .comic-title, title')
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                # Clean up title (remove site name suffix)
                info['title'] = re.sub(r'\s*[-|]\s*MangaPark.*$', '', title_text).strip()
            
            cover_elem = soup.select_one('.detail-set img, .attr-cover img, img[src*="cover"]')
            if cover_elem:
                info['cover'] = cover_elem.get('src', '')
            
            # Use GraphQL API to get chapters
            graphql_url = "https://mangapark.net/apo/"
            
            # GraphQL query to get chapter list
            query = """
            query get_comicChapterList($comicId: ID!) {
                get_comicChapterList(comicId: $comicId) {
                    data {
                        id
                        dname
                        title
                        dateCreate
                        urlPath
                    }
                }
            }
            """
            
            payload = {
                "query": query,
                "variables": {"comicId": comic_id}
            }
            
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Origin': 'https://mangapark.net',
                'Referer': url,
                'User-Agent': random.choice(self.USER_AGENTS)
            }
            
            # Set NSFW cookie for adult content
            cookies = {'__nsfw': '1'}
            
            resp = self.session.post(graphql_url, json=payload, headers=headers, cookies=cookies, timeout=15)
            
            if resp.status_code == 200:
                data = resp.json()
                chapters_data = data.get('data', {}).get('get_comicChapterList', {}).get('data', [])
                
                for chap in chapters_data:
                    chap_id = chap.get('id', '')
                    chap_name = chap.get('dname', '') or chap.get('title', '') or f"Chapter {chap_id}"
                    url_path = chap.get('urlPath', '')
                    
                    if url_path:
                        chap_url = f"https://mangapark.net{url_path}" if not url_path.startswith('http') else url_path
                    else:
                        # Construct URL from ID
                        chap_url = f"https://mangapark.net/title/{comic_id}/{chap_id}"
                    
                    info['chapters'].append({
                        'title': chap_name,
                        'url': chap_url,
                        'chapter_id': chap_id
                    })
                
                # Sort by chapter ID (usually numeric, oldest first)
                info['chapters'].sort(key=lambda x: int(x.get('chapter_id', 0)) if str(x.get('chapter_id', '')).isdigit() else 0)
                logger.info(f"MangaPark GraphQL found {len(info['chapters'])} chapters")
            else:
                logger.error(f"MangaPark GraphQL error: {resp.status_code}")
                
        except Exception as e:
            logger.error(f"MangaPark parsing error: {e}")
        
        return info
    
    def _parse_mangadex_info(self, url: str) -> Dict[str, Any]:
        """Parse MangaDex using their API"""
        info = {
            'title': '',
            'cover': '',
            'chapters': [],
            'source': 'MangaDex'
        }
        
        # Extract manga ID from URL
        match = re.search(r'/title/([a-f0-9-]+)', url)
        if not match:
            return info
        
        manga_id = match.group(1)
        
        try:
            # Get manga info
            api_url = f"https://api.mangadex.org/manga/{manga_id}?includes[]=cover_art"
            resp = self.session.get(api_url, headers=self._get_random_headers(), timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                manga_data = data.get('data', {})
                attrs = manga_data.get('attributes', {})
                
                # Title (prefer English)
                titles = attrs.get('title', {})
                info['title'] = titles.get('en', '') or list(titles.values())[0] if titles else ''
                
                # Cover
                relationships = manga_data.get('relationships', [])
                for rel in relationships:
                    if rel.get('type') == 'cover_art':
                        cover_filename = rel.get('attributes', {}).get('fileName', '')
                        if cover_filename:
                            info['cover'] = f"https://uploads.mangadex.org/covers/{manga_id}/{cover_filename}"
                        break
            
            # Get chapters (English only, sorted by chapter number) - paginate as API limit is 100
            # Track seen chapter numbers to avoid duplicates from different scanlation groups
            seen_chapter_nums = set()
            offset = 0
            limit = 100
            while True:
                chapters_url = f"https://api.mangadex.org/chapter?manga={manga_id}&translatedLanguage[]=en&order[chapter]=asc&limit={limit}&offset={offset}"
                resp = self.session.get(chapters_url, headers=self._get_random_headers(), timeout=15)
                if resp.status_code != 200:
                    break
                    
                data = resp.json()
                chapters_data = data.get('data', [])
                
                if not chapters_data:
                    break
                    
                for chap in chapters_data:
                    chap_attrs = chap.get('attributes', {})
                    chap_num = chap_attrs.get('chapter', '?')
                    chap_title = chap_attrs.get('title', '') or f"Chapter {chap_num}"
                    chap_id = chap.get('id', '')
                    
                    # Skip duplicate chapter numbers (keep first/best scanlation)
                    if chap_num in seen_chapter_nums:
                        continue
                    seen_chapter_nums.add(chap_num)
                    
                    if chap_id:
                        info['chapters'].append({
                            'title': f"Chapter {chap_num}: {chap_title}" if chap_title != f"Chapter {chap_num}" else chap_title,
                            'url': f"https://mangadex.org/chapter/{chap_id}",
                            'chapter_id': chap_id
                        })
                
                # Check if we need more pages
                total = data.get('total', 0)
                offset += limit
                if offset >= total:
                    break
        
        except Exception as e:
            logger.error(f"MangaDex API error: {e}")
        
        return info
    
    def _parse_asura_info(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Parse AsuraComic manga page"""
        info = {
            'title': '',
            'cover': '',
            'chapters': [],
            'source': 'AsuraComic'
        }
        
        # Title - from <title> tag or span with title text
        title_elem = soup.select_one('title')
        if title_elem:
            title_text = title_elem.get_text(strip=True)
            # Remove " - Asura Scans" suffix
            if ' - ' in title_text:
                info['title'] = title_text.split(' - ')[0].strip()
            else:
                info['title'] = title_text
        
        # Also try span with font-bold
        if not info['title']:
            title_span = soup.select_one('span.text-xl.font-bold')
            if title_span:
                info['title'] = title_span.get_text(strip=True)
        
        # Cover image
        cover_elem = soup.select_one('img.object-cover[src*="storage/media"]')
        if cover_elem:
            info['cover'] = cover_elem.get('src', '')
        
        # Chapters - look for chapter links in href
        chapter_links = soup.select('a[href*="/chapter/"], a[href*="chapter/"]')
        seen_urls = set()
        chapters = []
        
        for link in chapter_links:
            href = link.get('href', '')
            if not href.startswith('http'):
                # Normalize URL - ensure it's absolute and has /series/ prefix
                # Handle cases like "nano-machine-ac59e269/chapter/1", "/series/xxx/chapter/1", "/chapter/1"
                if '/series/' in href:
                    # Already has /series/, just make absolute
                    href = urljoin('https://asuracomic.net', href)
                elif href.startswith('/'):
                    # Starts with / but no /series/ - prepend base
                    href = urljoin('https://asuracomic.net', href)
                else:
                    # Relative path like "nano-machine-ac59e269/chapter/1"
                    href = f"https://asuracomic.net/series/{href}"
            
            # Skip duplicates
            if href in seen_urls:
                continue
            seen_urls.add(href)
            
            # Get chapter number from URL
            chap_match = re.search(r'/chapter/(\d+)', href)
            if chap_match:
                chap_num = chap_match.group(1)
                chapters.append({
                    'title': f"Chapter {chap_num}",
                    'url': href
                })
        
        # Sort chapters by number and reverse to get oldest first
        chapters.sort(key=lambda x: int(re.search(r'Chapter (\d+)', x['title']).group(1)) if re.search(r'Chapter (\d+)', x['title']) else 0)
        info['chapters'] = chapters
        
        logger.info(f"AsuraComic found {len(info['chapters'])} chapters for {info['title']}")
        return info
    
    def _parse_mangapill_info(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Parse MangaPill manga page"""
        info = {
            'title': '',
            'cover': '',
            'chapters': [],
            'source': 'MangaPill'
        }
        
        # Title - from h1 or div with manga name
        title_elem = soup.select_one('h1, div.font-bold.text-lg')
        if title_elem:
            info['title'] = title_elem.get_text(strip=True)
        
        # Cover image
        cover_elem = soup.select_one('img[src*="cover"], img.rounded')
        if cover_elem:
            info['cover'] = cover_elem.get('src', '')
        
        # Chapters - look for links to /chapters/ with proper chapter path format
        # Valid format: /chapters/ID-NUMBER/manga-name-chapter-X
        chapter_links = soup.select('a[href*="/chapters/"]')
        seen_chapters = set()  # Track by chapter number to avoid duplicates
        chapters = []
        
        for link in chapter_links:
            href = link.get('href', '')
            if not href:
                continue
            
            # Filter: only keep actual chapter links (should have manga slug + chapter in URL)
            # Format: /chapters/2-10001000/one-piece-chapter-1
            if not re.search(r'/chapters/\d+-\d+/', href):
                continue
            
            # Extract chapter number for deduplication
            chap_match = re.search(r'-chapter-(\d+)', href.lower())
            if chap_match:
                chap_num = int(chap_match.group(1))
                if chap_num in seen_chapters:
                    continue
                seen_chapters.add(chap_num)
            
            # Make absolute URL
            if not href.startswith('http'):
                href = urljoin('https://mangapill.com', href)
            
            chap_title = link.get_text(strip=True)
            chapters.append({
                'title': chap_title,
                'url': href,
                '_num': chap_num if chap_match else 0
            })
        
        # Sort by chapter number (oldest first) and remove the temp _num field
        chapters.sort(key=lambda x: x.get('_num', 0))
        for ch in chapters:
            ch.pop('_num', None)
        
        info['chapters'] = chapters
        logger.info(f"MangaPill found {len(info['chapters'])} chapters for {info['title']}")
        return info
    
    def _parse_madara_info(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Parse Madara WordPress theme manga pages (manhuaus, mangadass, novamanga, etc.)"""
        info = {
            'title': '',
            'cover': '',
            'chapters': [],
            'source': self._get_site_name(url)
        }
        
        # Parse base URL for making absolute URLs
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        # Title - Madara theme uses .post-title h1 or h1
        title_elem = soup.select_one('.post-title h1, .post-title, h1')
        if title_elem:
            # Remove badges/spans from title
            for badge in title_elem.select('span.manga-title-badges, span.hot, span.new'):
                badge.decompose()
            info['title'] = title_elem.get_text(strip=True)
        
        # Cover image - Madara uses .summary_image img or .tab-summary img
        cover_elem = soup.select_one('.summary_image img, .tab-summary img, .thumb img, img.wp-post-image')
        if cover_elem:
            info['cover'] = cover_elem.get('data-src') or cover_elem.get('src', '')
        
        # Chapters - Madara uses li.wp-manga-chapter with links
        chapter_links = soup.select('li.wp-manga-chapter a, .listing-chapters_wrap a, .version-chap a, ul.main a')
        seen_urls = set()
        chapters = []
        
        for link in chapter_links:
            href = link.get('href', '')
            if not href or href in seen_urls:
                continue
            
            # Skip non-chapter links (comments, etc.)
            if '#' in href or 'comment' in href.lower():
                continue
            
            seen_urls.add(href)
            
            # Make absolute URL
            if not href.startswith('http'):
                href = urljoin(base_url, href)
            
            chap_title = link.get_text(strip=True)
            chapters.append({
                'title': chap_title,
                'url': href
            })
        
        # Reverse to get oldest first (Madara shows newest first)
        chapters.reverse()
        
        info['chapters'] = chapters
        logger.info(f"Madara theme found {len(info['chapters'])} chapters for {info['title']}")
        return info
    
    def _parse_mangabuddy_info(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Parse MangaBuddy manga page"""
        info = {
            'title': '',
            'cover': '',
            'chapters': [],
            'source': 'MangaBuddy'
        }
        
        # Extract manga slug from URL for filtering chapters
        manga_slug = url.rstrip('/').split('/')[-1]
        # Clean slug - remove common suffixes like -manga
        clean_slug = re.sub(r'-manga$', '', manga_slug)
        
        logger.info(f"MangaBuddy parsing: url={url}, slug={manga_slug}, clean_slug={clean_slug}")
        
        # Title - MangaBuddy uses h1 for main title
        title_elem = soup.select_one('h1')
        if title_elem:
            info['title'] = title_elem.get_text(strip=True)
        
        # Cover image
        cover_elem = soup.select_one('.detail-cover img, .img-wrapper img, .thumb img, img.detail-img')
        if cover_elem:
            info['cover'] = cover_elem.get('data-src') or cover_elem.get('src', '')
        
        # Chapters - try multiple selectors
        # Method 1: Links with /manga-slug/chapter pattern
        chapter_links = soup.select(f'a[href*="/{manga_slug}/chapter"], a[href*="/{clean_slug}/chapter"]')
        
        # Method 2: If no chapters found, try broader chapter link selectors
        if not chapter_links:
            chapter_links = soup.select('a[href*="/chapter-"], a[href*="/chapter/"]')
            logger.info(f"MangaBuddy fallback: found {len(chapter_links)} potential chapter links")
        
        # Method 3: Look for chapter list container
        if not chapter_links:
            chapter_container = soup.select_one('.chapter-list, .chapters, #chapter-list, .list-chapters')
            if chapter_container:
                chapter_links = chapter_container.select('a[href]')
                logger.info(f"MangaBuddy container: found {len(chapter_links)} links in container")
        
        seen_urls = set()
        chapters = []
        
        for link in chapter_links:
            href = link.get('href', '')
            if not href or href in seen_urls:
                continue
            
            # Skip "Read Now" and other navigation links
            text = link.get_text(strip=True).lower()
            if any(skip in text for skip in ['read now', 'read last', 'latest', 'notice']):
                continue
            
            # Must contain 'chapter' in URL
            if 'chapter' not in href.lower():
                continue
            
            seen_urls.add(href)
            
            # Make absolute URL
            if not href.startswith('http'):
                href = urljoin('https://mangabuddy.com', href)
            
            chap_title = link.get_text(strip=True)
            # Clean chapter title (remove duplicate time info like "a year ago")
            chap_title = re.sub(r'(\d+)(a\s*\w+\s*ago)', r'\1', chap_title)
            
            chapters.append({
                'title': chap_title,
                'url': href
            })
        
        # Reverse to get oldest first
        chapters.reverse()
        
        info['chapters'] = chapters
        logger.info(f"MangaBuddy found {len(info['chapters'])} chapters for {info['title']}")
        return info
    
    def _parse_generic_manga_info(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Generic manga page parser"""
        info = {
            'title': '',
            'cover': '',
            'chapters': [],
            'source': self._get_site_name(url)
        }
        
        # Extract manga slug from URL for filtering chapters
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]
        manga_slug = path_parts[-1] if path_parts else ''
        
        # Title
        for selector in ['h1', '.manga-title', '.title', '.name', '.post-title h1']:
            elem = soup.select_one(selector)
            if elem:
                info['title'] = elem.get_text(strip=True)
                break
        
        # Cover
        for selector in ['img.cover', 'img.manga-cover', '.cover img', '.thumb img', '.summary_image img']:
            elem = soup.select_one(selector)
            if elem:
                src = elem.get('data-src') or elem.get('src', '')
                if src:
                    info['cover'] = src
                    break
        
        # Chapters - look for links with chapter in URL
        chapter_links = soup.select('a[href*="chapter"], a[href*="ch-"], .chapter-list a')
        seen_urls = set()
        chapters = []
        
        for link in chapter_links:
            href = link.get('href', '')
            if not href or href in seen_urls:
                continue
            
            # Filter: only include chapters for this manga (if slug is in URL)
            if manga_slug and manga_slug not in href:
                continue
            
            # Skip navigation links
            text = link.get_text(strip=True).lower()
            if any(skip in text for skip in ['read first', 'read last', 'read now', 'latest', 'newest']):
                continue
            
            seen_urls.add(href)
            chapters.append({
                'title': link.get_text(strip=True),
                'url': urljoin(url, href)
            })
        
        # Reverse to get oldest first (most sites show newest first)
        chapters.reverse()
        
        info['chapters'] = chapters
        return info
    
    def get_chapter_images(self, chapter_url: str, chapter_id: str = None) -> List[str]:
        """Get all image URLs for a chapter"""
        site_type = self._detect_site_type(chapter_url)
        
        if site_type == 'mangadex':
            return self._get_mangadex_images(chapter_id or chapter_url)
        elif site_type == 'manganato':
            return self._get_manganato_images(chapter_url)
        elif site_type == 'mangakakalot':
            return self._get_mangakakalot_images(chapter_url)
        elif site_type == 'mangapark':
            return self._get_mangapark_images(chapter_url)
        elif site_type == 'mangapill':
            return self._get_mangapill_images(chapter_url)
        elif site_type == 'asura':
            return self._get_asura_images(chapter_url)
        elif site_type == 'madara':
            return self._get_madara_images(chapter_url)
        elif site_type == 'mangabuddy':
            return self._get_mangabuddy_images(chapter_url)
        else:
            return self._get_generic_images(chapter_url)
    
    def _get_mangadex_images(self, chapter_id_or_url: str) -> List[str]:
        """Get MangaDex chapter images via API"""
        images = []
        
        # Extract chapter ID if URL
        if 'mangadex.org' in chapter_id_or_url:
            match = re.search(r'/chapter/([a-f0-9-]+)', chapter_id_or_url)
            if match:
                chapter_id = match.group(1)
            else:
                return images
        else:
            chapter_id = chapter_id_or_url
        
        try:
            # Get chapter server
            api_url = f"https://api.mangadex.org/at-home/server/{chapter_id}"
            resp = self.session.get(api_url, headers=self._get_random_headers(), timeout=10)
            if resp.status_code != 200:
                return images
            
            data = resp.json()
            base_url = data.get('baseUrl', '')
            chapter_data = data.get('chapter', {})
            chapter_hash = chapter_data.get('hash', '')
            page_files = chapter_data.get('data', [])  # High quality
            
            for page_file in page_files:
                img_url = f"{base_url}/data/{chapter_hash}/{page_file}"
                images.append(img_url)
                
        except Exception as e:
            logger.error(f"MangaDex images error: {e}")
        
        return images
    
    def _get_manganato_images(self, chapter_url: str) -> List[str]:
        """Get Manganato chapter images"""
        images = []
        
        try:
            resp = self.session.get(chapter_url, headers=self._get_random_headers(), timeout=15)
            if resp.status_code != 200:
                return images
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            img_elements = soup.select('.container-chapter-reader img, .navi-change-chapter-btn-mobile + div img')
            
            for img in img_elements:
                src = img.get('src', '') or img.get('data-src', '')
                if src and not 'logo' in src.lower():
                    images.append(src)
                    
        except Exception as e:
            logger.error(f"Manganato images error: {e}")
        
        return images
    
    def _get_mangakakalot_images(self, chapter_url: str) -> List[str]:
        """Get Mangakakalot chapter images - legacy domains only (mangakakalot.to removed)"""
        images = []
        
        try:
            headers = self._get_random_headers()
            resp = self.session.get(chapter_url, headers=headers, timeout=20)
            if resp.status_code != 200:
                return images
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Standard selectors for mangakakalot.com/tv
            img_elements = soup.select('.vung-doc img, #vungdoc img, .container-chapter-reader img, .reading-content img')
            
            for img in img_elements:
                src = img.get('src', '') or img.get('data-src', '')
                if src and 'logo' not in src.lower():
                    images.append(src)
                    
        except Exception as e:
            logger.error(f"Mangakakalot images error: {e}")
        
        return images
    
    def _get_mangapark_images(self, chapter_url: str) -> List[str]:
        """Get MangaPark chapter images using GraphQL API"""
        images = []
        
        try:
            # Extract chapter ID from URL
            # URL format: /title/123456-en-manga/456789-chapter-1
            chapter_id = None
            match = re.search(r'/(\d+)-chapter|/(\d+)$|/title/\d+[^/]*/(\d+)', chapter_url)
            if match:
                chapter_id = match.group(1) or match.group(2) or match.group(3)
            
            if not chapter_id:
                # Try fetching page and looking for ID
                resp = self.session.get(chapter_url, headers=self._get_random_headers(), timeout=15)
                if resp.status_code == 200:
                    page_text = resp.text
                    id_match = re.search(r'"chapterId"[:\s]*"?(\d+)"?', page_text)
                    if id_match:
                        chapter_id = id_match.group(1)
            
            if not chapter_id:
                logger.error(f"Could not extract MangaPark chapter ID from {chapter_url}")
                return images
            
            logger.info(f"MangaPark chapter ID: {chapter_id}")
            
            # Use GraphQL API to get chapter images
            graphql_url = "https://mangapark.net/apo/"
            
            query = """
            query get_chapterNode($id: ID!) {
                get_chapterNode(id: $id) {
                    data {
                        imageFile {
                            urlList
                        }
                    }
                }
            }
            """
            
            payload = {
                "query": query,
                "variables": {"id": chapter_id}
            }
            
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Origin': 'https://mangapark.net',
                'Referer': chapter_url,
                'User-Agent': random.choice(self.USER_AGENTS)
            }
            
            cookies = {'__nsfw': '1'}
            
            resp = self.session.post(graphql_url, json=payload, headers=headers, cookies=cookies, timeout=15)
            
            if resp.status_code == 200:
                data = resp.json()
                chapter_data = data.get('data', {}).get('get_chapterNode', {}).get('data', {})
                image_file = chapter_data.get('imageFile', {})
                url_list = image_file.get('urlList', [])
                
                if url_list:
                    images = url_list
                    logger.info(f"MangaPark GraphQL found {len(images)} images")
            else:
                logger.error(f"MangaPark image API error: {resp.status_code}")
                # Fallback to HTML parsing
                resp = self.session.get(chapter_url, headers=self._get_random_headers(), timeout=15)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    img_elements = soup.select('img[src*="mpcdn"], img[data-src*="mpcdn"]')
                    for img in img_elements:
                        src = img.get('src', '') or img.get('data-src', '')
                        if src:
                            images.append(src)
                    
        except Exception as e:
            logger.error(f"MangaPark images error: {e}")
        
        return images
    
    def _get_mangapill_images(self, chapter_url: str) -> List[str]:
        """Get MangaPill chapter images"""
        images = []
        
        try:
            resp = self.session.get(chapter_url, headers=self._get_random_headers(), timeout=20)
            if resp.status_code != 200:
                return images
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # MangaPill uses img tags with src pointing to CDN
            # Look for all images in the chapter container
            img_elements = soup.select('img')
            
            for img in img_elements:
                src = img.get('src', '') or img.get('data-src', '')
                # Filter for chapter images (usually from CDN with manga path)
                if src and ('mangap' in src or 'cdn' in src) and not any(x in src.lower() for x in ['logo', 'icon', 'avatar', 'header']):
                    images.append(src)
            
            logger.info(f"MangaPill found {len(images)} images for {chapter_url}")
                    
        except Exception as e:
            logger.error(f"MangaPill images error: {e}")
        
        return images
    
    def _get_asura_images(self, chapter_url: str) -> List[str]:
        """Get AsuraComic chapter images from script data"""
        images = []
        
        try:
            resp = self.session.get(chapter_url, headers=self._get_random_headers(), timeout=20)
            if resp.status_code != 200:
                return images
            
            # AsuraComic embeds image URLs in script tags / JSON data
            # Multiple formats exist:
            # - https://gg.asuracomic.net/storage/media/XXXXX/conversions/RANDOM-optimized.webp
            # - https://gg.asuracomic.net/storage/media/XXXXX/conversions/RANDOM-optimized.jpg
            # The media ID can be digits, the conversion ID can have letters, numbers, hyphens
            
            # Broad pattern to match all CDN image URLs (exclude thumb-small variants)
            all_urls = re.findall(
                r'https://gg\.asuracomic\.net/storage/media/\d+/conversions/[a-zA-Z0-9_-]+-optimized\.(?:webp|jpg|png)',
                resp.text
            )
            
            # Remove duplicates while preserving order (first occurrence)
            seen = set()
            unique_urls = []
            for url in all_urls:
                # Skip thumbnail variants
                if 'thumb' in url.lower():
                    continue
                if url not in seen:
                    seen.add(url)
                    unique_urls.append(url)
            
            images = unique_urls
            
            # If regex failed, try fallback: look for img tags with asuracomic CDN
            if not images:
                soup = BeautifulSoup(resp.content, 'html.parser')
                for img in soup.select('img[src*="gg.asuracomic.net"]'):
                    src = img.get('src', '')
                    if src and 'thumb' not in src.lower() and 'storage/media' in src:
                        images.append(src)
            
            logger.info(f"AsuraComic found {len(images)} images for {chapter_url}")
                    
        except Exception as e:
            logger.error(f"AsuraComic images error: {e}")
        
        return images
    
    def _get_madara_images(self, chapter_url: str) -> List[str]:
        """Get chapter images from Madara WordPress theme sites"""
        images = []
        
        try:
            resp = self.session.get(chapter_url, headers=self._get_random_headers(), timeout=20)
            if resp.status_code != 200:
                logger.warning(f"Madara chapter returned {resp.status_code}")
                return images
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Madara theme uses .reading-content for chapter images
            # Also try .page-break img which is common variant
            selectors = [
                '.reading-content img',
                '.page-break img',
                '#chapter-content img',
                '.entry-content img',
                '.chapter-content img',
                'img.wp-manga-chapter-img',
            ]
            
            for selector in selectors:
                img_elements = soup.select(selector)
                if img_elements:
                    for img in img_elements:
                        # Get src from multiple attributes (lazy loading)
                        src = (img.get('data-src') or img.get('data-lazy-src') or 
                               img.get('data-cfsrc') or img.get('src', ''))
                        
                        if not src:
                            continue
                        
                        # Filter out ads, logos, placeholders
                        if any(x in src.lower() for x in ['logo', 'banner', 'ad-', 'ads/', 'loading', 'placeholder']):
                            continue
                        
                        # Make absolute URL
                        if not src.startswith('http'):
                            src = urljoin(chapter_url, src)
                        
                        images.append(src)
                    
                    if images:
                        break
            
            # Deduplicate while preserving order
            seen = set()
            unique = []
            for img in images:
                if img not in seen:
                    seen.add(img)
                    unique.append(img)
            images = unique
            
            logger.info(f"Madara found {len(images)} images for {chapter_url}")
                    
        except Exception as e:
            logger.error(f"Madara images error: {e}")
        
        return images
    
    def _get_mangabuddy_images(self, chapter_url: str) -> List[str]:
        """Get MangaBuddy chapter images"""
        images = []
        
        try:
            resp = self.session.get(chapter_url, headers=self._get_random_headers(), timeout=20)
            if resp.status_code != 200:
                return images
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Primary method: Extract from chapImages JavaScript variable
            # MangaBuddy stores all images as comma-separated URLs in a JS variable
            scripts = soup.select('script')
            for script in scripts:
                text = script.get_text() if script.string is None else script.string
                if text and 'chapImages' in text:
                    # Extract the chapImages variable value
                    match = re.search(r"var\s+chapImages\s*=\s*'([^']+)'", text)
                    if match:
                        image_urls = match.group(1).split(',')
                        for url in image_urls:
                            url = url.strip()
                            if url and ('mbcdn' in url or '/res/manga/' in url):
                                images.append(url)
                        if images:
                            logger.info(f"MangaBuddy extracted {len(images)} images from chapImages JS variable")
                            break
            
            # Fallback: look for img tags with CDN domains
            if not images:
                img_elements = soup.select('img')
                for img in img_elements:
                    src = img.get('data-src') or img.get('src', '')
                    if src and ('mbcdn' in src or '/res/manga/' in src):
                        images.append(src)
            
            # Deduplicate while preserving order
            seen = set()
            unique = []
            for img in images:
                if img not in seen:
                    seen.add(img)
                    unique.append(img)
            images = unique
            
            logger.info(f"MangaBuddy found {len(images)} images for {chapter_url}")
                    
        except Exception as e:
            logger.error(f"MangaBuddy images error: {e}")
        
        return images
    
    def _get_generic_images(self, chapter_url: str) -> List[str]:
        """Generic chapter image extraction"""
        images = []
        
        try:
            resp = self.session.get(chapter_url, headers=self._get_random_headers(), timeout=15)
            if resp.status_code != 200:
                return images
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Method 1: Look for reader container images
            for selector in ['.reader-area img', '.chapter-content img', '#chapter-container img', 
                           '.page-chapter img', 'img[id*="page"]', 'img[class*="chapter"]']:
                img_elements = soup.select(selector)
                if img_elements:
                    for img in img_elements:
                        src = img.get('src', '') or img.get('data-src', '')
                        if src and not any(x in src.lower() for x in ['logo', 'banner', 'ad']):
                            images.append(urljoin(chapter_url, src))
                    if images:
                        break
            
            # Method 2: Look for CDN images with manga/chapter patterns
            if not images:
                all_imgs = soup.select('img')
                for img in all_imgs:
                    src = img.get('src', '') or img.get('data-src', '')
                    if not src:
                        continue
                    src_lower = src.lower()
                    # Skip common non-content images (check path not full URL to avoid false positives)
                    path_only = src_lower.split('?')[0].split('/')[-1]  # Get filename part
                    if any(x in path_only for x in ['logo', 'banner', 'icon', 'loading', 'avatar']):
                        continue
                    # Include images from manga CDNs or with upload/chapter paths
                    if any(x in src_lower for x in ['/uploads/', '/manga/', '/chapter/', 'cdn', 'img01.', 'img02.', 'img03.']):
                        images.append(urljoin(chapter_url, src))
            
            logger.info(f"Generic found {len(images)} images for {chapter_url}")
                        
        except Exception as e:
            logger.error(f"Generic images error: {e}")
        
        return images
    
    def download_chapter_images(self, chapter: Dict[str, Any], output_dir: str, 
                                 progress_callback: Optional[Callable] = None,
                                 max_retries: int = 3) -> List[str]:
        """Download all images for a chapter with retry logic"""
        if self.cancelled:
            return []
        
        chapter_url = chapter.get('url', '')
        chapter_id = chapter.get('chapter_id', '')
        chapter_title = chapter.get('title', 'Chapter')
        
        # Get image URLs
        image_urls = self.get_chapter_images(chapter_url, chapter_id)
        
        if not image_urls:
            logger.warning(f"No images found for {chapter_title}")
            return []
        
        downloaded = []
        failed_images = []
        
        # Create chapter directory
        safe_title = re.sub(r'[^\w\s-]', '', chapter_title)[:50]
        chapter_dir = os.path.join(output_dir, safe_title)
        os.makedirs(chapter_dir, exist_ok=True)
        
        def download_single_image(i: int, img_url: str, retry_count: int = 0) -> Optional[str]:
            """Download a single image with retries - uses Playwright for protected sites"""
            if self.cancelled:
                return None
            
            use_stealth = is_protected_manga_site(chapter_url) and self.use_playwright
            
            try:
                headers = self._get_random_headers()
                # Use chapter page URL as referer (required for hotlink protection)
                headers['Referer'] = chapter_url
                # Also set Accept header for images
                headers['Accept'] = 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8'
                
                if retry_count > 0:
                    self._random_delay(1.0, 2.0)
                
                img_data = None
                
                if use_stealth and retry_count >= 1:
                    try:
                        from playwright_scraper import fetch_image_stealth
                        img_data = run_pw_async(fetch_image_stealth(img_url, headers))
                    except Exception as e:
                        logger.debug(f"Playwright image download failed: {e}")
                
                if not img_data:
                    resp = self.session.get(img_url, headers=headers, timeout=30)
                    if resp.status_code == 200 and len(resp.content) > 1000:
                        img_data = resp.content
                
                if img_data and len(img_data) > 1000:
                    ext = '.jpg'
                    if img_data[:8].startswith(b'\x89PNG'):
                        ext = '.png'
                    elif img_data[:4] == b'RIFF' and img_data[8:12] == b'WEBP':
                        ext = '.webp'
                    
                    filename = f"{i+1:03d}{ext}"
                    filepath = os.path.join(chapter_dir, filename)
                    
                    with open(filepath, 'wb') as f:
                        f.write(img_data)
                    
                    return filepath
                elif retry_count < max_retries:
                    logger.info(f"Retrying image {i+1} (attempt {retry_count + 2}/{max_retries + 1})")
                    return download_single_image(i, img_url, retry_count + 1)
                    
            except Exception as e:
                if retry_count < max_retries:
                    logger.info(f"Retrying image {i+1} after error: {e}")
                    return download_single_image(i, img_url, retry_count + 1)
                logger.warning(f"Failed to download image {i+1} after {max_retries + 1} attempts: {e}")
            
            return None
        
        # Download all images with retry
        total_images = len(image_urls)
        last_progress_update = 0
        
        for i, img_url in enumerate(image_urls):
            if self.cancelled:
                break
            
            filepath = download_single_image(i, img_url)
            if filepath:
                downloaded.append(filepath)
            else:
                failed_images.append((i, img_url))
            
            # Send progress updates every 5 images or at key milestones
            current_count = len(downloaded)
            if progress_callback and (current_count - last_progress_update >= 5 or i == total_images - 1):
                progress_callback(f"Downloading {current_count}/{total_images} images for {chapter_title}")
                last_progress_update = current_count
            
            self._random_delay(0.1, 0.3)
        
        # Final retry pass for any remaining failed images
        if failed_images and not self.cancelled:
            logger.info(f"Retrying {len(failed_images)} failed images for {chapter_title}...")
            self._random_delay(2.0, 4.0)  # Longer pause before final retry
            
            for i, img_url in failed_images:
                filepath = download_single_image(i, img_url, retry_count=max_retries - 1)
                if filepath:
                    downloaded.append(filepath)
        
        # Sort by filename to maintain order
        downloaded.sort()
        
        # Send final completion message
        if progress_callback:
            total = len(image_urls)
            success = len(downloaded)
            if success == total:
                progress_callback(f"Downloaded all {success} images for {chapter_title}")
            else:
                progress_callback(f"Downloaded {success}/{total} images for {chapter_title}")
        
        return downloaded
    
    def download_manga(self, manga_url: str, chapter_start: int, chapter_end: int,
                       progress_callback: Optional[Callable] = None,
                       max_workers: int = 3) -> Dict[str, Any]:
        """Download multiple chapters of a manga"""
        self.reset()
        
        if progress_callback:
            progress_callback("Fetching manga info...")
        
        manga_info = self.get_manga_info(manga_url)
        
        if 'error' in manga_info:
            return {'error': manga_info['error']}
        
        if not manga_info.get('chapters'):
            return {'error': 'No chapters found'}
        
        title = manga_info.get('title', 'Manga')
        all_chapters = manga_info.get('chapters', [])
        
        # Select chapter range
        total_chapters = len(all_chapters)
        start_idx = max(0, chapter_start - 1)
        end_idx = min(total_chapters, chapter_end)
        
        selected_chapters = all_chapters[start_idx:end_idx]
        
        if not selected_chapters:
            return {'error': 'No chapters in selected range'}
        
        if progress_callback:
            progress_callback(f"Downloading {len(selected_chapters)} chapters...")
        
        # Create temp directory
        safe_title = re.sub(r'[^\w\s-]', '', title)[:50]
        output_dir = f"manga_{safe_title}_{chapter_start}-{chapter_end}"
        os.makedirs(output_dir, exist_ok=True)
        
        downloaded_chapters = []
        
        for i, chapter in enumerate(selected_chapters):
            if self.cancelled:
                break
            
            if progress_callback:
                progress_callback(f"Downloading chapter {i+1}/{len(selected_chapters)}: {chapter.get('title', '')}")
            
            images = self.download_chapter_images(chapter, output_dir, progress_callback)
            
            if images:
                downloaded_chapters.append({
                    'title': chapter.get('title', f'Chapter {i+1}'),
                    'images': images
                })
            
            self._random_delay(0.3, 0.8)
        
        return {
            'title': title,
            'cover': manga_info.get('cover', ''),
            'chapters': downloaded_chapters,
            'output_dir': output_dir,
            'source': manga_info.get('source', '')
        }

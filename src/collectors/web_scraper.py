"""
Web scraper module for collecting CEPs from codigo-postal.org
"""

import re
import time
import csv
import threading
from pathlib import Path
from typing import List, Set, Optional
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from src.utils.logger import setup_logger


class WebScraper:
    """
    Web scraper for collecting CEPs from codigo-postal.org
    Navigates neighborhood by neighborhood to collect exactly 10,000 CEPs
    """

    def __init__(
        self,
        base_url: str = "https://codigo-postal.org/pt-br/brasil/sp/sao-paulo/",
        delay: float = 2.0,
        max_ceps: int = 10000,
        timeout: int = 30,
        max_workers: int = 3,
        parallel: bool = True
    ):
        """
        Initialize the web scraper.

        Args:
            base_url: Base URL for scraping
            delay: Delay between requests in seconds (reduced when parallel)
            max_ceps: Maximum number of CEPs to collect
            timeout: Request timeout in seconds
            max_workers: Maximum number of parallel workers (only if parallel=True)
            parallel: Enable parallel processing (default: True)
        """
        self.base_url = base_url
        self.delay = delay
        self.max_ceps = max_ceps
        self.timeout = timeout
        self.max_workers = max_workers
        self.parallel = parallel
        self.logger = setup_logger(name="web_scraper")
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.collected_ceps: Set[str] = set()
        self._ceps_lock = threading.Lock()  # Thread-safe access to collected_ceps
        self._visited_lock = threading.Lock()  # Thread-safe access to visited_urls and urls_to_visit

    def _extract_cep(self, text: str) -> List[str]:
        """
        Extract CEPs from text using regex pattern.

        Args:
            text: Text to search for CEPs

        Returns:
            List of found CEPs (formatted as XXXXX-XXX)
        """
        # CEP pattern: 5 digits, optional hyphen, 3 digits
        cep_pattern = r'\b\d{5}-?\d{3}\b'
        matches = re.findall(cep_pattern, text)
        
        # Normalize CEPs (add hyphen if missing)
        normalized_ceps = []
        for cep in matches:
            cep = cep.replace('-', '')
            if len(cep) == 8:
                normalized_ceps.append(f"{cep[:5]}-{cep[5:]}")
        
        return normalized_ceps

    def _is_valid_cep(self, cep: str) -> bool:
        """
        Validate CEP format and ensure it's a valid São Paulo CEP.
        São Paulo CEPs must start with 0 (zero).

        Args:
            cep: CEP string to validate (format: XXXXX-XXX)

        Returns:
            True if valid São Paulo CEP, False otherwise
        """
        # First check format: 5 digits - 3 digits
        pattern = r'^\d{5}-\d{3}$'
        if not re.match(pattern, cep):
            return False
        
        # São Paulo CEPs must start with 0 (zero)
        if not cep.startswith('0'):
            return False
        
        return True

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a web page.

        Args:
            url: URL to fetch

        Returns:
            BeautifulSoup object or None if error
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None

    def _get_neighborhoods(self, soup: BeautifulSoup) -> List[str]:
        """
        Extract neighborhood links from the page.
        Focuses on actual neighborhood pages, avoiding individual CEP pages and logradouros.
        Optimized based on actual HTML structure.

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            List of neighborhood URLs
        """
        neighborhoods = []
        
        # Method 1: Look for list structure (ul with id="ul_list") - common in bairro listing pages
        ul_list = soup.find('ul', id='ul_list')
        if ul_list:
            for li in ul_list.find_all('li'):
                link = li.find('a', href=True)
                if link:
                    href = link.get('href', '')
                    if self._is_valid_neighborhood_link(href):
                        full_url = urljoin(self.base_url, href)
                        if self._is_same_domain(full_url):
                            neighborhoods.append(full_url)
        
        # Method 2: Look for all links that match neighborhood pattern
        links = soup.find_all('a', href=True)
        
        for link in links:
            href = link.get('href', '')
            
            if not href:
                continue
            
            if self._is_valid_neighborhood_link(href):
                full_url = urljoin(self.base_url, href)
                if self._is_same_domain(full_url):
                    neighborhoods.append(full_url)
        
        return list(set(neighborhoods))  # Remove duplicates
    
    def _is_valid_neighborhood_link(self, href: str) -> bool:
        """
        Check if a link is a valid neighborhood page link.
        
        Args:
            href: Link href to check (can be relative or absolute URL)
            
        Returns:
            True if valid neighborhood link, False otherwise
        """
        if not href:
            return False
        
        href_lower = href.lower()
        
        # Skip individual CEP pages (pattern: /cep/XXXXX-XXX/)
        if '/cep/' in href_lower:
            return False
        
        # Skip logradouro pages (pattern: /logradouro/...)
        if '/logradouro/' in href_lower:
            return False
        
        # Skip blog and other non-neighborhood pages
        if any(skip in href_lower for skip in ['/blog/', '/sobre/', '/contato/', '/meu-cep/']):
            return False
        
        # Skip if it's just the base city page (sao-paulo/ with no neighborhood)
        # Pattern: /pt-br/brasil/sp/sao-paulo/ (exactly, no additional path)
        # Also skip if it ends with /sao-paulo// (double slash)
        href_clean = href_lower.rstrip('/').rstrip('/')
        if href_clean.endswith('/sao-paulo') or href_clean.endswith('sao-paulo/'):
            return False
        
        # Extract path parts (handle both relative and absolute URLs)
        # Remove protocol and domain if present
        path = href
        if '://' in path:
            # Absolute URL - extract path
            path = '/' + '/'.join(path.split('://', 1)[1].split('/', 1)[1:]) if '/' in path.split('://', 1)[1] else '/'
        elif not path.startswith('/'):
            # Relative URL - make it absolute for parsing
            path = '/' + path
        
        path_parts = [p for p in path.split('/') if p]
        
        # Valid neighborhood pattern: /pt-br/brasil/sp/sao-paulo/{bairro-name}/
        # Must have a neighborhood name after sao-paulo
        if '/sao-paulo/' not in href_lower:
            return False
        
        # Find the index of 'sao-paulo' in path parts
        try:
            sp_index = path_parts.index('sao-paulo')
            # There should be at least one more part after sao-paulo (the neighborhood name)
            if len(path_parts) <= sp_index + 1:
                return False
            
            # Check that the part after sao-paulo is not empty and looks like a neighborhood name
            neighborhood_name = path_parts[sp_index + 1]
            if not neighborhood_name or neighborhood_name in ['', 'sp']:
                return False
        except ValueError:
            return False
        
        # Additional validation: should have pt-br or brasil in path
        if 'pt-br' not in path_parts and 'brasil' not in path_parts:
            return False
        
        return True
    
    def _is_same_domain(self, url: str) -> bool:
        """
        Check if URL is from the same domain as base_url.
        
        Args:
            url: URL to check
            
        Returns:
            True if same domain, False otherwise
        """
        try:
            return urlparse(url).netloc == urlparse(self.base_url).netloc
        except Exception:
            return False

    def _collect_ceps_from_page(self, soup: BeautifulSoup) -> Set[str]:
        """
        Extract CEPs from a page.
        Optimized to extract from table structure found in neighborhood pages.

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            Set of CEPs found on the page
        """
        ceps = set()
        
        # Method 1: Extract from table structure (most efficient for neighborhood pages)
        # Look for table with id="ul_list" and tbody id="tbody_results"
        table = soup.find('table', id='ul_list')
        if table:
            tbody = table.find('tbody', id='tbody_results')
            if tbody:
                # Extract CEPs from first column (td) which contains links
                for tr in tbody.find_all('tr'):
                    first_td = tr.find('td')
                    if first_td:
                        # Check links in first column
                        for link in first_td.find_all('a', href=True):
                            href = link.get('href', '')
                            # Extract CEP from href pattern: /pt-br/brasil/cep/XXXXX-XXX/
                            cep_match = re.search(r'/cep/(\d{5}-\d{3})/', href)
                            if cep_match:
                                cep = cep_match.group(1)
                                if self._is_valid_cep(cep):
                                    ceps.add(cep)
                            
                            # Also check link text
                            link_text = link.get_text(strip=True)
                            found_ceps = self._extract_cep(link_text)
                            for cep in found_ceps:
                                if self._is_valid_cep(cep):
                                    ceps.add(cep)
        
        # Method 2: Extract from all links with CEP pattern in href
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            cep_match = re.search(r'/cep/(\d{5}-\d{3})/', href)
            if cep_match:
                cep = cep_match.group(1)
                if self._is_valid_cep(cep):
                    ceps.add(cep)
        
        # Method 3: Fallback - extract from text content (for pages without table structure)
        if not ceps:
            page_text = soup.get_text()
            found_ceps = self._extract_cep(page_text)
            for cep in found_ceps:
                if self._is_valid_cep(cep):
                    ceps.add(cep)
        
        return ceps

    def _save_to_csv(self, output_path: Path) -> None:
        """
        Save collected CEPs to CSV file.
        CEPs are saved without hyphens (numbers only).

        Args:
            output_path: Path to output CSV file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        sorted_ceps = sorted(self.collected_ceps)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['cep'])  # Header
            for cep in sorted_ceps:
                # Save CEP without hyphen (numbers only)
                cep_numbers = cep.replace('-', '')
                writer.writerow([cep_numbers])
        
        self.logger.info(f"Saved {len(sorted_ceps)} CEPs to {output_path}")

    def _process_url(self, url: str, visited_urls: Set[str], urls_to_visit: List[str]) -> Optional[Set[str]]:
        """
        Process a single URL and return new CEPs found.
        Thread-safe method for parallel processing.

        Args:
            url: URL to process
            visited_urls: Set of visited URLs (thread-safe)
            urls_to_visit: List of URLs to visit (thread-safe)

        Returns:
            Set of new CEPs found, or None if error
        """
        # Check if already visited (thread-safe)
        with self._visited_lock:
            if url in visited_urls:
                return None
            visited_urls.add(url)
        
        # Create a new session for this thread (thread-safe)
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            response = session.get(url, timeout=self.timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None
        
        # Collect CEPs from page
        page_ceps = self._collect_ceps_from_page(soup)
        
        # Thread-safe update of collected CEPs
        with self._ceps_lock:
            new_ceps = page_ceps - self.collected_ceps
            if new_ceps:
                self.collected_ceps.update(new_ceps)
                self.logger.info(f"Found {len(new_ceps)} new CEPs from {url}. Total: {len(self.collected_ceps)}")
        
        # Get neighborhoods (thread-safe)
        neighborhoods = self._get_neighborhoods(soup)
        if neighborhoods:
            with self._visited_lock:
                added_count = 0
                for neighborhood_url in neighborhoods:
                    if neighborhood_url not in visited_urls and neighborhood_url not in urls_to_visit:
                        urls_to_visit.append(neighborhood_url)
                        added_count += 1
                if added_count > 0:
                    self.logger.debug(f"Added {added_count} new neighborhoods from {url}")
        
        return new_ceps

    def scrape(self, output_path: Path = Path("data/ceps_collected.csv")) -> Path:
        """
        Main scraping method. Collects CEPs neighborhood by neighborhood.
        Supports both sequential and parallel processing.

        Args:
            output_path: Path to save the CSV file

        Returns:
            Path to the created CSV file
        """
        mode = "parallel" if self.parallel else "sequential"
        self.logger.info(f"Starting web scraping ({mode}). Target: {self.max_ceps} CEPs")
        
        if self.parallel:
            return self._scrape_parallel(output_path)
        else:
            return self._scrape_sequential(output_path)

    def _scrape_sequential(self, output_path: Path) -> Path:
        """Sequential scraping (original implementation)"""
        visited_urls: Set[str] = set()
        urls_to_visit: List[str] = [self.base_url]
        pages_visited = 0
        
        while urls_to_visit and len(self.collected_ceps) < self.max_ceps:
            current_url = urls_to_visit.pop(0)
            
            if current_url in visited_urls:
                continue
            
            visited_urls.add(current_url)
            pages_visited += 1
            self.logger.info(f"Page {pages_visited}: Visiting {current_url} (Collected: {len(self.collected_ceps)}/{self.max_ceps})")
            
            soup = self._get_page(current_url)
            if not soup:
                continue
            
            # Collect CEPs from current page
            page_ceps = self._collect_ceps_from_page(soup)
            new_ceps = page_ceps - self.collected_ceps
            
            if new_ceps:
                self.collected_ceps.update(new_ceps)
                self.logger.info(f"Found {len(new_ceps)} new CEPs on this page. Total: {len(self.collected_ceps)}")
            
            # Stop immediately if we've collected enough from this page
            if len(self.collected_ceps) >= self.max_ceps:
                self.logger.info(f"Reached target of {self.max_ceps} CEPs from page {pages_visited}! Stopping navigation.")
                break
            
            # Only look for more neighborhoods if we haven't reached the limit
            if len(self.collected_ceps) < self.max_ceps:
                neighborhoods = self._get_neighborhoods(soup)
                if neighborhoods:
                    self.logger.info(f"Found {len(neighborhoods)} neighborhood links. Queue size: {len(urls_to_visit)}")
                    for neighborhood_url in neighborhoods:
                        if neighborhood_url not in visited_urls and neighborhood_url not in urls_to_visit:
                            urls_to_visit.append(neighborhood_url)
                else:
                    self.logger.warning(f"No neighborhood links found on this page. URL: {current_url}")
            
            # Respect rate limiting
            time.sleep(self.delay)
        
        self.logger.info(f"Scraping summary: Visited {pages_visited} pages, collected {len(self.collected_ceps)} CEPs")
        return self._finalize_scraping(output_path)

    def _scrape_parallel(self, output_path: Path) -> Path:
        """Parallel scraping using ThreadPoolExecutor"""
        visited_urls: Set[str] = set()
        urls_to_visit: List[str] = [self.base_url]
        pages_visited = 0
        active_futures = set()
        
        # Reduced delay for parallel processing (less aggressive)
        parallel_delay = max(0.5, self.delay / self.max_workers)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            while (urls_to_visit or active_futures) and len(self.collected_ceps) < self.max_ceps:
                # Check if we've reached the target
                if len(self.collected_ceps) >= self.max_ceps:
                    break
                
                # Submit new URLs for processing (up to max_workers)
                with self._visited_lock:
                    # Get up to max_workers URLs to process
                    urls_to_process = []
                    available_slots = self.max_workers - len(active_futures)
                    while urls_to_visit and len(urls_to_process) < available_slots:
                        url = urls_to_visit.pop(0)
                        if url not in visited_urls:
                            urls_to_process.append(url)
                    
                    for url in urls_to_process:
                        pages_visited += 1
                        self.logger.info(f"Page {pages_visited}: Submitting {url} (Collected: {len(self.collected_ceps)}/{self.max_ceps})")
                        future = executor.submit(self._process_url, url, visited_urls, urls_to_visit)
                        active_futures.add(future)
                
                # Process completed futures (non-blocking check)
                completed_futures = [f for f in active_futures if f.done()]
                for future in completed_futures:
                    active_futures.discard(future)
                    
                    try:
                        new_ceps = future.result()
                        if new_ceps:
                            self.logger.debug(f"Completed: {len(new_ceps)} new CEPs")
                    except Exception as e:
                        self.logger.error(f"Error processing URL: {e}")
                    
                    # Check if we've reached the target
                    if len(self.collected_ceps) >= self.max_ceps:
                        break
                
                # Small sleep to avoid busy waiting
                if not active_futures and urls_to_visit:
                    time.sleep(0.1)
                elif active_futures:
                    # Wait a bit for futures to complete
                    time.sleep(parallel_delay)
        
        self.logger.info(f"Scraping summary: Visited {pages_visited} pages, collected {len(self.collected_ceps)} CEPs")
        return self._finalize_scraping(output_path)

    def _finalize_scraping(self, output_path: Path) -> Path:
        """Finalize scraping: trim to exact count and save to CSV"""
        # Trim to exactly max_ceps if we collected more
        if len(self.collected_ceps) > self.max_ceps:
            sorted_ceps = sorted(self.collected_ceps)
            self.collected_ceps = set(sorted_ceps[:self.max_ceps])
            self.logger.info(f"Trimmed to exactly {self.max_ceps} CEPs")
        
        # Save to CSV
        self._save_to_csv(output_path)
        
        self.logger.info(f"Scraping completed. Collected {len(self.collected_ceps)} CEPs")
        return output_path

import re
from urllib.parse import urlparse
#student imports:
from urllib.robotparser import RobotFileParser #imported to handle robots.txt parsing | https://docs.python.org/3/library/urllib.robotparser.html#module-urllib.robotparser
import time
from utils.download import download
from urllib.parse import urljoin, urldefrag
from collections import defaultdict


class Scraper:
    # stopwords - we filter these out when counting words so they don't dominate the top 50
    STOPWORDS = {
        "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
        "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
        "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
        "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
        "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
        "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
        "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
        "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
        "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
        "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
        "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
        "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
        "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
        "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
        "they've", "this", "those", "through", "to", "too", "under", "until", "up",
        "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
        "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
        "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
        "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
        "yourself", "yourselves",
    }
     # allowed host suffixes per assignment (*.ics.uci.edu, *.cs.uci.edu, etc.)
    ALLOWED_HOST_SUFFIXES = ("ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu")
    # phrases that indicate a soft 404 (server returns 200 but page is "not found" / error)
    SOFT404_PHRASES = ("not found", "page not found", "404", "error", "page not available")
   

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.permissions_cache = {}
        self.time_visited_cache = {}
        self.visited_urls = set()
        self.max_length_page = ("", 0)
        self.word_frequencies = defaultdict(int)
        # store word-n-gram signatures of pages we've accepted (to avoid near-duplicate content)
        self.page_signatures = set()

    def _tokenize_text(self, text):
        """Split on non-letters and keep only tokens of length >= 2 (for counting and n-grams)."""
        if not text:
            return []
        raw = re.split(r"[^a-zA-Z]+", text.lower())
        return [w for w in raw if len(w) >= 2]

    def _shared_fraction(self, set_a, set_b):
        """What fraction of items (in either set) appear in both? 0 = nothing in common, 1 = identical."""
        if not set_a or not set_b:
            return 0.0
        in_both = len(set_a & set_b)
        in_either = len(set_a | set_b)
        return in_both / in_either if in_either else 0.0

    def _page_too_similar_to_previous(self, tokens):
        """If this page shares too many 4-word phrases with one we've already seen, skip it (near-duplicate)."""
        phrase_length = 4
        # how much overlap (shared phrases / all phrases) counts as "too similar"
        overlap_threshold = 0.85
        if len(tokens) < phrase_length:
            return False
        # each 4-word chunk gets a hash; we compare sets of these hashes
        phrases = frozenset(
            hash(" ".join(tokens[i : i + phrase_length]))
            for i in range(len(tokens) - phrase_length + 1)
        )
        for sig in self.page_signatures:
            if self._shared_fraction(phrases, sig) >= overlap_threshold:
                return True
        self.page_signatures.add(phrases)
        return False

    def _title_suggests_error_page(self, soup):
        """Treat as soft 404 if the title contains a clear error phrase (e.g. 'not found', '404')."""
        if not soup or not soup.title:
            return False
        title = soup.title.get_text().lower().strip()
        if not title:
            return False
        return any(phrase in title for phrase in self.SOFT404_PHRASES)

    def _passes_content_filter(self, tokens):
        """Reject pages that are too short, too stopword-heavy, or too lexically narrow."""
        min_tokens = 15
        max_stopword_ratio = 0.5
        min_unique_ratio = 0.04
        if not tokens or len(tokens) < min_tokens:
            return False
        n = len(tokens)
        stop = sum(1 for t in tokens if t in self.STOPWORDS)
        distinct = len(set(tokens))
        return stop / n <= max_stopword_ratio and distinct / n >= min_unique_ratio

    def scraper(self, url, resp):
        links = self.extract_next_links(url, resp)
        return [link for link in links if self.is_valid(link) and link not in self.visited_urls]

    def extract_next_links(self, url, resp):
        # url: the URL that was used to get the page
        # resp.url: the actual url of the page
        # resp.status: 200 means OK
        # resp.raw_response.content: the page HTML
        # Return list of links (defragmented) from the page.
        # Only process 200 OK; skip 403 Forbidden, 404 Not Found, and any other status.
        links = set()
        if resp.status != 200 or not resp.raw_response or not resp.raw_response.content:
            return list(links)

        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.raw_response.content, "lxml")
        except Exception:
            try:
                soup = BeautifulSoup(resp.raw_response.content, "html.parser")
            except Exception:
                return list(links)

        base_url = resp.url or url

        # remove tags that don't contain meaningful page text 
        for tag in soup.find_all(
            ["meta", "script", "style", "noscript", "object", "embed"]):
            tag.decompose()

        page_text = soup.get_text(separator=" ", strip=True)
        tokens = self._tokenize_text(page_text)

        # filter order: content first (so we don't store signatures for junk), then duplicate check, then soft 404
        if not self._passes_content_filter(tokens):
            return list(links)
        if self._page_too_similar_to_previous(tokens):
            return list(links)
        if self._title_suggests_error_page(soup):
            return list(links)

        # count words on this page: total for Q2 (longest page), non-stopword for Q3 (frequencies)
        total_word_count = len(tokens)
        page_counts = defaultdict(int)
        for t in tokens:
            if t not in self.STOPWORDS:
                page_counts[t] += 1

        for w, cnt in page_counts.items():
            self.word_frequencies[w] += cnt

        clean_url, _ = urldefrag(url)
        self.visited_urls.add(clean_url)
        if total_word_count > self.max_length_page[1]:
            self.max_length_page = (clean_url, total_word_count)

        # collect all links from <a href="...">
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue
            try:
                full = urljoin(base_url, href)
                full, _ = urldefrag(full)
                links.add(full)
            except ValueError:
                continue

        return list(links)

    def is_valid(self, url):
        # Return True if we should crawl this URL, False otherwise.
        try:
            parsed = urlparse(url)
        except TypeError:
            print("TypeError for", url)
            raise

        if parsed.scheme not in ("http", "https"):
            return False
        if "date=" in url.lower():
            return False

        # host = the server name, e.g. www.ics.uci.edu or ics.uci.edu
        host = parsed.netloc.lower()
        if not (host in self.ALLOWED_HOST_SUFFIXES or any(host.endswith("." + s) for s in self.ALLOWED_HOST_SUFFIXES)):
            return False
        # avoid UCI ML repository (very large datasets, per course warning)
        if host == "archive.ics.uci.edu":
            return False

        domain = f"{parsed.scheme}://{parsed.netloc}"
        if domain not in self.permissions_cache:
            self.create_rfp(domain)
        rfp = self.permissions_cache[domain]
        if rfp and not rfp.can_fetch("*", url):
            return False
        # respect crawl delay from robots.txt
        if rfp:
            delay = rfp.crawl_delay('*')
            if delay and time.time() - self.time_visited_cache[domain] < delay:
                time.sleep(delay - (time.time() - self.time_visited_cache[domain]))

        # no pdf, images, etc.
        if re.match( 
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4|mpg"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False

        path_lower = parsed.path.lower()
        query_lower = parsed.query.lower()

        # trap patterns: dates in path (e.g. /2019/02/15/, or YYYYMMDD), repo commit pages, etc.
        if re.search(r"/\d{4}/\d{1,2}/\d{1,2}(/|$)", path_lower):
            return False
        if re.search(r"\d{8}(-\d+)?", path_lower):  # YYYYMMDD or YYYYMMDD-HHMMSS
            return False
        if re.search(r"/-/commit/[0-9a-f]{32,40}", path_lower) or re.search(r"/commit/[0-9a-f]{32,40}", path_lower):
            return False

        # other known traps: calendar/event traps, image galleries, wiki/doku
        if re.search(r"/events?/", path_lower):
            return False
        for fragment in ("/pix", "/bibs/"):
            if fragment in path_lower:
                return False
        if "version=" in query_lower or "from=" in query_lower:
            return False
        if ".php" in path_lower and "http" in query_lower:
            return False
        # skip share links (same content as canonical URL)
        if "share=" in query_lower:
            return False
        # skip login/sign-in pages (no crawlable content, often traps)
        if re.search(r"/(login|signin|sign-in|wp-login|user/login|auth)(/|$|\?)", path_lower):
            return False
        if re.search(r"(^|&)(login|signin|action=login)=", query_lower):
            return False

        return True

    def create_rfp(self, domain):
        """Fetch and parse robots.txt for this domain, cache the result."""
        robots_url = f"{domain}/robots.txt"
        rfp = RobotFileParser(robots_url)
        resp = download(robots_url, self.config, self.logger)
        if resp and resp.status == 200 and resp.raw_response:
            try:
                rfp.parse(resp.raw_response.text.splitlines())
            except Exception:
                pass
        elif resp and resp.status == 404:
            pass
        else:
            rfp = None
        self.permissions_cache[domain] = rfp
        self.time_visited_cache[domain] = time.time()

    def write_report(self, output_filename="crawl_report.txt"):
        """Write Q1–Q4 stats to a file: unique pages, longest page, top 50 words, subdomains."""
        try:
            with open(output_filename, "w", encoding="utf-8", errors="ignore") as f:
                f.write(f"Q1: {len(self.visited_urls)} unique pages\n\n")
                url_long, num_long = self.max_length_page
                f.write(f"Q2: Longest page: {url_long} with {num_long} words\n\n")
                f.write("Top 50 most common words:\n\n")
                top50 = sorted(self.word_frequencies.items(), key=lambda x: x[1], reverse=True)[:50]
                for word, count in top50:
                    f.write(f"{word}: {count}\n")
                f.write("\nSubdomains:\n\n")
                # count how many pages per host (e.g. www.ics.uci.edu, ics.uci.edu)
                host_counts = defaultdict(int)
                for u in self.visited_urls:
                    parsed = urlparse(u)
                    host = parsed.netloc.lower()
                    if re.match(r"^(?:[\w-]+\.)?(ics|cs|informatics|stat)\.uci\.edu$", host):
                        host_counts[host] += 1
                for host, count in sorted(host_counts.items()):
                    f.write(f"{host}, {count}\n")
        except Exception as e:
            if self.logger:
                self.logger.error("Error generating crawl report: %s", e)

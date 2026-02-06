import re
from urllib.parse import urlparse
#student imports:
from urllib.robotparser import RobotFileParser #imported to handle robots.txt parsing | https://docs.python.org/3/library/urllib.robotparser.html#module-urllib.robotparser
import time
from utils.download import download

class Scraper:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.permissions_cache = {} #(str::domain, rfp::RobotFileParser)
        self.time_visited_cache = {} #(str::domain, float::time)

    def scraper(self, url, resp):
        links = self.extract_next_links(url, resp)
        return [link for link in links if self.is_valid(link)]

    def extract_next_links(self, url, resp):
        # Implementation required.
        # url: the URL that was used to get the page
        # resp.url: the actual url of the page
        # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
        # resp.error: when status is not 200, you can check the error here, if needed.
        # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
        #         resp.raw_response.url: the url, again
        #         resp.raw_response.content: the content of the page!
        # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
        return list()

    def is_valid(self, url):
        # Decide whether to crawl this url or not. 
        # If you decide to crawl it, return True; otherwise return False.
        # There are already some conditions that return False.
        
        try:
            parsed = urlparse(url)
            if parsed.scheme not in set(["http", "https"]):
                return False
            
        except TypeError:
            print ("TypeError for ", parsed)
            raise
        
        allowed_domains = [
            ".ics.uci.edu", 
            ".cs.uci.edu", 
            ".informatics.uci.edu", 
            ".stat.uci.edu"
        ]
        
        # Check if the hostname ends with one of the allowed domains
        if not any(parsed.netloc.endswith(d) for d in allowed_domains):
            return False
        domain = f"{parsed.scheme}://{parsed.netloc}"

        #--Browse robots.txt--
        if not domain in self.permissions_cache:
            self.create_rfp(domain)
        
        if self.permissions_cache[domain] and not self.permissions_cache[domain].can_fetch('*', url): #robots.txt exists and we don't have permission to parse
            return False

        if self.permissions_cache[domain] and self.permissions_cache.crawl_delay('*'):
            delay = self.permissions_cache.crawl_delay('*')
            if time.time() - self.time_visited_cache[domain] < delay: # if its been less than delay amount of time since our last visit
                time.sleep(delay - (time.time() - self.time_visited_cache[domain])) # wait the rest of the time in the delay

        return not re.match( 
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    def create_rfp(self, domain):
        robotsURL = f"{domain}/robots.txt"
        rfp = RobotFileParser(robotsURL)
        resp = download(robotsURL, self.config, self.logger)

        if resp and resp.status == 200 and resp.raw_response: 
            try:
                lines = resp.raw_response.text.splitlines()
                rfp.parse(lines)
            except Exception as e:
                pass
                
        elif resp and resp.status == 404:
            #error raised if domain/robots.txt does not exist, 
            pass 
            
        else: #a un handled error probably means we shouldn't parse
            rfp = None
        
        self.permissions_cache[domain] = rfp #cache the whole RFP object to keep information about url access
        self.time_visited_cache[domain] = time.time()
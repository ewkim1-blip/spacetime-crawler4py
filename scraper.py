import re
from urllib.parse import urlparse
#student imports:
from urllib.robotparser import RobotFileParser #imported to handle robots.txt parsing | https://docs.python.org/3/library/urllib.robotparser.html#module-urllib.robotparser
import time

permissions_cache = {} #str::domain, RobotFileParser::rfp
time_visited_cache = {} 

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
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

def is_valid(url):
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

    #--Browse robots.txt--
    if not domain in permissions_cache:
        domain = f"{parsed.scheme}://{parsed.netloc}"
        robotsURL = f"{domain}/robots.txt"
        rfp = RobotFileParser(robotsURL)
        
        try: #error raised if domain/robots.txt does not exist
            rfp.read()
            permissions_cache[domain] = rfp #cache the whole RFP object to keep information about url access
        except:
            permissions_cache[domain] = None #assuming sites without a robots.txt are by default allowed to read

        time_visited_cache[domain] = time.time()

    if permissions_cache[domain] and permissions_cache[domain].can_fetch('*', url): #robots.txt exists and we don't have permission to parse
        return False

    #Here we can consider adding a sleep(duration) call. we also need to cache these results, 
    #so we can avoid spamming robots.txt reads and abide by rfp.crawl_delay('*')
    return not re.match( 
        r".*\.(css|js|bmp|gif|jpe?g|ico"
        + r"|png|tiff?|mid|mp2|mp3|mp4"
        + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
        + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
        + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
        + r"|epub|dll|cnf|tgz|sha1"
        + r"|thmx|mso|arff|rtf|jar|csv"
        + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

import scrapy
import json
import html
from w3lib.html import remove_tags
from datetime import datetime, timezone
import re

class TestSpider(scrapy.Spider):
    name = "Test"
    allowed_domains = ["deshimula.com"]
    start_urls = ["https://deshimula.com/"]

    def parse(self, response):

        posts = response.css("div.row")

        for post in posts:

            post_title = post.css("div.post-title::text").get(default="").strip()
            company_name = post.css("span.company-name::text").get(default="").strip()
            reviewer_role = post.css("span.reviewer-role::text").get(default="").strip()
            upvotes = post.css("div.row :nth-child(2) :first_child :nth-child(2) span.ms-2::text").get(default="").strip()
            downvotes = post.css("div.row :nth-child(2) :first_child :nth-child(3) span.ms-2::text").get(default="").strip()
            num_comments = post.css("div.row :nth-child(2) :first_child :nth-child(4) span.ms-2::text").get(default="").strip()
            sentiment = post.css("div.badge :nth-child(2)::text").get(default="").strip()
            verified = post.css("div.row :last_child span.ms-1::text").get(default="").strip()

            relative_url = post.css("div.row :nth-child(2) :first_child a::attr(href)").get()
            relative_url = relative_url or ""

            full_url = response.urljoin(relative_url)

            yield response.follow(
                full_url,
                callback=self.parse_story,
                meta = {
                    "post_title": post_title,
                    "company_name": company_name,
                    "reviewer_role": reviewer_role,
                    "upvotes": upvotes,
                    "downvotes": downvotes,
                    "num_comments": num_comments,
                    "sentiment": sentiment,
                    "verified": verified,
                    "post_id": relative_url.split("/")[-1] if relative_url else ""
                }
            )
        
        # current_url = response.url
        if response.url.strip("/") == "https://deshimula.com":
            current_page = 1
        else:
            match = re.search(r'/stories/(\d+)', response.url)
            current_page = int(match.group(1)) if match else 1

        # if "stories/" in current_url:
            # current_page = int(current_url.split("stories/")[-1])
        # else:
            # current_page = 0

        if current_page < 35:
            next_page = f"https://deshimula.com/stories/{current_page + 1}"
            yield response.follow(next_page, callback=self.parse)

        
    def parse_story(self, response):

        post_title = response.meta["post_title"]
        company_name = response.meta["company_name"]
        reviewer_role = response.meta["reviewer_role"]
        upvotes = response.meta["upvotes"]
        downvotes = response.meta["downvotes"]
        num_comments = response.meta["num_comments"]
        sentiment = response.meta["sentiment"]
        verified = response.meta["verified"]
        post_id = response.meta["post_id"]

        review = response.css("main :nth-child(3) div.row p *::text").getall()
        full_review = " ".join([p.strip() for p in review if p.strip()]) 

        comments_api = f"https://deshimula.com/Mula/GetComments?postId={post_id}&pageNumber=1"

        yield scrapy.Request(
            comments_api,
            callback=self.parse_comments,
            meta={
                "post_title": post_title,
                "company_name": company_name,
                "reviewer_role": reviewer_role,
                "upvotes": upvotes,
                "downvotes": downvotes,
                "num_comments": num_comments,
                "sentiment": sentiment,
                "verified": verified,
                "full_review": full_review,
                "review_url": response.url,
                "post_id": post_id,
                "post_date": self.extract_date_from_post_id(post_id),
            }
        )


    def parse_comments(self, response):

        data = json.loads(response.text)
        comments = data.get("Data", {}).get("Comments", [])

        comments_filtered = []

        for comment in comments:
            comments_filtered.append(
                {
                    "comment_text": remove_tags(html.unescape(comment.get("Text", ""))),
                    "comment_time": comment.get("DateTime", "")
                }   
            ) 

        yield{
            "post_title": response.meta["post_title"],
            "company_name": response.meta["company_name"],
            "reviewer_role": response.meta["reviewer_role"],
            "upvotes": response.meta["upvotes"],
            "downvotes": response.meta["downvotes"],
            "num_comments": response.meta["num_comments"],
            "sentiment": response.meta["sentiment"],
            "verified": response.meta["verified"],
            "post_id": response.meta["post_id"],
            "post_date": response.meta["post_date"],
            "full_review": response.meta["full_review"],
            "review_url": response.meta["review_url"],
            "comments": comments_filtered
        }
    
    @staticmethod
    def extract_date_from_post_id(post_id):
        if not post_id or len(post_id) < 8:
            return None
        
        try:
            timestamp = int(post_id[:8], 16)
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError, OSError):
            return None
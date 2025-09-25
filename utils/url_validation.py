import re
from typing import List, Optional


def extract_username_from_x_url(url: str) -> Optional[str]:
    """Extract username from X/Twitter URL."""
    patterns = [
        r'https?://(?:www\.)?(?:twitter\.com|x\.com)/([^/\?]+)(?:/status/\d+|/\d+)',
        r'https?://(?:www\.)?(?:twitter\.com|x\.com)/([^/\?]+)',
        r'https?://(?:mobile\.)?(?:twitter\.com|x\.com)/([^/\?]+)(?:/status/\d+|/\d+)',
    ]

    for pattern in patterns:
        match = re.search(pattern, url, re.IGNORECASE)
        if match:
            username = match.group(1).lower()
            if username not in [
                    'home', 'search', 'notifications', 'messages', 'i',
                    'explore', 'settings'
            ]:
                return username

    return None


def validate_reply_link(url: str, expected_username: str) -> bool:
    """Validate if URL belongs to expected X username."""
    extracted = extract_username_from_x_url(url)
    if not extracted:
        return False
    if '/status/' not in url.lower():
        return False
    return extracted.lower() == expected_username.lower()


def extract_urls_bulk_optimized(text: str) -> List[str]:
    """Optimized URL extraction for large text blocks."""
    url_pattern = r'https?://(?:www\.|mobile\.|m\.)?(?:twitter\.com|x\.com)/[^\s<>"\'`\n\r]+'
    urls = re.findall(url_pattern, text, re.IGNORECASE)
    seen = set()
    unique_urls = []
    for url in urls:
        cleaned_url = url.rstrip('.,;!?)')
        if cleaned_url not in seen:
            seen.add(cleaned_url)
            unique_urls.append(cleaned_url)
    return unique_urls


def extract_urls(text: str) -> List[str]:
    """Extract URLs from message text."""
    url_pattern = r'https?://(?:www\.|mobile\.|m\.)?(?:twitter\.com|x\.com)/[^\s<>"\'`\n\r]+'
    urls = re.findall(url_pattern, text, re.IGNORECASE)
    cleaned_urls = []
    for url in urls:
        cleaned_url = url.rstrip('.,;!?)')
        if cleaned_url not in cleaned_urls:
            cleaned_urls.append(cleaned_url)
    return cleaned_urls

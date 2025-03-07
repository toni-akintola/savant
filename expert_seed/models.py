class PartialBlueskyUser:
    def __init__(
        self, name, handle, description=None, followers=None, following=None, rank=None
    ):
        self.rank = rank
        self.name = name
        self.handle = handle
        self.description = description
        self.followers = followers
        self.following = following

    def __repr__(self):
        rank_str = f"#{self.rank}: " if self.rank is not None else ""
        description_str = f" :({self.description})" if self.description else ""
        return f"{rank_str}{self.name}, (@{self.handle}){description_str}"

    def __hash__(self):
        # Make hashable by handle (normalized to remove @ if present)
        return hash(self.handle.lstrip("@"))

    def __eq__(self, other):
        if not isinstance(other, PartialBlueskyUser):
            return False
        return self.handle.lstrip("@") == other.handle.lstrip("@")

    def to_dict(self):
        result = {
            "name": self.name,
            "handle": self.handle,
            "followers": self.followers,
            "following": self.following,
        }
        if self.rank is not None:
            result["rank"] = self.rank
        return result

    def __str__(self):
        return self.__repr__()


class WikipediaThumbnail:
    """
    Represents a thumbnail image from a Wikipedia page.
    """

    def __init__(
        self, mimetype: str, width: int, height: int, url: str, duration: float = None
    ):
        """
        Initialize a Wikipedia thumbnail.

        Args:
            mimetype: The MIME type of the thumbnail (e.g., "image/jpeg")
            width: The width of the thumbnail in pixels
            height: The height of the thumbnail in pixels
            url: The URL of the thumbnail
            duration: The duration of the media (for videos), if applicable
        """
        self.mimetype = mimetype
        self.width = width
        self.height = height
        self.url = url
        self.duration = duration

    def to_dict(self) -> dict:
        """Convert the thumbnail to a dictionary."""
        return {
            "mimetype": self.mimetype,
            "width": self.width,
            "height": self.height,
            "duration": self.duration,
            "url": self.url,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WikipediaThumbnail":
        """Create a thumbnail from a dictionary."""
        return cls(
            mimetype=data.get("mimetype", ""),
            width=data.get("width", 0),
            height=data.get("height", 0),
            url=data.get("url", ""),
            duration=data.get("duration"),
        )


class WikipediaPage:
    """
    Represents a Wikipedia page result from a search.
    """

    def __init__(
        self,
        id: int,
        key: str,
        title: str,
        excerpt: str,
        description: str,
        matched_title: str = None,
        thumbnail: WikipediaThumbnail = None,
    ):
        """
        Initialize a Wikipedia page.

        Args:
            id: The Wikipedia page ID
            key: The key/slug of the Wikipedia page
            title: The title of the Wikipedia page
            excerpt: An excerpt from the page, often with search matches highlighted
            description: A short description of the page content
            matched_title: An alternate title that matched the search query, if applicable
            thumbnail: Thumbnail image information, if available
        """
        self.id = id
        self.key = key
        self.title = title
        self.excerpt = excerpt
        self.description = description
        self.matched_title = matched_title
        self.thumbnail = thumbnail

    def to_dict(self) -> dict:
        """Convert the Wikipedia page to a dictionary."""
        result = {
            "id": self.id,
            "key": self.key,
            "title": self.title,
            "excerpt": self.excerpt,
            "description": self.description,
            "matched_title": self.matched_title,
        }

        if self.thumbnail:
            result["thumbnail"] = self.thumbnail.to_dict()

        return result

    @classmethod
    def from_dict(cls, data: dict) -> "WikipediaPage":
        """Create a Wikipedia page from a dictionary."""
        thumbnail_data = data.get("thumbnail")
        thumbnail = (
            WikipediaThumbnail.from_dict(thumbnail_data) if thumbnail_data else None
        )

        return cls(
            id=data.get("id", 0),
            key=data.get("key", ""),
            title=data.get("title", ""),
            excerpt=data.get("excerpt", ""),
            description=data.get("description", ""),
            matched_title=data.get("matched_title"),
            thumbnail=thumbnail,
        )

    def get_url(self) -> str:
        """Get the full Wikipedia URL for this page."""
        return f"https://en.wikipedia.org/wiki/{self.key}"

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

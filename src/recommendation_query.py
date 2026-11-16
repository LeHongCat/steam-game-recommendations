"""Map supported Vietnamese and English phrases to Steam tags and ABSA aspects."""

import re
import unicodedata
from dataclasses import dataclass

FPS_TAG = 1663
PACE_TAG = 1734
PRIMARY_TAG_LIMIT = 5

# IDs from Steam IStoreService/GetMostPopularTags. The export stores up to 20 tags per game.
TAGS = {
    FPS_TAG: (
        "FPS",
        ("fps", "first person shooter", "ban sung goc nhin thu nhat", "ban sung goc nhin thu 1"),
    ),
    1774: ("Shooter", ("shooter", "shooting game", "ban sung")),
    122: ("RPG", ("rpg", "role playing", "nhap vai")),
    9: ("Strategy", ("strategy", "chien thuat")),
    599: ("Simulation", ("simulation", "mo phong")),
    699: ("Racing", ("racing", "dua xe")),
    1667: ("Horror", ("horror", "kinh di")),
    1662: ("Survival", ("survival", "sinh ton")),
    1695: ("Open world", ("open world", "the gioi mo")),
    1716: ("Roguelike", ("roguelike",)),
    1664: ("Puzzle", ("puzzle", "giai do")),
    1685: ("Co-op", ("co op", "coop", "cooperative", "choi cung ban be")),
    3859: ("Multiplayer", ("multiplayer", "nhieu nguoi")),
    4182: ("Single-player", ("singleplayer", "single player", "choi don")),
    PACE_TAG: ("Fast-paced", ("fast paced", "nhip do nhanh", "gameplay nhanh", "toc do cao")),
    19: ("Action", ("action", "hanh dong")),
    21: ("Adventure", ("adventure", "phieu luu")),
    701: ("Sports", ("sports", "sport", "the thao", "bong da", "football", "soccer")),
    3810: ("Sandbox", ("sandbox",)),
    176981: ("Battle Royale", ("battle royale",)),
    1742: ("Story rich", ("story rich", "story driven")),
    1659: ("Zombies", ("zombie", "zombies", "xac song")),
    1644: ("Driving", ("driving", "drive", "truck", "trucks", "lai xe", "xe tai")),
    3942: ("Sci-fi", ("sci fi", "scifi", "science fiction", "khoa hoc vien tuong")),
    1684: ("Fantasy", ("fantasy", "gia tuong", "ky ao")),
    1666: ("Card game", ("card game", "the bai", "danh bai")),
    113: ("Free to play", ("free to play", "mien phi")),
}
ASPECTS = {
    "GRAPHICS#GENERAL": (
        "do hoa", "hinh anh", "dep mat", "graphics", "visuals", "beautiful", "gorgeous"
    ),
    "GAMEPLAY#GENERAL": ("gameplay", "loi choi", "co che choi", "combat"),
    "STORY#GENERAL": ("cot truyen", "cau chuyen", "story", "narrative"),
    "AUDIO#GENERAL": ("am thanh", "nhac nen", "soundtrack", "audio"),
    "TECHNICAL#PERFORMANCE": (
        "hieu nang",
        "toi uu",
        "chay muot",
        "it lag",
        "performance",
        "smooth",
    ),
    "TECHNICAL#RELIABILITY": ("on dinh", "it loi", "it crash", "stability", "reliability"),
    "GAME#ATMOSPHERE": ("khong khi", "bau khong khi", "atmosphere", "immersive"),
    "GAMEPLAY#DIFFICULTY": ("do kho", "thu thach", "challenging", "difficulty"),
    "GAMEPLAY#LEARNABILITY": ("de lam quen", "de hoc", "easy to learn", "beginner friendly"),
    "MULTIPLAYER#COMMUNITY": ("cong dong", "community"),
    "CONTROLS#USABILITY": ("dieu khien", "controls"),
}
NEGATION = re.compile(
    r"\b(?:khong(?: (?:muon|thich))?|tranh|avoid|without|no|not|"
    r"don t(?: want| like)?)(?: (?:a|any|game|play|choi|the loai))* $"
)
OPTIONAL = re.compile(r"\b(?:khong can|no need for)(?: (?:game|the loai|a|any))* $")
ALTERNATIVE = re.compile(r"\s+(?:hoac|or)\s+")


def matcher(aliases):
    by_phrase = {phrase: key for key, values in aliases.items() for phrase in values}
    words = sorted(by_phrase, key=len, reverse=True)
    pattern = re.compile(r"\b(?:" + "|".join(map(re.escape, words)) + r")\b")
    return pattern, by_phrase


TAG_PATTERN, TAG_ALIASES = matcher({key: aliases for key, (_, aliases) in TAGS.items()})
ASPECT_PATTERN, ASPECT_ALIASES = matcher(ASPECTS)


@dataclass
class QueryIntent:
    required_groups: list[set[int]]
    excluded: set[int]
    preferred: set[int]
    aspects: list[str]

    @property
    def recognized(self):
        return bool(self.required_groups or self.excluded or self.preferred or self.aspects)

    @property
    def required_tags(self):
        return set().union(*self.required_groups)

    def allows(self, tags):
        return not (tags & self.excluded) and all(tags & group for group in self.required_groups)


def normalize(value):
    value = unicodedata.normalize("NFKD", value.casefold().replace("đ", "d"))
    text = "".join(character for character in value if not unicodedata.combining(character))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9,;.!?]+", " ", text))


def query_game_tags(game):
    tags = set(game["tag_ids"] or [])
    primary = game.get("primary_tag_ids")
    if primary is not None and FPS_TAG not in primary:
        tags.discard(FPS_TAG)
    return tags


def parse_query(query):
    text = normalize(query)
    intent = QueryIntent([], set(), set(), [])
    last_end = 0
    last_group = None
    for match in TAG_PATTERN.finditer(text):
        tag = TAG_ALIASES[match.group()]
        prefix = text[last_end : match.start()]
        last_end = match.end()
        # Framerate such as "60 FPS" does not identify a genre.
        if OPTIONAL.search(prefix) or (match.group() == "fps" and re.search(r"\b\d+\s*$", prefix)):
            last_group = None
            continue
        if NEGATION.search(prefix):
            intent.excluded.add(tag)
            last_group = None
        elif tag == PACE_TAG:
            intent.preferred.add(tag)
            last_group = None
        elif last_group is not None and ALTERNATIVE.fullmatch(prefix):
            last_group.add(tag)
        else:
            last_group = {tag}
            intent.required_groups.append(last_group)
    for match in ASPECT_PATTERN.finditer(text):
        aspect = ASPECT_ALIASES[match.group()]
        prefix = text[: match.start()]
        if aspect not in intent.aspects and not (
            NEGATION.search(prefix) or OPTIONAL.search(prefix)
        ):
            intent.aspects.append(aspect)
    return intent

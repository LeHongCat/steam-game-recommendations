"""English labels for current predictions and previously recorded searches."""

from src.recommendation_query import TAGS

ASPECT_LABELS = {
    "GAME#GENERAL": ("Overall experience", "Trải nghiệm tổng thể"),
    "GAMEPLAY#GENERAL": ("Gameplay", "Gameplay"),
    "GAMEPLAY#DIFFICULTY": ("Difficulty", "Độ khó"),
    "GAMEPLAY#LEARNABILITY": ("Ease of learning", "Dễ làm quen"),
    "GRAPHICS#GENERAL": ("Graphics", "Đồ họa"),
    "STORY#GENERAL": ("Story", "Cốt truyện"),
    "GAME#ATMOSPHERE": ("Atmosphere", "Không khí"),
    "TECHNICAL#PERFORMANCE": ("Performance", "Hiệu năng"),
    "TECHNICAL#RELIABILITY": ("Stability", "Độ ổn định"),
    "MULTIPLAYER#GENERAL": ("Multiplayer", "Multiplayer"),
    "MULTIPLAYER#COMMUNITY": ("Community", "Cộng đồng"),
    "GAME#PRICE": ("Price", "Giá game"),
    "GAME#CONTENT": ("Content", "Nội dung"),
    "AUDIO#GENERAL": ("Audio", "Âm thanh"),
    "CONTROLS#USABILITY": ("Controls", "Điều khiển"),
    "CONTROLS#ACCESSIBILITY": ("Accessibility", "Khả năng tiếp cận"),
    "DEVELOPER#SUPPORT": ("Developer support", "Hỗ trợ từ nhà phát triển"),
    "DEVELOPER#POLICY": ("Developer policy", "Chính sách nhà phát triển"),
    "GAME#GENRE": ("Genre", "Thể loại"),
    "GAME#LICENSED_CONTENT": ("Licensed content", "Nội dung có bản quyền"),
    "GAME#LOCALISATION": ("Localization", "Bản địa hóa"),
    "GAME#MODS": ("Mods", "Mod"),
    "GAME#MONETISATION": ("Monetization", "Mua hàng trong game"),
    "TECHNICAL#COMPATIBILITY": ("Compatibility", "Tương thích"),
    "TECHNICAL#VR": ("Virtual reality", "Thực tế ảo"),
}
LEGACY_LABELS = {
    legacy: english for english, legacy in ASPECT_LABELS.values()
} | {
    "Bắn súng": "Shooter", "Chiến thuật": "Strategy", "Mô phỏng": "Simulation",
    "Đua xe": "Racing", "Kinh dị": "Horror", "Sinh tồn": "Survival",
    "Thế giới mở": "Open world", "Giải đố": "Puzzle", "Chơi đơn": "Single-player",
    "Nhịp độ nhanh": "Fast-paced",
}


def display_name(aspect):
    if aspect in ASPECT_LABELS:
        return ASPECT_LABELS[aspect][0]
    return aspect.replace("#", " · ").replace("_", " ").title()


def tag_labels(ids):
    return [{"id": tag, "label": TAGS[tag][0]} for tag in sorted(ids)]


def criterion(label):
    if label.startswith("Loại "):
        return "Exclude " + criterion(label[5:])
    return LEGACY_LABELS.get(label, label)


def explanation(aspects, tags, pace=0, similar=False):
    statements = []
    if tags:
        statements.append("Matches Steam tags " + ", ".join(tag["label"] for tag in tags) + ".")
    if aspects:
        statements.append("Review evidence for " + ", ".join(row["label"].lower() for row in aspects[:3]) + ".")
    elif similar:
        statements.append("Similar to games you selected, opened, or positively reviewed.")
    elif not tags:
        statements.append("Ranked using Steam review popularity.")
    if pace:
        statements.append(f"{pace} positive gameplay reviews mention fast pacing.")
    return " ".join(statements)

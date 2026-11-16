"""Item-based CF, content similarity and sentiment profiles for Steam reviews."""

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import combinations

from src.recommendation_query import PACE_TAG, query_game_tags

MODEL_VERSION = "steam-hybrid-v1"
MIN_SUPPORT = 10
QUERY_MODEL_WEIGHT = 0.30
AVERAGE_WEIGHTS = (0.10, 0.50, 0.25, 0.15)
EXPERIMENTS = {
    "E1": "Popularity",
    "E2": "Collaborative filtering",
    "E3": "CF + content",
    "E4": "CF + content + ABSA",
    "E5": "E4 + aspect reranking",
}


@dataclass
class Ranker:
    games: dict
    users: dict
    popularity: dict
    similarities: dict
    item_aspects: dict
    weights: tuple = AVERAGE_WEIGHTS

    def rank(
        self,
        user_id=None,
        seeds=(),
        aspects=(),
        top_k=10,
        experiment="E5",
        *,
        intent=None,
        clicked=(),
        semantic=None,
        semantic_weight=QUERY_MODEL_WEIGHT,
    ):
        user = self.users.get(user_id, {})
        history = list(dict.fromkeys([*seeds, *user.get("liked", []), *clicked]))
        excluded = set(seeds) | set(user.get("seen", [])) | set(clicked)
        prefs = user.get("aspects", {})
        priority = dict.fromkeys(aspects, 1.0)
        if not priority:
            prominent = sorted(prefs, key=lambda name: (-prefs[name]["support"], name))
            priority = {name: prefs[name]["support"] for name in prominent[:4]}
        candidates = []
        query_tags = intent.required_tags | intent.preferred if intent else set()
        for item_id, game in self.games.items():
            if item_id in excluded or (semantic is not None and item_id not in semantic):
                continue
            tags = query_game_tags(game) if intent else set()
            if intent and not intent.allows(tags):
                continue
            neighbors = self.similarities[item_id]
            cf = max((neighbors.get(seed, (0, 0))[0] for seed in history), default=0)
            content = max((neighbors.get(seed, (0, 0))[1] for seed in history), default=0)
            profile = self.item_aspects.get(item_id, {})
            shared = [
                name for name in prefs if profile.get(name, {}).get("support", 0) >= MIN_SUPPORT
            ]
            # Aspect frequency controls importance. A negative historical sentiment can still
            # make an aspect important. Favor quality independently of sentiment in the history.
            weighted = [(profile[name]["quality"], prefs[name]["support"]) for name in shared]
            aspect = quality_average(weighted)
            matches = [
                {"aspect": name, "priority_weight": priority[name], **profile[name]}
                for name in priority
                if profile.get(name, {}).get("support", 0) >= MIN_SUPPORT
            ]
            rerank = quality_average([(row["quality"], priority[row["aspect"]]) for row in matches])
            candidates.append((item_id, cf, content, aspect, rerank, matches, tags & query_tags))

        max_cf = max((row[1] for row in candidates), default=0)
        cf_scale = max_cf or 1
        content_scale = max((row[2] for row in candidates), default=0) or 1
        results = []
        for item, cf, content, aspect, rerank, matches, matched_tags in candidates:
            signals = {
                "popularity": self.popularity[item],
                "cf": cf / cf_scale,
                "content": content / content_scale,
                "absa": aspect,
            }
            blend = list(self.weights)
            if experiment == "E1":
                blend = [1, 0, 0, 0]
            elif experiment == "E2":
                blend = [0, 1, 0, 0] if max_cf else [1, 0, 0, 0]
            elif experiment == "E3":
                blend[3] = 0
            if not history or experiment == "E1":
                blend[1:3] = [0, 0]
            active = [
                (value, weight)
                for value, weight in zip(signals.values(), blend, strict=True)
                if value is not None and weight
            ]
            weight_sum = sum(weight for _, weight in active)
            score = sum(value * weight for value, weight in active) / weight_sum
            components = []
            for (name, value), weight in zip(signals.items(), blend, strict=True):
                effective = weight / weight_sum if value is not None else 0.0
                components.append({
                    "signal": name,
                    "value": value,
                    "weight": effective,
                    "contribution": (value or 0.0) * effective,
                })
            base_score = score
            adjustments = []
            if experiment == "E5" and rerank is not None:
                coverage = sum(priority[row["aspect"]] for row in matches) / sum(priority.values())
                signals["aspect_rerank"] = rerank * coverage
                before = score
                if intent:
                    score = min(1, max(0, score + 0.25 * coverage * (rerank - 0.5)))
                else:
                    score = 0.8 * score + 0.2 * rerank * coverage
                adjustments.append({
                    "kind": "query_aspects" if intent else "priority_aspects",
                    "quality": rerank, "coverage": coverage,
                    "before": before, "after": score,
                })
            pace_reviews = 0
            if intent and PACE_TAG in intent.preferred:
                pace_reviews = self.games[item].get("absa_summary", {}).get("fast_paced_reviews", 0)
                pace_match = (
                    1.0
                    if PACE_TAG in matched_tags
                    else min(pace_reviews / 10, 0.8)
                    if pace_reviews >= 2
                    else 0.0
                )
                if pace_match:
                    before = score
                    score += 0.20 * (1 - score) * pace_match
                    adjustments.append({
                        "kind": "fast_pace", "match": pace_match,
                        "source": "steam_tag" if PACE_TAG in matched_tags else "absa_reviews",
                        "review_support": pace_reviews, "before": before, "after": score,
                    })
            if semantic is not None:
                before = score
                similarity = semantic[item]
                relevance = min(1, max(0, (similarity - 0.70) / 0.25))
                score = (1 - semantic_weight) * score + semantic_weight * relevance
                adjustments.append({
                    "kind": "semantic_query", "weight": semantic_weight,
                    "similarity": similarity, "relevance": relevance, "before": before, "after": score,
                })
            results.append(
                {
                    "item_id": item,
                    "score": score,
                    "signals": signals,
                    "matched_aspects": matches if experiment == "E5" else [],
                    "matched_tag_ids": sorted(matched_tags),
                    "pace_review_support": pace_reviews if pace_reviews >= 2 else 0,
                    "scoring": {
                        "components": components, "base_score": base_score,
                        "adjustments": adjustments, "final_score": score,
                        "eligible_candidates": len(candidates),
                    },
                }
            )
        results.sort(
            key=lambda row: (-row["score"], -self.popularity[row["item_id"]], row["item_id"])
        )
        return results[:top_k]


def quality_average(rows):
    if not rows:
        return None
    return (
        1 + sum(value * weight for value, weight in rows) / sum(weight for _, weight in rows)
    ) / 2


def aspect_profiles(reviews, mentions, reference):
    items, users = defaultdict(dict), defaultdict(dict)
    for review in reviews:
        age = max(0, (reference - review["updated_at"]).total_seconds() / 86400)
        decay = 0.5 ** (age / 180)
        topics = mentions.get(review["review_id"], {})
        for topic, scores in topics.items():
            mean = sum(scores) / len(scores)
            for target, value in ((items[review["item_id"]], decay), (users[review["user_id"]], 1)):
                acc = target.setdefault(topic, [0.0, 0.0, 0])
                acc[0] += mean * value
                acc[1] += value
                acc[2] += 1
    item_profiles = {
        item: {
            name: {"quality": score / (weight + 5), "support": count}
            for name, (score, weight, count) in profile.items()
        }
        for item, profile in items.items()
    }
    user_profiles = {
        user: {
            name: {"sentiment": score / weight, "support": count}
            for name, (score, weight, count) in profile.items()
        }
        for user, profile in users.items()
    }
    return item_profiles, user_profiles


def fit(games, reviews, mentions, reference):
    item_aspects, user_aspects = aspect_profiles(reviews, mentions, reference)
    ratings = Counter(row["item_id"] for row in reviews)
    positives = Counter()
    users = {}
    for row in reviews:
        item, user = row["item_id"], row["user_id"]
        profile = users.setdefault(user, {"seen": set(), "liked": set()})
        profile["seen"].add(item)
        if row["recommended"]:
            profile["liked"].add(item)
            positives[item] += 1
    cooccurrence = Counter()
    for user, profile in users.items():
        profile["seen"] = sorted(profile["seen"])
        profile["liked"] = sorted(profile["liked"])
        profile["aspects"] = user_aspects.get(user, {})
        cooccurrence.update(combinations(profile["liked"], 2))
    prior = sum(positives.values()) / max(1, len(reviews))
    popularity = {item: (positives[item] + 25 * prior) / (ratings[item] + 25) for item in games}
    token_sets = {}
    for item, game in games.items():
        terms = re.findall(r"[a-z0-9]{4,}", (game["description"] or "").lower())
        tokens = set(terms)
        tokens.update(f"tag:{tag}" for tag in game["tag_ids"] or [])
        tokens.update(f"category:{tag}" for tag in game["category_ids"] or [])
        token_sets[item] = tokens
    similarities = {item: {} for item in games}
    for left, right in combinations(sorted(games), 2):
        overlap = cooccurrence[left, right]
        cf = overlap / math.sqrt(max(1, positives[left] * positives[right]))
        a, b = token_sets[left], token_sets[right]
        content = len(a & b) / math.sqrt(max(1, len(a) * len(b)))
        similarities[left][right] = similarities[right][left] = (cf, content)
    return Ranker(games, users, popularity, similarities, item_aspects)

import json
import random
from collections import defaultdict, Counter

# ==========================================================
# CONFIGURATION
# ==========================================================

INPUT_FILE = "real_news_dataset_categorized.json"
OUTPUT_FILE = "real_news_dataset_30k.json"

TARGET_SIZE = 30000

MIN_WORDS = 50
MAX_WORDS = 1500

MAX_YEAR = 2023

SEED = 42

# ==========================================================
# Chargement
# ==========================================================

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

articles = data["articles"]

print(f"\nArticles initiaux : {len(articles)}")

# ==========================================================
# Nettoyage
# ==========================================================

clean_articles = []

removed_date = 0
removed_short = 0
removed_long = 0

for article in articles:

    # -----------------------------
    # Vérification de la date
    # -----------------------------

    try:
        year = int(article["published_date"][:4])

        if year > MAX_YEAR:
            removed_date += 1
            continue

    except:
        removed_date += 1
        continue

    # -----------------------------
    # Nombre de mots
    # -----------------------------

    text = article.get("text", "")

    words = len(text.split())

    if words < MIN_WORDS:
        removed_short += 1
        continue

    if words > MAX_WORDS:
        removed_long += 1
        continue

    clean_articles.append(article)

print(f"Après nettoyage : {len(clean_articles)}")

# ==========================================================
# Distribution originale
# ==========================================================

category_counter = Counter()

for article in clean_articles:
    category_counter[article["category"]] += 1

print("\nDistribution originale\n")

for c, n in category_counter.most_common():
    print(f"{c:15s} : {n}")

# ==========================================================
# Regroupement par catégorie
# ==========================================================

groups = defaultdict(list)

for article in clean_articles:
    groups[article["category"]].append(article)

# ==========================================================
# Calcul automatique des quotas
# ==========================================================

total = len(clean_articles)

quotas = {}

for category in groups:

    proportion = len(groups[category]) / total

    quotas[category] = round(proportion * TARGET_SIZE)

# Ajustement pour avoir exactement TARGET_SIZE

difference = TARGET_SIZE - sum(quotas.values())

if difference != 0:

    biggest = max(quotas, key=quotas.get)

    quotas[biggest] += difference

print("\nQuotas calculés\n")

for c, q in sorted(quotas.items()):
    print(f"{c:15s} : {q}")

# ==========================================================
# Tirage aléatoire stratifié
# ==========================================================

random.seed(SEED)

selected = []

for category, articles_cat in groups.items():

    random.shuffle(articles_cat)

    quota = min(quotas[category], len(articles_cat))

    selected.extend(articles_cat[:quota])

random.shuffle(selected)

# ==========================================================
# Sauvegarde
# ==========================================================

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:

    json.dump(
        {"articles": selected},
        f,
        ensure_ascii=False,
        indent=4
    )

# ==========================================================
# Statistiques finales
# ==========================================================

final_counter = Counter()

for article in selected:
    final_counter[article["category"]] += 1

print("\n")
print("=" * 70)

print("RÉSUMÉ")

print("=" * 70)

print(f"Articles initiaux           : {len(articles)}")
print(f"Dates supprimées            : {removed_date}")
print(f"Articles < {MIN_WORDS} mots      : {removed_short}")
print(f"Articles > {MAX_WORDS} mots    : {removed_long}")
print(f"Articles après nettoyage    : {len(clean_articles)}")
print(f"Articles finaux             : {len(selected)}")

print("\nDistribution finale\n")

for c, n in final_counter.most_common():
    print(f"{c:15s} : {n}")

print("\nDataset sauvegardé dans :")
print(OUTPUT_FILE)

print("=" * 70)
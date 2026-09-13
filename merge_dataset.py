import os
import json
import re

# ==============================
# Configuration
# ==============================

INPUT_FOLDER = "cleaned/Fake"
OUTPUT_FILE = "fake_news_dataset.json"

# Expression régulière des fichiers
pattern = re.compile(r"cleaned_dataset.*\.json$")

all_articles = []
seen = set()

files_found = 0
total_before = 0

# ==============================
# Parcours des fichiers
# ==============================

for filename in sorted(os.listdir(INPUT_FOLDER)):

    if not pattern.match(filename):
        continue

    filepath = os.path.join(INPUT_FOLDER, filename)

    files_found += 1

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        articles = data.get("articles", [])

        total_before += len(articles)

        for article in articles:

            title = article.get("title", "").strip()
            text = article.get("text", "").strip()

            # Clé pour supprimer les doublons
            key = (title, text)

            if key in seen:
                continue

            seen.add(key)
            all_articles.append(article)

    except Exception as e:
        print(f"Erreur dans {filename} : {e}")

# ==============================
# Sauvegarde
# ==============================

dataset = {
    "articles": all_articles
}

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(dataset, f, ensure_ascii=False, indent=4)

# ==============================
# Statistiques
# ==============================

print("=" * 60)
print(f"Fichiers trouvés            : {files_found}")
print(f"Articles avant fusion       : {total_before}")
print(f"Articles après dédoublonnage: {len(all_articles)}")
print(f"Dataset sauvegardé dans     : {OUTPUT_FILE}")
print("=" * 60)
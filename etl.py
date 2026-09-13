"""
etl.py
------
Pipeline ETL pour préparer un dataset d'articles de presse.

Fonctionnalités :
1. Chargement d'un fichier JSON
2. Nettoyage du texte
3. Normalisation des dates
4. Suppression des doublons
5. Ajout du label (Fake / Real)
6. Catégorisation des articles Real
7. Statistiques détaillées
8. Sauvegarde du dataset nettoyé

UTILISATION :

    python etl.py fichier.json Fake

ou :

    python etl.py fichier.json Real

Pour un dataset Fake, le résultat est sauvegardé dans :
    cleaned/Fake/

Pour un dataset Real, le résultat est sauvegardé dans :
    cleaned/Real/

IMPORTANT :
- Par défaut, OUTPUT_DIR est "cleaned/Fake".
- Si vous traitez un dataset Real, changez OUTPUT_DIR en "cleaned/Real"
  ou utilisez l'argument "Real" lors de l'exécution.
"""

import json
import os
import re
import sys
from collections import Counter
from datetime import datetime
from statistics import mean, median


# ============================================================
# CONFIGURATION
# ============================================================

DEFAULT_OUTPUT_DIR = "cleaned/Fake"

# IMPORTANT :
# Pour un dataset REAL, utiliser :
#     OUTPUT_DIR = "cleaned/Real"
#
# Le script permet aussi de choisir Fake/Real directement
# avec l'argument de ligne de commande.


# ============================================================
# DICTIONNAIRE DES CATÉGORIES
# ============================================================

CATEGORY_KEYWORDS = {
    "Politics": [
        "رئيس", "الرئيس", "رئاسة", "الرئاسة", "ملك", "الملك",
        "ولي العهد", "أمير", "الأمير", "الحكومة", "حكومة",
        "وزير", "وزارة", "مجلس الوزراء", "البرلمان", "النواب",
        "مجلس النواب", "مجلس الشيوخ", "مجلس", "الدولة", "الدستور",
        "القانون", "القوانين", "الانتخابات", "انتخاب", "انتخابية",
        "التصويت", "مرشح", "مرشحين", "الحزب", "الأحزاب", "المعارضة",
        "الائتلاف", "البرلمانية", "الرئاسية", "المحافظ", "والي",
        "بلدية", "الداخلية", "الخارجية", "السفارة", "السفير",
        "الدبلوماسية", "القنصلية", "الأمم المتحدة", "مجلس الأمن",
        "الاتحاد الأوروبي", "الناتو", "الجامعة العربية",
        "الرئيس الفلسطيني", "عباس", "نتنياهو", "بوتين", "بايدن",
        "ترامب", "محمد السادس", "عبد الفتاح السيسي", "قيس سعيد",
        "احتلال", "إسرائيل", "فلسطين", "القدس", "غزة", "الضفة الغربية",
        "الحوثيين", "الحوثي", "اليمن", "العراق", "لبنان", "سوريا",
        "إيران", "قطر", "السعودية", "الإمارات", "الكويت", "الأردن",
        "وقف إطلاق النار", "هدنة", "مفاوضات", "وساطة", "قمة", "اجتماع",
        "عقوبات", "اتفاق", "اتفاقية", "معاهدة", "تطبيع", "سيادة",
        "الجيش", "القوات المسلحة", "القوات", "وزارة الدفاع", "الدفاع",
        "الشرطة العسكرية", "الاستخبارات", "الأمن القومي"
    ],

    "Sports": [
        "رياضة", "رياضي", "كرة", "كرة القدم", "الدوري", "كأس",
        "بطولة", "بطولات", "المنتخب", "منتخب", "مباراة", "مباريات",
        "لاعب", "لاعبين", "مدرب", "المدرب", "هدف", "أهداف", "فوز",
        "خسارة", "تعادل", "دوري أبطال أوروبا", "كأس العالم",
        "كوبا أمريكا", "يورو", "الفيفا", "فيفا", "الاتحاد الدولي",
        "ميسي", "ليونيل ميسي", "رونالدو", "محمد صلاح", "بنزيما",
        "هالاند", "مبابي", "نيمار", "برشلونة", "ريال مدريد",
        "ليفربول", "تشيلسي", "مانشستر سيتي", "مانشستر يونايتد",
        "أرسنال", "الرجاء", "الوداد", "الجيش الملكي", "نهضة بركان",
        "الأهلي", "الزمالك", "الترجي", "الهلال", "الحكم", "ركلة جزاء",
        "ركنية", "بطاقة حمراء", "بطاقة صفراء", "ملعب", "الجمهور",
        "كرة السلة", "كرة اليد", "الكرة الطائرة", "تنس", "ماراثون",
        "سباق", "سباقات", "الألعاب الأولمبية", "الأولمبياد"
    ],

    "Health": [
        "الصحة", "وزارة الصحة", "مستشفى", "مستشفيات", "طبيب", "أطباء",
        "ممرض", "ممرضة", "عيادة", "مرض", "مريض", "مرضى", "كورونا",
        "كوفيد", "كوفيد-19", "فيروس", "فيروسات", "لقاح", "تلقيح",
        "تطعيم", "وباء", "جائحة", "عدوى", "الفطر الأسود", "جدري القرود",
        "إنفلونزا", "سرطان", "سكري", "ضغط الدم", "القلب", "جلطة",
        "سكتة", "أورام", "أمراض مزمنة", "جراحة", "عملية جراحية",
        "تحاليل", "مختبر", "العناية المركزة", "إنعاش", "أكسجين",
        "دواء", "أدوية", "صيدلية", "علاج", "مناعة", "مناعة الجسم",
        "لقاحات", "وزارة الصحة والسكان", "الصحة العالمية",
        "منظمة الصحة العالمية"
    ],

    "Economy": [
        "اقتصاد", "اقتصادية", "اقتصادي", "الاقتصاد", "مالية",
        "وزارة المالية", "ميزانية", "الموازنة", "ضرائب", "ضريبة",
        "استثمار", "مستثمر", "استثمارات", "بنك", "البنك", "البنوك",
        "المصرف", "المصارف", "بورصة", "الأسهم", "سندات", "تضخم",
        "العملة", "الدولار", "اليورو", "درهم", "ريال", "جنيه", "دينار",
        "النفط", "البترول", "الغاز", "الطاقة", "الكهرباء", "المحروقات",
        "التجارة", "التصدير", "الاستيراد", "الجمارك", "الصادرات",
        "الواردات", "شركة", "شركات", "مؤسسة", "مؤسسات", "مصنع",
        "مصانع", "الإنتاج", "الصناعة", "القطاع الخاص", "ريادة الأعمال",
        "البطالة", "التشغيل", "الوظائف", "الأجور", "القروض",
        "التمويل", "التمويلات", "البنك المركزي", "صندوق النقد الدولي"
    ],

    "Technology": [
        "تكنولوجيا", "تقنية", "الذكاء الاصطناعي", "ذكاء اصطناعي",
        "روبوت", "حاسوب", "كمبيوتر", "إنترنت", "الإنترنت", "جوجل",
        "غوغل", "مايكروسوفت", "آبل", "أبل", "هاتف", "هواتف",
        "أندرويد", "أيفون", "برمجيات", "تطبيق", "تطبيقات", "سايبر",
        "الأمن السيبراني", "البيانات", "الرقمنة", "البرمجة"
    ],

    "Business": [
        "شركة", "شركات", "رجل أعمال", "أعمال", "استثمار", "استثمارات",
        "مؤسسة", "مؤسسات", "مبيعات", "أرباح", "خسائر", "عائدات",
        "عملاء", "منتج", "منتجات", "تسويق", "علامة تجارية", "مصنع",
        "إنتاج", "رأس المال", "مؤشر", "بورصة"
    ],

    "Entertainment": [
        "فيلم", "أفلام", "ممثل", "ممثلة", "مغني", "مغنية", "موسيقى",
        "أغنية", "مسلسل", "دراما", "سينما", "مهرجان", "فنان", "فنانة",
        "حفلة", "عرض", "نتفليكس", "يوتيوب", "إعلام", "تلفزيون"
    ],

    "Science": [
        "بحث", "دراسة", "علمي", "العلماء", "جامعة", "مختبر", "اكتشاف",
        "تجربة", "فيزياء", "كيمياء", "بيولوجيا", "فضاء", "ناسا",
        "قمر صناعي", "علم", "ابتكار", "اختراع"
    ],

    "Environment": [
        "المناخ", "الاحتباس الحراري", "التغير المناخي", "البيئة",
        "تلوث", "الغابات", "الفيضانات", "الجفاف", "الأمطار", "الطقس",
        "الزراعة", "المياه", "البحر", "المحيط", "الرياح"
    ],

    "Crime": [
        "شرطة", "الأمن", "اعتقال", "محكمة", "قضاء", "جريمة", "مجرم",
        "قتل", "سرقة", "مخدرات", "سجن", "نيابة", "تحقيق", "إرهاب",
        "تفجير", "محاكمة", "ضحية"
    ],

    "Education": [
        # التعليم العام
        "تعليم", "التعليم", "تعليمي", "التربوي", "تربية", "التربية",
        "وزارة التربية", "وزارة التربية الوطنية", "وزارة التعليم",
        "وزارة التعليم العالي", "المدارس",

        # établissements
        "مدرسة", "مدارس", "ثانوية", "إعدادية", "ابتدائية",
        "جامعة", "جامعات", "كلية", "كليات", "معهد", "معاهد",
        "أكاديمية", "أكاديميات", "مؤسسة تعليمية",

        # étudiants
        "طالب", "طالبة", "طلاب", "طالبات", "تلميذ", "تلميذة",
        "تلاميذ", "متعلم", "متعلمين",

        # enseignants
        "أستاذ", "أستاذة", "أساتذة", "معلم", "معلمة", "معلمين",
        "هيئة التدريس",

        # الدراسة
        "دراسة", "الدراسة", "الدروس", "منهج", "مناهج", "مقرر",
        "المقررات", "حصة", "حصص", "الفصل الدراسي", "السنة الدراسية",
        "العام الدراسي",

        # الامتحانات
        "امتحان", "امتحانات", "اختبار", "اختبارات", "بكالوريا",
        "البكالوريا", "الباكالوريا", "الثانوية العامة",
        "نتائج الامتحانات", "النجاح", "الرسوب",

        # التسجيل
        "تسجيل", "التسجيل", "قبول", "القبول", "منحة", "منح",
        "منحة دراسية", "التوجيه", "المنصة التعليمية",

        # التعليم الإلكتروني
        "تعليم عن بعد", "التعليم عن بعد", "منصة تعليمية",
        "التعلم الإلكتروني", "التعليم الإلكتروني", "الدروس عن بعد",
        "زووم", "Microsoft Teams", "Google Classroom",

        # البحث العلمي
        "بحث علمي", "باحث", "باحثون", "رسالة ماجستير", "ماجستير",
        "دكتوراه", "أطروحة", "أطروحات", "النشر العلمي",

        # كلمات شائعة
        "الجامعة", "المدرسة", "الكلية", "التلميذ", "الطالب الجامعي",
        "التكوين", "التكوين المهني", "التدريب", "التدريب المهني"
    ]
}


# ============================================================
# NETTOYAGE DU TEXTE
# ============================================================

def clean_text(text):
    """Nettoie et normalise le texte sans modifier son contenu."""
    if not text:
        return ""

    text = str(text)

    # Caractères invisibles Unicode
    text = re.sub(r"[\u200B-\u200F\u202A-\u202E\u2066-\u2069]", "", text)

    # Tatweel
    text = text.replace("ـ", "")

    # Guillemets
    text = text.replace("“", '"').replace("”", '"')
    text = text.replace("‘", "'").replace("’", "'")

    # Retours à la ligne multiples
    text = re.sub(r"\n+", "\n", text)

    # Espaces multiples
    text = re.sub(r"[ \t]+", " ", text)

    return text.strip()


# ============================================================
# NORMALISATION DES DATES
# ============================================================

def normalize_date(date_str):
    """Convertit les formats de date connus vers YYYY-MM-DD."""
    if not date_str:
        return ""

    date_str = str(date_str).strip()
    date_str = date_str.replace("Z", "+00:00")

    formats = [
        None,  # ISO 8601
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
    ]

    for fmt in formats:
        try:
            if fmt is None:
                dt = datetime.fromisoformat(date_str)
            else:
                dt = datetime.strptime(date_str, fmt)

            return dt.strftime("%Y-%m-%d")

        except (ValueError, TypeError):
            continue

    return date_str


# ============================================================
# CATÉGORISATION
# ============================================================

def classify_article(text):
    """
    Catégorisation heuristique basée sur les mots-clés.
    Retourne :
        catégorie, score, mots-clés trouvés
    """
    text_lower = text.lower()

    scores = {}
    matched_words = {}

    for category, keywords in CATEGORY_KEYWORDS.items():

        score = 0
        found = []

        for keyword in keywords:
            keyword_lower = keyword.lower()
            occurrences = text_lower.count(keyword_lower)

            if occurrences > 0:
                score += occurrences
                found.append(keyword)

        scores[category] = score
        matched_words[category] = found

    best_category = max(scores, key=scores.get)

    if scores[best_category] == 0:
        return "Other", 0, []

    return (
        best_category,
        scores[best_category],
        matched_words[best_category]
    )


# ============================================================
# CHARGEMENT
# ============================================================

def load_json(input_file):
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        articles = data.get("articles", [])
    elif isinstance(data, list):
        articles = data
    else:
        raise ValueError("Format JSON non supporté.")

    if not isinstance(articles, list):
        raise ValueError("La clé 'articles' doit contenir une liste.")

    return articles


# ============================================================
# PIPELINE ETL
# ============================================================

def run_etl(input_file, label="Fake", output_dir=DEFAULT_OUTPUT_DIR):
    label = label.capitalize()

    if label not in {"Fake", "Real"}:
        raise ValueError("Le label doit être 'Fake' ou 'Real'.")

    os.makedirs(output_dir, exist_ok=True)

    print("=" * 75)
    print("ETL - PREPARATION DU DATASET")
    print("=" * 75)
    print(f"Fichier source : {input_file}")
    print(f"Label          : {label}")
    print(f"Dossier sortie : {output_dir}")
    print("=" * 75)

    # --------------------------------------------------------
    # 1. Chargement
    # --------------------------------------------------------

    articles = load_json(input_file)

    initial_count = len(articles)

    print(f"\nArticles initiaux : {initial_count}")

    # --------------------------------------------------------
    # 2. Nettoyage + dates + dédoublonnage
    # --------------------------------------------------------

    cleaned_articles = []
    seen = set()

    duplicates_removed = 0

    for article in articles:

        title = clean_text(article.get("title", ""))
        text = clean_text(article.get("text", ""))

        # Accepte les deux variantes de nom de date
        raw_date = article.get(
            "published_date",
            article.get("published date", "")
        )

        date = normalize_date(raw_date)

        # Dédoublonnage basé sur titre + texte
        key = (title, text)

        if key in seen:
            duplicates_removed += 1
            continue

        seen.add(key)

        cleaned_article = {
            "published_date": date,
            "title": title,
            "text": text,
            "label": label
        }

        # ----------------------------------------------------
        # 3. Catégorisation
        # ----------------------------------------------------

        # La catégorisation Real est appliquée uniquement aux
        # datasets Real, comme dans categorized.py.
        #
        # Pour Fake, la catégorie peut rester "Unknown".
        if label == "Real":

            content = f"{title} {text}"

            category, score, keywords = classify_article(content)

            cleaned_article["category"] = category
            cleaned_article["category_score"] = score
            cleaned_article["matched_keywords"] = keywords

        cleaned_articles.append(cleaned_article)

    # --------------------------------------------------------
    # 4. Statistiques
    # --------------------------------------------------------

    year_counter = Counter()
    month_counter = Counter()
    category_counter = Counter()
    label_counter = Counter()

    length_words = []
    length_chars = []

    for article in cleaned_articles:

        date = article.get("published_date", "")

        if len(date) >= 7:

            if date[:4].isdigit():
                year_counter[date[:4]] += 1

            if date[:7].count("-") == 1:
                month_counter[date[:7]] += 1

        category_counter[
            article.get("category", "Unknown")
        ] += 1

        label_counter[
            article.get("label", "Unknown")
        ] += 1

        text = article.get("text", "")

        length_words.append(len(text.split()))
        length_chars.append(len(text))

    # --------------------------------------------------------
    # 5. Sauvegarde
    # --------------------------------------------------------

    output_file = os.path.join(
        output_dir,
        f"cleaned_dataset_{label.lower()}.json"
    )

    output = {
        "articles": cleaned_articles
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(
            output,
            f,
            ensure_ascii=False,
            indent=4
        )

    # --------------------------------------------------------
    # 6. Affichage des statistiques
    # --------------------------------------------------------

    print("\n" + "=" * 75)
    print("STATISTIQUES")
    print("=" * 75)

    print(f"Articles initiaux       : {initial_count}")
    print(f"Doublons supprimés      : {duplicates_removed}")
    print(f"Articles conservés      : {len(cleaned_articles)}")

    print("\nLABELS")
    for value, count in label_counter.items():
        print(f"{value:15s}: {count}")

    print("\nCATÉGORIES")
    for category, count in category_counter.most_common():
        print(f"{category:15s}: {count}")

    print("\nANNÉES")
    for year, count in sorted(year_counter.items()):
        print(f"{year}: {count}")

    print("\nTOP 20 MOIS")
    for month, count in month_counter.most_common(20):
        print(f"{month}: {count}")

    if length_words:

        print("\nLONGUEUR DES ARTICLES")

        print(f"Mots minimum       : {min(length_words)}")
        print(f"Mots maximum       : {max(length_words)}")
        print(f"Moyenne mots       : {mean(length_words):.1f}")
        print(f"Médiane mots       : {median(length_words)}")

        print(f"\nCaractères minimum : {min(length_chars)}")
        print(f"Caractères maximum : {max(length_chars)}")
        print(f"Moyenne caractères : {mean(length_chars):.1f}")
        print(f"Médiane caractères : {median(length_chars)}")

        # Distribution
        bins = {
            "<50": 0,
            "50-100": 0,
            "100-200": 0,
            "200-400": 0,
            "400-800": 0,
            "800-1500": 0,
            ">1500": 0
        }

        for n in length_words:

            if n < 50:
                bins["<50"] += 1
            elif n < 100:
                bins["50-100"] += 1
            elif n < 200:
                bins["100-200"] += 1
            elif n < 400:
                bins["200-400"] += 1
            elif n < 800:
                bins["400-800"] += 1
            elif n < 1500:
                bins["800-1500"] += 1
            else:
                bins[">1500"] += 1

        print("\nDISTRIBUTION DES LONGUEURS")

        for key, value in bins.items():
            print(f"{key:10s}: {value}")

    print("\n" + "=" * 75)
    print("ETL TERMINÉ")
    print("=" * 75)
    print(f"Dataset sauvegardé : {output_file}")
    print("=" * 75)

    return output_file


# ============================================================
# POINT D'ENTRÉE
# ============================================================

if __name__ == "__main__":

    if len(sys.argv) < 2:

        print(
            "\nUtilisation :\n"
            "  python etl.py fichier.json Fake\n"
            "  python etl.py fichier.json Real\n\n"
            "Exemple Fake :\n"
            "  python etl.py source_134_scraped_articles.json Fake\n\n"
            "Exemple Real :\n"
            "  python etl.py real_news_dataset.json Real\n"
        )

        sys.exit(1)

    input_file = sys.argv[1]

    label = sys.argv[2] if len(sys.argv) >= 3 else "Fake"

    # Dossier de sortie automatique selon le label.
    #
    # Fake  -> cleaned/fake
    # Real  -> cleaned/real
    #
    # Si vous préférez le modifier manuellement :
    # OUTPUT_DIR = "cleaned/fake"
    # et pour un dataset Real :
    # OUTPUT_DIR = "cleaned/real"

    output_dir = (
        "cleaned/fake"
        if label.capitalize() == "Fake"
        else "cleaned/real"
    )

    run_etl(
        input_file=input_file,
        label=label,
        output_dir=output_dir
    )

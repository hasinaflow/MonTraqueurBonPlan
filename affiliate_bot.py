# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════╗
║       BOT D'AFFILIATION AUTOMATISE - TELEGRAM v5.3                 ║
║  Scraper | Generateur | Filtre | Expediteur | Nettoyeur | Vitrine   ║
╠══════════════════════════════════════════════════════════════════════╣
║  DEPENDANCES :                                                       ║
║    pip install python-telegram-bot requests beautifulsoup4           ║
║                feedparser lxml python-dotenv                         ║
╠══════════════════════════════════════════════════════════════════════╣
║  FICHIERS GENERES AUTOMATIQUEMENT :                                  ║
║    - database.db           -> Base SQLite anti-doublon               ║
║    - prix_historique.json  -> Prix moyens des 30 derniers jours      ║
║    - bot.log               -> Journal des evenements                 ║
╠══════════════════════════════════════════════════════════════════════╣
║  NOUVEAUTES v5.3 :                                                   ║
║    - Publications 2x/jour : 13h00 et 20h00 uniquement               ║
║    - Top 10 meilleures promos par session (triees par reduction %)   ║
║    - Spotlight : 2 meilleures offres Amazon mises en avant           ║
║    - Filtre voitures/auto : aucune promo auto publiee                ║
║    - Score de qualite pour classer les offres                        ║
╚══════════════════════════════════════════════════════════════════════╝

FICHIER .env A CREER DANS LE MEME DOSSIER :
    TELEGRAM_BOT_TOKEN=TON_TOKEN_BOTFATHER
    TELEGRAM_CHANNEL_ID=-1001234567890
    TELEGRAM_CANAL_USERNAME=Ton_traqueur_bon_plan   (sans @)
    AMAZON_AFFILIATE_TAG=monsite-21
    CDISCOUNT_AFFILIATE_ID=MON_ID           (optionnel)
    RAKUTEN_AFFILIATE_ID=MON_ID             (optionnel)
    AMAZON_ACCESS_KEY=AKIAXXXXXXXXXXXXXXXX  (optionnel - PA-API)
    AMAZON_SECRET_KEY=xxxxxxxxxxxxxxxx      (optionnel - PA-API)
"""

# =====================================================================
#  IMPORTS
# =====================================================================

import os
import io
import json
import sqlite3
import logging
import hashlib
import asyncio
import re
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import requests
import feedparser
from bs4 import BeautifulSoup
from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from telegram.error import TelegramError
from telegram.constants import ParseMode
from dotenv import load_dotenv

# =====================================================================
#  CHARGEMENT DES VARIABLES D'ENVIRONNEMENT
# =====================================================================

load_dotenv()

# =====================================================================
#  CONFIGURATION
# =====================================================================

# Telegram — Creer un bot via @BotFather -> /newbot
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# ID du canal (format : -100XXXXXXXXXX)
# Pour l'obtenir : ajouter @username_to_id_bot a ton canal
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")

# Username du canal SANS le @ (ex: Ton_traqueur_bon_plan)
TELEGRAM_CANAL_USERNAME = os.getenv(
    "TELEGRAM_CANAL_USERNAME", "TON_CANAL"
)

# Amazon Affiliation
# Inscription : https://partenaires.amazon.fr/
AMAZON_AFFILIATE_TAG = os.getenv("AMAZON_AFFILIATE_TAG", "")

# Cdiscount Affiliation (optionnel)
# Inscription : https://www.cdiscount.com/affiliation/
CDISCOUNT_AFFILIATE_ID = os.getenv("CDISCOUNT_AFFILIATE_ID", "")

# Rakuten Affiliation (optionnel)
# Inscription : https://www.rakuten.fr/partenaires/editeurs/
RAKUTEN_AFFILIATE_ID = os.getenv("RAKUTEN_AFFILIATE_ID", "")

# Amazon PA-API 5.0 (optionnel)
# Inscription : https://webservices.amazon.fr/paapi5/documentation/
AMAZON_ACCESS_KEY = os.getenv("AMAZON_ACCESS_KEY", "")
AMAZON_SECRET_KEY = os.getenv("AMAZON_SECRET_KEY", "")

# ── Parametres de publication ─────────────────────────────────────────
# Heures de publication journalieres (format 24h)
# Le bot publie exactement 2 fois par jour a ces heures
HEURES_PUBLICATION  = [13, 20]   # 13h00 et 20h00

# Nombre maximum d'offres publiees par session
MAX_OFFRES_PAR_SESSION = 10

# Nombre d'offres Amazon mises en avant en debut de session
NB_SPOTLIGHT_AMAZON = 2

# Reduction minimale pour qu'un produit soit eligible
REDUCTION_MINIMUM_PCT = 15

# Duree de conservation des donnees
MAX_HISTORIQUE_JOURS = 30

# Fichiers de donnees
DB_NAME                 = "database.db"
FICHIER_PRIX_HISTORIQUE = "prix_historique.json"
FICHIER_LOG             = "bot.log"

# =====================================================================
#  VALIDATION DES VARIABLES CRITIQUES
# =====================================================================

if not TELEGRAM_BOT_TOKEN:
    print("ERREUR : TELEGRAM_BOT_TOKEN manquant dans le fichier .env")
    exit(1)
if not TELEGRAM_CHANNEL_ID:
    print("ERREUR : TELEGRAM_CHANNEL_ID manquant dans le fichier .env")
    exit(1)

# =====================================================================
#  INITIALISATION DU LOGGER
# =====================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(FICHIER_LOG, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Reduit le bruit des logs HTTP de python-telegram-bot
# A placer APRES logging.basicConfig pour que ca fonctionne
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# =====================================================================
#  BASE DE DONNEES SQLITE
# =====================================================================

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            url        TEXT PRIMARY KEY,
            date_ajout TEXT NOT NULL
        )
    """)
    # Table anti-doublon cross-sites (titres normalises)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS titres_postes (
            titre_cle  TEXT PRIMARY KEY,
            date_ajout TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    logger.info("[SQLite] Base de donnees initialisee")




def est_deja_poste_db(url: str) -> bool:
    """Verifie si l'URL est deja dans la base SQLite."""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM posts WHERE url = ?", (url,))
        resultat = cursor.fetchone()
        conn.close()
        return resultat is not None
    except sqlite3.Error as e:
        logger.error(f"[SQLite] Erreur lecture : {e}")
        return False


def marquer_comme_poste_db(url: str):
    """Ajoute l'URL dans la base SQLite avec la date du jour."""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO posts (url, date_ajout) VALUES (?, ?)",
            (url, datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logger.error(f"[SQLite] Erreur ecriture : {e}")

def _normaliser_titre(titre: str) -> str:
    """
    Normalise un titre pour la comparaison cross-sites.
    Supprime les mots communs, garde les mots cles produit.
    Ex: 'Bon plan - Casque Sony WH-1000XM5 (-25%)' 
     -> 'casque sony wh-1000xm5'
    """
    # Minuscules
    titre = titre.lower()
    # Supprime les patterns de prix et pourcentages
    titre = re.sub(r"\d+[\.,]?\d*\s*(?:eur|euro|euros|\u20ac|%)", "", titre)
    # Supprime les mots parasites
    mots_parasites = [
        "bon plan", "promo", "promotion", "deal", "offre",
        "reduction", "remise", "solde", "vente flash",
        "reconditionne", "neuf", "occasion", "certifie",
        "-", ":", "/", "(", ")", "[", "]",
    ]
    for mot in mots_parasites:
        titre = titre.replace(mot, " ")
    # Normalise les espaces et garde les 5 premiers mots significatifs
    mots = [m for m in titre.split() if len(m) > 2][:5]
    return " ".join(mots).strip()


def est_titre_deja_poste(titre: str) -> bool:
    """
    Verifie si un titre similaire a deja ete publie.
    Protege contre les doublons cross-sites (meme produit, URL differente).
    """
    titre_cle = _normaliser_titre(titre)
    if not titre_cle:
        return False
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM titres_postes WHERE titre_cle = ?",
            (titre_cle,)
        )
        resultat = cursor.fetchone()
        conn.close()
        return resultat is not None
    except sqlite3.Error as e:
        logger.error(f"[SQLite] Erreur lecture titre : {e}")
        return False


def marquer_titre_comme_poste(titre: str):
    """Enregistre le titre normalise dans la base."""
    titre_cle = _normaliser_titre(titre)
    if not titre_cle:
        return
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO titres_postes (titre_cle, date_ajout) "
            "VALUES (?, ?)",
            (titre_cle, datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logger.error(f"[SQLite] Erreur ecriture titre : {e}")


def nettoyer_db():
    """Supprime les entrees de plus de MAX_HISTORIQUE_JOURS jours."""
    try:
        limite = (
            datetime.now() - timedelta(days=MAX_HISTORIQUE_JOURS)
        ).isoformat()
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM posts WHERE date_ajout < ?", (limite,))
        cursor.execute(
            "DELETE FROM titres_postes WHERE date_ajout < ?", (limite,)
        )
        nb = cursor.rowcount
        conn.commit()
        conn.close()
        if nb > 0:
            logger.info(f"[SQLite] {nb} entrees expirees supprimees")
    except sqlite3.Error as e:
        logger.error(f"[SQLite] Erreur nettoyage : {e}")

# =====================================================================
#  PLANIFICATEUR — Calcul de la prochaine heure de publication
# =====================================================================

def prochaine_publication() -> float:
    """
    Calcule le nombre de secondes avant la prochaine publication.
    Publications programmees a HEURES_PUBLICATION (13h et 20h).
    Retourne 0 si on est dans la fenetre de publication (+-2 min).
    """
    maintenant = datetime.now()
    heure_actuelle = maintenant.hour
    minute_actuelle = maintenant.minute

    for heure in sorted(HEURES_PUBLICATION):
        if heure_actuelle == heure and minute_actuelle <= 2:
            return 0  # On est dans la fenetre de publication

    # Cherche la prochaine heure programmee aujourd'hui
    for heure in sorted(HEURES_PUBLICATION):
        if heure_actuelle < heure:
            prochaine = maintenant.replace(
                hour=heure, minute=0, second=0, microsecond=0
            )
            delta = (prochaine - maintenant).total_seconds()
            logger.info(
                f"[Planificateur] Prochaine publication a "
                f"{prochaine.strftime('%H:%M')} "
                f"(dans {int(delta // 3600)}h{int((delta % 3600) // 60)}min)"
            )
            return delta

    # Toutes les heures du jour sont passees -> attendre demain 13h
    demain = (maintenant + timedelta(days=1)).replace(
        hour=HEURES_PUBLICATION[0], minute=0, second=0, microsecond=0
    )
    delta = (demain - maintenant).total_seconds()
    logger.info(
        f"[Planificateur] Prochaine publication demain a "
        f"{demain.strftime('%H:%M')} "
        f"(dans {int(delta // 3600)}h{int((delta % 3600) // 60)}min)"
    )
    return delta


# =====================================================================
#  COMMANDE /start
# =====================================================================

async def start_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """
    Handler de la commande /start.
    Affiche un message de bienvenue avec bouton pour rejoindre le canal.
    Parse mode HTML — plus sur que MARKDOWN sur les titres francais.
    """
    url_canal = f"https://t.me/{TELEGRAM_CANAL_USERNAME}"

    sep = "\u2501" * 20
    message_bienvenue = (
        "\U0001f916 <b>Bienvenue sur ton Traqueur de Bons Plans !</b>\n"
        + sep + "\n"
        + "Je scanne Amazon, Cdiscount et d'autres sites en continu "
        + "pour te trouver les meilleures promos tech.\n\n"
        + "\U0001f4c5 <b>Deux sessions par jour :</b>\n"
        + "\u2022 13h00 — Top 10 bons plans du midi\n"
        + "\u2022 20h00 — Top 10 bons plans du soir\n\n"
        + "\U0001f680 <b>Rejoins le canal pour ne rien rater :</b>\n"
        + f"\U0001f449 {url_canal}\n"
        + sep + "\n"
        + "\U0001f4a1 <i>Astuce : Active les notifications du canal "
        + "pour etre le premier sur les stocks limites !</i>"
    )

    keyboard = [[
        InlineKeyboardButton(
            text="\U0001f4e2 Rejoindre le Canal d'Alertes",
            url=url_canal
        )
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        message_bienvenue,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )
    logger.info(
        f"[/start] Commande recue de "
        f"{update.effective_user.username or update.effective_user.id}"
    )


# =====================================================================
#  MODULE 1 — LE SCRAPER
# =====================================================================

class Scraper:
    """
    Scrape les sources : Amazon, Cdiscount, Dealabs, flux RSS promos.
    Retourne une liste de dicts standardises :
      { titre, prix_actuel, prix_original, image_url,
        url, description, source, condition }
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8",
        "Accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,*/*;q=0.8"
        ),
        "Connection": "keep-alive",
    }

    HEADERS_RSS = {
        "User-Agent": (
            "FeedFetcher-Google; "
            "(+http://www.google.com/feedfetcher.html)"
        ),
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
        "Accept-Language": "fr-FR,fr;q=0.9",
    }

    # ------------------------------------------------------------------
    #  VITRINE — Produits phares surveilles en permanence
    #
    #  COMMENT TROUVER UN ASIN :
    #    1. Va sur la fiche produit Amazon.fr
    #    2. L'ASIN est dans l'URL : amazon.fr/dp/XXXXXXXXXX
    #    3. Copie les 10 caracteres apres /dp/
    #
    #  Format : (ASIN, nom affiche dans les logs, condition)
    #  condition : "Neuf" ou "Reconditionne"
    # ------------------------------------------------------------------
    PRODUITS_VITRINE = [
        ("B0CHX1W1XY", "PS5 Console Sony PlayStation 5", "Neuf"),
        ("B0CHX1W1XY", "PS5 Console Sony PlayStation 5", "Reconditionne"),

        # Nintendo Switch 2 — ASIN a remplir quand disponible
        # ("XXXXXXXXXX", "Nintendo Switch 2", "Neuf"),

        ("B0CM5JL5XP", "iPhone 14 Pro", "Neuf"),
        ("B0CM5JL5XP", "iPhone 14 Pro", "Reconditionne"),

        ("B0DGJMXZ4H", "iPhone 16", "Neuf"),
        ("B0DGJMXZ4H", "iPhone 16", "Reconditionne"),

        ("B08H99BPJN", "Manette DualSense PS5", "Neuf"),

        # Xiaomi Redmi Note 15 Pro — ASIN a remplir quand disponible
        # ("XXXXXXXXXX", "Xiaomi Redmi Note 15 Pro", "Neuf"),
    ]

    def scraper_vitrine_amazon(self) -> list:
        """
        Scrape les prix des produits phares de la vitrine.
        Publie TOUJOURS, sans condition de reduction.
        """
        produits = []

        for asin, nom, condition in self.PRODUITS_VITRINE:
            if "XXXXX" in asin:
                continue

            url = (
                f"https://www.amazon.fr/dp/{asin}?condition=renewed"
                if condition == "Reconditionne"
                else f"https://www.amazon.fr/dp/{asin}"
            )

            try:
                session = requests.Session()
                response = session.get(url, headers=self.HEADERS, timeout=15)
                if response.status_code != 200:
                    logger.warning(f"[Vitrine] Inaccessible : {nom}")
                    continue

                soup = BeautifulSoup(response.text, "lxml")

                titre_el = soup.select_one("#productTitle")
                titre = titre_el.get_text(strip=True) if titre_el else nom

                prix_actuel = None
                for sel in [
                    "span.a-price-whole",
                    "#priceblock_ourprice",
                    "#priceblock_dealprice",
                    "span.apexPriceToPay span.a-offscreen",
                    ".a-price .a-offscreen",
                ]:
                    prix_el = soup.select_one(sel)
                    if prix_el:
                        prix_actuel = self._extraire_prix(
                            prix_el.get_text(strip=True)
                        )
                        if prix_actuel:
                            break

                prix_barre_el = soup.select_one(
                    "span.a-price.a-text-price span.a-offscreen"
                )
                prix_original = (
                    self._extraire_prix(prix_barre_el.get_text(strip=True))
                    if prix_barre_el else None
                )

                img_el = soup.select_one("#landingImage")
                image_url = img_el.get("src") if img_el else None

                if prix_actuel:
                    produits.append({
                        "titre":         f"{titre[:60]} ({condition})",
                        "prix_actuel":   prix_actuel,
                        "prix_original": prix_original,
                        "image_url":     image_url,
                        "url":           url,
                        "description":   f"Vitrine | {condition}",
                        "source":        "amazon",
                        "vitrine":       True,
                        "condition":     condition,
                        "score":         100,  # Score max pour la vitrine
                    })
                    logger.info(
                        f"[Vitrine] {nom} ({condition}) -> {prix_actuel} EUR"
                    )

            except Exception as e:
                logger.error(f"[Vitrine] Erreur sur {nom} : {e}")

        return produits

    def scraper_amazon_ventes_flash(self) -> list:
        """
        Scrape la page Ventes Flash d'Amazon.fr
        URL : https://www.amazon.fr/events/deals

        Amazon bloque souvent le scraping HTML direct.
        Methode recommandee : scraper_amazon_api() via PA-API 5.0.
        Si les selecteurs ne fonctionnent plus -> F12 sur la page.
        """
        produits = []
        url = "https://www.amazon.fr/events/deals"

        try:
            session = requests.Session()
            response = session.get(url, headers=self.HEADERS, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            cartes = soup.select("div[data-testid='grid-unit']")

            for carte in cartes:
                try:
                    titre_el = carte.select_one(
                        "span[data-testid='deal-title']"
                    )
                    titre = titre_el.get_text(strip=True) if titre_el else None

                    prix_el = carte.select_one("span.a-price-whole")
                    prix_actuel = (
                        self._extraire_prix(prix_el.get_text(strip=True))
                        if prix_el else None
                    )

                    prix_barre_el = carte.select_one(
                        "span.a-price.a-text-price span.a-offscreen"
                    )
                    prix_original = (
                        self._extraire_prix(prix_barre_el.get_text(strip=True))
                        if prix_barre_el else None
                    )

                    img_el = carte.select_one("img.deal-image")
                    image_url = (
                        img_el["src"]
                        if img_el and img_el.get("src") else None
                    )

                    lien_el = carte.select_one("a[href*='/dp/']")
                    lien = (
                        "https://www.amazon.fr" + lien_el["href"]
                        if lien_el else None
                    )

                    if all([titre, prix_actuel, lien]):
                        produits.append({
                            "titre":         titre,
                            "prix_actuel":   prix_actuel,
                            "prix_original": prix_original,
                            "image_url":     image_url,
                            "url":           lien,
                            "description":   "",
                            "source":        "amazon",
                            "condition":     "Neuf",
                            "score":         0,
                        })

                except Exception as e:
                    logger.debug(f"Erreur parsing carte Amazon : {e}")
                    continue

            logger.info(
                f"[Scraper Amazon] {len(produits)} produits recuperes"
            )

        except requests.RequestException as e:
            logger.error(f"[Scraper Amazon] Erreur reseau : {e}")

        return produits

    def scraper_amazon_api(self) -> list:
        """
        METHODE RECOMMANDEE pour Amazon — PA-API 5.0 officielle.

        PREREQUIS :
          1. Minimum 3 ventes via ton tag partenaire
          2. S'inscrire : https://webservices.amazon.fr/paapi5/
          3. Remplir .env : AMAZON_ACCESS_KEY et AMAZON_SECRET_KEY
          4. Installer : pip install amazon-paapi5

        Decommenter et adapter ce bloc quand tu as tes cles API.
        """
        produits = []

        if not AMAZON_ACCESS_KEY or not AMAZON_SECRET_KEY:
            logger.warning(
                "[API Amazon] Cles PA-API non configurees. Skipping."
            )
            return produits

        try:
            # Decommenter quand tu as tes cles PA-API
            # from paapi5_python_sdk.api.default_api import DefaultApi
            # from paapi5_python_sdk.models.search_items_request import (
            #     SearchItemsRequest
            # )
            # import paapi5_python_sdk as paapi
            # client = paapi.ApiClient()
            # client.configuration.access_key = AMAZON_ACCESS_KEY
            # client.configuration.secret_key  = AMAZON_SECRET_KEY
            # api = DefaultApi(client)
            # recherches = [
            #     "PS5 PlayStation 5", "iPhone 16",
            #     "Nintendo Switch", "Redmi Note Xiaomi",
            #     "ventes flash tech",
            # ]
            # for mot_cle in recherches:
            #     request = SearchItemsRequest(
            #         partner_tag=AMAZON_AFFILIATE_TAG,
            #         partner_type="Associates",
            #         keywords=mot_cle,
            #         search_index="All",
            #         item_count=5,
            #         resources=[
            #             "Images.Primary.Large",
            #             "ItemInfo.Title",
            #             "Offers.Listings.Price",
            #             "Offers.Listings.SavingBasis",
            #         ]
            #     )
            #     response = api.search_items(request)
            #     for item in response.search_result.items:
            #         prix = float(item.offers.listings[0].price.amount)
            #         prix_ref = item.offers.listings[0].saving_basis
            #         produits.append({
            #             "titre":         item.item_info.title.display_value,
            #             "prix_actuel":   prix,
            #             "prix_original": float(prix_ref.amount)
            #                              if prix_ref else None,
            #             "image_url":     item.images.primary.large.url,
            #             "url":           item.detail_page_url,
            #             "description":   "",
            #             "source":        "amazon_api",
            #             "condition":     "Neuf",
            #             "score":         0,
            #         })
            logger.info(f"[API Amazon] {len(produits)} produits via PA-API")

        except Exception as e:
            logger.error(f"[API Amazon PA-API] Erreur : {e}")

        return produits

    def scraper_cdiscount_promos(self) -> list:
        """
        Scrape les promotions Cdiscount.
        URL : https://www.cdiscount.com/le-coin-des-bonnes-affaires.html
        Selecteurs CSS a mettre a jour si Cdiscount change son design.
        """
        produits = []
        url = "https://www.cdiscount.com/le-coin-des-bonnes-affaires.html"

        try:
            response = requests.get(url, headers=self.HEADERS, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")
            cartes = soup.select("li.prdtBILi")

            for carte in cartes:
                try:
                    titre_el    = carte.select_one("div.prdtBTit")
                    prix_el     = carte.select_one("span.price")
                    prix_bar_el = carte.select_one("span.stricken")
                    img_el      = carte.select_one("img.imgPrd")
                    lien_el     = carte.select_one("a[href]")

                    titre = (
                        titre_el.get_text(strip=True) if titre_el else None
                    )
                    prix_actuel = (
                        self._extraire_prix(prix_el.get_text(strip=True))
                        if prix_el else None
                    )
                    prix_original = (
                        self._extraire_prix(prix_bar_el.get_text(strip=True))
                        if prix_bar_el else None
                    )
                    image_url = (
                        img_el.get("src") or img_el.get("data-src")
                        if img_el else None
                    )
                    lien = lien_el["href"] if lien_el else None

                    if lien and not lien.startswith("http"):
                        lien = "https://www.cdiscount.com" + lien

                    if all([titre, prix_actuel, lien]):
                        produits.append({
                            "titre":         titre,
                            "prix_actuel":   prix_actuel,
                            "prix_original": prix_original,
                            "image_url":     image_url,
                            "url":           lien,
                            "description":   "",
                            "source":        "cdiscount",
                            "condition":     "Neuf",
                            "score":         0,
                        })

                except Exception as e:
                    logger.debug(f"Erreur parsing Cdiscount : {e}")
                    continue

            logger.info(
                f"[Scraper Cdiscount] {len(produits)} produits recuperes"
            )

        except requests.RequestException as e:
            logger.error(f"[Scraper Cdiscount] Erreur reseau : {e}")

        return produits

    def scraper_rakuten(self) -> list:
        """
        RAKUTEN AFFILIATION — A activer quand tu as ton compte.

        PREREQUIS :
          1. S'inscrire : https://www.rakuten.fr/partenaires/editeurs/
          2. Remplir .env : RAKUTEN_AFFILIATE_ID
          3. Obtenir ton token API dans le tableau de bord Rakuten

        MID utiles (a verifier dans ton dashboard) :
          - Fnac : 43962 | Darty : 43963

        Decommenter et adapter quand tu as ton compte.
        """
        produits = []

        if not RAKUTEN_AFFILIATE_ID:
            logger.debug("[Rakuten] ID non configure. Skipping.")
            return produits

        try:
            # Decommenter quand tu as ton ID Rakuten
            # url_flux = (
            #     "https://api.linksynergy.com/productsearch/1.0"
            #     f"?token=TON_TOKEN_API&keyword=tech&max=20&mid=43962"
            # )
            # response = requests.get(url_flux, headers=self.HEADERS, timeout=15)
            # data = response.json()
            # for item in data.get("result", {}).get("item", []):
            #     produits.append({
            #         "titre":         item.get("productname", ""),
            #         "prix_actuel":   float(item.get("saleprice",{}).get("$",0)),
            #         "prix_original": float(item.get("listprice",{}).get("$",0)) or None,
            #         "image_url":     item.get("imageurl", None),
            #         "url":           item.get("linkurl", ""),
            #         "description":   item.get("description", ""),
            #         "source":        "rakuten",
            #         "condition":     "Neuf",
            #         "score":         0,
            #     })
            logger.info(f"[Rakuten] {len(produits)} produits recuperes")

        except Exception as e:
            logger.error(f"[Rakuten] Erreur : {e}")

        return produits

    def scraper_dealabs(
        self,
        url_rss: str = "https://www.dealabs.com/rss/groupe/tech"
    ) -> list:
        """
        Scrape les deals Dealabs via leur flux RSS tech.

        URL RSS alternatives :
          - /rss/groupe/tech  -> Tech uniquement (recommande)
          - /rss/discussions  -> Tous les deals
        """
        produits = []

        try:
            response = requests.get(url_rss, headers=self.HEADERS, timeout=15)
            logger.info(f"[Dealabs] Status : {response.status_code}")

            if response.status_code != 200:
                logger.warning(
                    f"[Dealabs] Bloque : erreur {response.status_code}"
                )
                return produits

            flux = feedparser.parse(io.BytesIO(response.content))
            logger.info(f"[Dealabs] Entrees : {len(flux.entries)}")

            for entree in flux.entries[:20]:
                titre_brut = entree.get("title", "")
                summary    = entree.get("summary", "")
                titre_propre = self._nettoyer_titre(titre_brut)

                prix = self._extraire_prix_depuis_texte(titre_propre)
                if not prix:
                    prix = self._extraire_prix_depuis_texte(summary)

                lien_original = entree.get("link", "")
                lien_marchand = self.extraire_vrai_lien_marchand(lien_original)

                produits.append({
                    "titre":         titre_propre,
                    "prix_actuel":   prix,
                    "prix_original": None,
                    "image_url":     None,
                    "url":           lien_marchand,
                    "description":   summary,
                    "source":        "dealabs",
                    "condition":     "Neuf",
                    "score":         0,
                })

            logger.info(f"[Dealabs] {len(produits)} deals recuperes")

        except Exception as e:
            logger.error(f"[Dealabs] Erreur : {e}")

        return produits

    def extraire_vrai_lien_marchand(self, url_dealabs: str) -> str:
        """
        Extrait le lien direct du marchand depuis une page Dealabs.
        Si 'cept-dealBtn' ne fonctionne plus -> F12 sur une page Dealabs.
        """
        try:
            response = requests.get(url_dealabs, headers=self.HEADERS, timeout=10)
            if response.status_code != 200:
                return url_dealabs

            soup = BeautifulSoup(response.content, "html.parser")
            link_tag = soup.find("a", {"class": "cept-dealBtn"})
            if not link_tag or not link_tag.get("href"):
                return url_dealabs

            visit_url = "https://www.dealabs.com" + link_tag.get("href")
            final_res = requests.get(
                visit_url, headers=self.HEADERS,
                allow_redirects=True, timeout=10
            )
            return final_res.url.split("?")[0]

        except Exception as e:
            logger.debug(f"[Dealabs] Extraction lien echouee : {e}")
            return url_dealabs

    def scraper_flux_rss(self, url_rss: str, source: str = "rss") -> list:
        """
        Scrape un flux RSS generique.

        FLUX RSS PROMOS UNIQUEMENT RECOMMANDES :
          Frandroid promos    | frandroid.com/bons-plans/feed
          LesNumeriques bons  | lesnumeriques.com/bons-plans/rss.xml
          01net bons plans    | 01net.com/bons-plans/feed/
          BDM deals           | blogdumoderateur.com/deals/feed/
          Korben              | korben.info/feed

        MAUVAIS FLUX (melangent tout) :
          - frandroid.com/feed         (news + tests + promos)
          - lesnumeriques.com/rss.xml  (tous types d'articles)
        """
        produits = []

        try:
            response = requests.get(
                url_rss, headers=self.HEADERS_RSS, timeout=15, stream=False
            )
            logger.info(f"[RSS] {source} -> Status : {response.status_code}")

            if response.status_code != 200:
                logger.warning(
                    f"[RSS] Inaccessible ({response.status_code}) : {url_rss}"
                )
                return produits

            response.encoding = response.apparent_encoding
            feed = feedparser.parse(response.text)
            logger.info(f"[RSS] {source} -> {len(feed.entries)} entrees")

            for entry in feed.entries:
                titre_brut   = entry.get("title", "")
                summary      = entry.get("summary", "")
                titre_propre = self._nettoyer_titre(titre_brut)

                prix = self._extraire_prix_depuis_texte(titre_propre)
                if not prix:
                    prix = self._extraire_prix_depuis_texte(summary)

                produits.append({
                    "titre":         titre_propre,
                    "prix_actuel":   prix,
                    "prix_original": None,
                    "image_url":     None,
                    "url":           entry.get("link", ""),
                    "description":   summary,
                    "source":        source,
                    "condition":     "Neuf",
                    "score":         0,
                })

        except requests.exceptions.Timeout:
            logger.error(f"[RSS] Timeout : {url_rss}")
        except Exception as e:
            logger.error(f"[RSS] Erreur {source} : {e}")

        return produits

    @staticmethod
    def _nettoyer_titre(titre: str) -> str:
        """
        Nettoie un titre RSS.
        Supprime les coupures "..." et prefixes "DEAL : ".
        """
        if not titre:
            return ""
        titre = titre.split("...")[0].strip()
        if " : " in titre:
            parties = titre.split(" : ", 1)
            titre = max(parties, key=len).strip()
        if len(titre) > 80:
            titre = titre[:80].rsplit(" ", 1)[0] + "..."
        return titre

    @staticmethod
    def _extraire_prix_depuis_texte(texte: str) -> float | None:
        """Cherche un prix dans un texte libre."""
        if not texte:
            return None
        match = re.search(
            r"(\d[\d\s]*[\.,]?\d*)\s*(?:\u20ac|EUR|euro|euros)",
            texte, re.IGNORECASE
        )
        if match:
            val = match.group(1).replace(" ", "").replace(",", ".")
            try:
                return float(val)
            except ValueError:
                pass
        match2 = re.search(r"\b(\d{2,4})[.,](\d{2})\b", texte)
        if match2:
            try:
                return float(f"{match2.group(1)}.{match2.group(2)}")
            except ValueError:
                pass
        return None

    @staticmethod
    def _extraire_prix(texte: str) -> float | None:
        """Extrait un float depuis une chaine prix (ex: '129,99 EUR')."""
        if not texte:
            return None
        texte_clean = re.sub(r"[^\d,\.]", "", texte.replace(",", "."))
        match = re.search(r"\d+\.?\d*", texte_clean)
        return float(match.group()) if match else None


# =====================================================================
#  MODULE 2 — LE GENERATEUR DE LIENS AFFILIES
# =====================================================================

class GenerateurAffiliation:

    def generer_lien(self, url: str, source: str) -> str:
        """Dispatch vers le bon generateur selon la source."""
        if source in ("amazon", "amazon_api"):
            return self._lien_amazon(url)
        elif source == "cdiscount":
            return self._lien_cdiscount(url)
        elif source == "rakuten":
            return self._lien_rakuten(url)
        elif source == "dealabs":
            if "amazon.fr" in url:
                return self._lien_amazon(url)
            return url
        else:
            return url

    def _lien_amazon(self, url: str) -> str:
        """
        Genere un lien affilie Amazon.
        Format : https://www.amazon.fr/dp/ASIN?tag=TON_TAG
        """
        if not AMAZON_AFFILIATE_TAG:
            return url
        match = re.search(r"/dp/([A-Z0-9]{10})", url)
        if match:
            return (
                f"https://www.amazon.fr/dp/{match.group(1)}"
                f"?tag={AMAZON_AFFILIATE_TAG}"
            )
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}tag={AMAZON_AFFILIATE_TAG}"

    def _lien_cdiscount(self, url: str) -> str:
        """
        Genere un lien affilie Cdiscount.
        Documentation : https://www.cdiscount.com/affiliation/
        """
        if not CDISCOUNT_AFFILIATE_ID:
            return url
        sep = "&" if "?" in url else "?"
        return (
            f"{url}{sep}cm_mmc="
            f"Affiliation-_-{CDISCOUNT_AFFILIATE_ID}-_-NA-_-NA"
        )

    def _lien_rakuten(self, url: str) -> str:
        """
        Genere un lien affilie Rakuten.
        Remplir RAKUTEN_AFFILIATE_ID dans .env pour activer.
        MID marchand : Fnac=43962, Darty=43963
        """
        if not RAKUTEN_AFFILIATE_ID:
            return url
        # Decommenter quand tu as ton ID Rakuten
        # mid = "43962"
        # return (
        #     f"https://click.linksynergy.com/deeplink"
        #     f"?id={RAKUTEN_AFFILIATE_ID}&mid={mid}"
        #     f"&murl={quote_plus(url)}"
        # )
        return url

    def raccourcir_lien(self, url: str) -> str:
        """
        Raccourcit un lien via TinyURL (gratuit, sans cle).

        Alternative avec stats de clics : bit.ly
          -> Inscription : https://bitly.com/
          -> Ajouter BITLY_TOKEN dans .env
          -> Decommenter le bloc ci-dessous :
        #
        # BITLY_TOKEN = os.getenv("BITLY_TOKEN", "")
        # if BITLY_TOKEN:
        #     r = requests.post(
        #         "https://api-ssl.bitly.com/v4/shorten",
        #         headers={"Authorization": f"Bearer {BITLY_TOKEN}"},
        #         json={"long_url": url}, timeout=10
        #     )
        #     return r.json().get("link", url)
        """
        try:
            r = requests.get(
                f"https://tinyurl.com/api-create.php?url={quote_plus(url)}",
                timeout=10
            )
            if r.status_code == 200:
                return r.text.strip()
        except Exception as e:
            logger.warning(f"[TinyURL] Echec : {e}")
        return url


# =====================================================================
#  MODULE 3 — LE FILTRE
# =====================================================================

class Filtre:
    """
    Plusieurs niveaux de filtrage :
      1. SQLite anti-doublon
      2. Filtre voitures/auto (NOUVEAU v5.3)
      3. Filtre articles inutiles (tests, avis, dossiers...)
      4. Filtre presence de prix obligatoire
      5. Filtre mots-cles tech
      6. Validation reduction (prix historique)
      7. Score de qualite pour classer les offres
    """

    # Mots-cles tech acceptes
    MOTS_CLES_TECH = [
        # Audio
        "casque", "ecouteurs", "airpods", "sony", "bose", "jabra", "jbl",
        "sennheiser", "beats", "marshall", "audio-technica",
        # Smartphones et tablettes
        "iphone", "samsung", "xiaomi", "ipad", "tablette", "oppo",
        "pixel", "honor", "realme", "motorola", "oneplus", "redmi",
        "nothing phone",
        # Informatique
        "laptop", "pc portable", "macbook", "ssd", "ram", "disque dur",
        "clavier", "souris", "webcam", "imprimante", "routeur", "nas",
        # TV et photo
        "tv", "4k", "8k", "oled", "qled", "appareil photo", "gopro",
        "drone", "stabilisateur", "objectif", "reflex",
        # Gaming
        "manette", "playstation", "ps5", "ps4", "xbox", "nintendo",
        "switch", "steam deck", "gaming", "razer", "corsair", "logitech",
        # Montres et objets connectes
        "apple watch", "galaxy watch", "garmin", "fitbit",
        "montre connectee", "bracelet connecte", "smartwatch",
        # Maison connectee
        "alexa", "google home", "philips hue", "roomba",
        "aspirateur robot", "enceinte connectee",
        "ampoule connectee", "camera surveillance",
    ]

    # Mots qui indiquent une promotion automobile/vehicule
    # Ces produits sont exclus car hors-sujet pour un canal tech
    MOTS_CLES_AUTO = [
        "voiture", "automobile", "auto ", "vehicule", "berline",
        "suv", "4x4", "monospace", "cabriolet", "coupe auto",
        "pneu", "pneus", "jante", "jantes", "essuie-glace",
        "pare-brise", "amortisseur", "frein", "freins",
        "batterie voiture", "batterie auto",
        "huile moteur", "filtre huile", "filtre air auto",
        "siege auto", "rehausseur", "booster auto",
        "gps voiture", "autoradio", "dashcam",
        "tapis de sol voiture", "housse siege",
        "chargeur voiture", "adaptateur allume-cigare",
        "moto ", "scooter", "velo electrique", "trottinette",
        "camping-car", "caravane",
        "cire voiture", "nettoyant voiture",
    ]

    # Mots qui indiquent un article non-commercial
    MOTS_INTERDITS_TITRE = [
        "ACTUALITE", "TEST", "CRITIQUE", "AVIS", "CHRONIQUE",
        "REPORTAGE", "DOSSIER", "GUIDE D'ACHAT", "GUIDE DACHAT",
        "RUMEUR", "FUITE", "BREVET", "INTERVIEW", "PORTRAIT",
        "DEBAT", "EDITO", "EDITORIAL", "ANALYSE", "COMPARATIF",
        "TUTO", "TUTORIEL", "COMMENT ", "POURQUOI", "QU'EST-CE",
        "REVIEW", "HANDS-ON", "PRISE EN MAIN", "APERCU",
    ]

    def __init__(self):
        self.historique_prix = self._charger_historique_prix()

    # -- Filtre voitures/auto (NOUVEAU v5.3) ---------------------------

    def est_produit_auto(self, titre: str, description: str = "") -> bool:
        """
        Retourne True si le produit est lie a l'automobile.
        Ces produits sont exclus du canal tech.
        Verifie le titre ET la description pour etre exhaustif.
        """
        texte = f"{titre} {description}".lower()
        return any(mot in texte for mot in self.MOTS_CLES_AUTO)

    # -- Filtre articles inutiles --------------------------------------

    def est_article_inutile(self, titre: str) -> bool:
        """Retourne True si le titre indique un article non-commercial."""
        titre_upper = titre.upper()
        return any(mot in titre_upper for mot in self.MOTS_INTERDITS_TITRE)

    # -- Filtre presence de prix ---------------------------------------

    def contient_prix(self, titre: str, description: str) -> bool:
        """Un article sans prix est probablement une news."""
        texte = f"{titre} {description}"
        if re.search(
            r"\d+[\s,\.]?\d*\s*(?:\u20ac|EUR|euro|euros)",
            texte, re.IGNORECASE
        ):
            return True
        return bool(re.search(r"\d{2,4}[\.,]\d{2}", texte))

    # -- Filtre mots-cles tech -----------------------------------------

    def est_produit_tech(self, titre: str) -> bool:
        """Retourne True si le titre contient au moins un mot-cle tech."""
        titre_lower = titre.lower()
        return any(mot in titre_lower for mot in self.MOTS_CLES_TECH)

    # -- Score de qualite (NOUVEAU v5.3) --------------------------------

    def calculer_score(
        self,
        produit: dict,
        reduction_pct: float | None
    ) -> int:
        """
        Calcule un score de qualite pour classer les offres.
        Le top 10 est selectionne selon ce score.

        Criteres de scoring :
          - Reduction % : jusqu'a 60 points
          - Source Amazon : +20 points (source la plus fiable)
          - Prix disponible : +10 points
          - Image disponible : +5 points
          - Produit vitrine : +5 points (bonus prestige)
        """
        score = 0

        # Reduction (critere principal)
        if reduction_pct:
            score += min(int(reduction_pct), 60)

        # Bonus source Amazon (liens affilies directs + fiabilite)
        if "amazon" in produit.get("source", ""):
            score += 20

        # Bonus prix disponible
        if produit.get("prix_actuel"):
            score += 10

        # Bonus image disponible
        if produit.get("image_url"):
            score += 5

        # Bonus produit vitrine
        if produit.get("vitrine"):
            score += 5

        return score

    # -- Validation de la reduction ------------------------------------

    def calculer_reduction(
        self,
        prix_actuel: float | None,
        prix_original: float | None,
        url: str
    ) -> float | None:
        """
        Calcule le % de reduction.
        Priorite 1 : prix barre affiche.
        Priorite 2 : prix moyen historique 30 jours.
        """
        if prix_actuel is None:
            return None

        id_produit = self._generer_id(url)
        prix_reference = None
        self._enregistrer_prix(id_produit, prix_actuel)

        if prix_original and prix_original > prix_actuel:
            prix_reference = prix_original
        elif id_produit in self.historique_prix:
            prix_moyen = self._calculer_prix_moyen(id_produit)
            if prix_moyen and prix_moyen > prix_actuel:
                prix_reference = prix_moyen

        if prix_reference is None:
            return None

        return round(
            ((prix_reference - prix_actuel) / prix_reference) * 100, 1
        )

    def est_bonne_affaire(self, reduction_pct: float | None) -> bool:
        """Retourne True si la reduction depasse le seuil configure."""
        return (
            reduction_pct is not None
            and reduction_pct >= REDUCTION_MINIMUM_PCT
        )

    def est_une_grosse_promo(
        self, description: str, seuil: int = 15
    ) -> tuple:
        """Cherche un % de reduction dans un texte RSS."""
        match = re.search(r"-?\s*(\d+)\s*%", description)
        if match:
            valeur = int(match.group(1))
            return valeur >= seuil, valeur
        return False, 0

    # -- Visuels -------------------------------------------------------

    @staticmethod
    def generer_barre(remise_val: int) -> str:
        """Barre visuelle d'intensite. 0% -> aucune flamme | 50%+ -> cinq."""
        nb_feu  = min(remise_val // 10, 5)
        nb_vide = 5 - nb_feu
        return "\U0001f525" * nb_feu + "\u25ac" * nb_vide

    @staticmethod
    def generer_hashtags(titre: str, source: str) -> str:
        """Genere des hashtags pour booster la visibilite du canal."""
        tags = ["#BonPlan", "#Promo", "#Deal", "#Tech"]
        t = titre.lower()

        if "ps5" in t or "playstation" in t:
            tags += ["#PS5", "#PlayStation", "#Gaming"]
        if "xbox" in t:
            tags += ["#Xbox", "#Gaming", "#Microsoft"]
        if "nintendo" in t or "switch" in t:
            tags += ["#Nintendo", "#Switch", "#Gaming"]
        if "iphone" in t or "apple" in t:
            tags += ["#iPhone", "#Apple", "#Smartphone"]
        if "samsung" in t:
            tags += ["#Samsung", "#Galaxy", "#Smartphone"]
        if "xiaomi" in t or "redmi" in t:
            tags += ["#Xiaomi", "#Redmi", "#Smartphone"]
        if "sony" in t:
            tags += ["#Sony", "#Audio"]
        if any(m in t for m in ["casque", "ecouteurs", "airpods"]):
            tags += ["#Audio", "#Casque"]
        if any(m in t for m in ["tv", "oled", "qled", "4k"]):
            tags += ["#TV", "#4K", "#HiFi"]
        if any(m in t for m in ["laptop", "pc portable", "macbook"]):
            tags += ["#PC", "#Laptop", "#Informatique"]
        if "amazon" in source:
            tags.append("#Amazon")
        if "cdiscount" in source:
            tags.append("#Cdiscount")
        if "dealabs" in source:
            tags.append("#Dealabs")

        return " ".join(list(dict.fromkeys(tags))[:6])

    # -- Historique des prix -------------------------------------------

    def _enregistrer_prix(self, id_produit: str, prix: float):
        entree = {"prix": prix, "date": datetime.now().isoformat()}
        self.historique_prix.setdefault(id_produit, []).append(entree)
        self._sauvegarder_historique_prix()

    def _calculer_prix_moyen(self, id_produit: str) -> float | None:
        limite = datetime.now() - timedelta(days=MAX_HISTORIQUE_JOURS)
        entrees = [
            e["prix"]
            for e in self.historique_prix.get(id_produit, [])
            if datetime.fromisoformat(e["date"]) >= limite
        ]
        return round(sum(entrees) / len(entrees), 2) if entrees else None

    def _charger_historique_prix(self) -> dict:
        if os.path.exists(FICHIER_PRIX_HISTORIQUE):
            with open(FICHIER_PRIX_HISTORIQUE, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _sauvegarder_historique_prix(self):
        with open(FICHIER_PRIX_HISTORIQUE, "w", encoding="utf-8") as f:
            json.dump(
                self.historique_prix, f, ensure_ascii=False, indent=2
            )

    @staticmethod
    def _generer_id(url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()[:12]


# =====================================================================
#  MODULE 4 — L'EXPEDITEUR TELEGRAM (VERSION ASYNC)
# =====================================================================

class ExpediteurTelegram:

    def __init__(self, bot: Bot):
        """Prend un objet Bot en parametre (cree par Application)."""
        self.bot = bot

    async def tester_connexion(self):
        """Envoie un message de demarrage enrichi sur le canal."""
        try:
            heure = datetime.now().strftime("%H:%M")
            date  = datetime.now().strftime("%d/%m/%Y")
            sep   = "\u2501" * 20
            await self.bot.send_message(
                chat_id=TELEGRAM_CHANNEL_ID,
                text=(
                    "\U0001f680 <b>Bot d'affiliation actif !</b>\n"
                    + sep + "\n"
                    + f"\U0001f4c5 {date} \u2022 \U0001f550 {heure}\n"
                    + "\U0001f4b0 Reduction min : "
                    + f"{REDUCTION_MINIMUM_PCT}%\n"
                    + "\U0001f4ca <b>Sessions :</b> 13h00 et 20h00\n"
                    + f"\U0001f3c6 Top {MAX_OFFRES_PAR_SESSION} "
                    + "offres par session\n"
                    + sep + "\n"
                    + "\U0001f50d <b>Sources surveillees :</b>\n"
                    + "\u2022 Amazon (Ventes Flash + Vitrine)\n"
                    + "\u2022 Cdiscount (Bons plans)\n"
                    + "\u2022 Dealabs Tech\n"
                    + "\u2022 Frandroid Bons Plans\n"
                    + "\u2022 LesNumeriques Promos\n"
                    + "\u2022 01net Bons Plans\n"
                    + "\u2022 Korben\n"
                    + "\u2022 Blog du Moderateur Deals\n"
                    + sep + "\n"
                    + "\U0001f514 <i>Active les notifications "
                    + "pour ne rien rater !</i>"
                ),
                parse_mode=ParseMode.HTML
            )
            logger.info("[Telegram] Connexion OK")
        except TelegramError as e:
            logger.error(f"[Telegram] Erreur connexion : {e}")

    async def publier_intro_session(self, heure_session: int):
        """
        Publie un message d'introduction au debut de chaque session.
        Annonce les 10 meilleures offres qui vont suivre.
        """
        session_label = "du Midi" if heure_session == 13 else "du Soir"
        sep = "\u2501" * 20
        try:
            await self.bot.send_message(
                chat_id=TELEGRAM_CHANNEL_ID,
                text=(
                    f"\U0001f4e3 <b>SESSION BONS PLANS {session_label}</b>"
                    f" \U0001f4e3\n"
                    + sep + "\n"
                    + f"\U0001f3c6 <b>Top {MAX_OFFRES_PAR_SESSION} "
                    + "meilleures offres du moment !</b>\n"
                    + f"\u2b50 Les {NB_SPOTLIGHT_AMAZON} meilleurs deals "
                    + "Amazon en tete\n"
                    + sep + "\n"
                    + "\U0001f447 <i>Les offres arrivent dans quelques "
                    + "secondes, prepare-toi !</i>"
                ),
                parse_mode=ParseMode.HTML
            )
            await asyncio.sleep(3)
        except TelegramError as e:
            logger.error(f"[Telegram] Erreur intro session : {e}")

    async def publier_spotlight_amazon(self, produit: dict, rang: int):
        """
        Publie un produit Amazon en format Spotlight (mis en avant).
        Utilise pour les NB_SPOTLIGHT_AMAZON meilleures offres Amazon.
        Format plus grand et plus visible que les offres normales.
        """
        try:
            texte  = self._formater_spotlight(produit, rang)
            boutons = self._creer_boutons(
                produit.get("lien_court", produit.get("url", "")),
                produit["source"]
            )
            clavier = InlineKeyboardMarkup(boutons)

            if produit.get("image_url"):
                await self.bot.send_photo(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    photo=produit["image_url"],
                    caption=texte,
                    parse_mode=ParseMode.HTML,
                    reply_markup=clavier,
                )
            else:
                await self.bot.send_message(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    text=texte,
                    parse_mode=ParseMode.HTML,
                    reply_markup=clavier,
                )

            logger.info(
                f"[Spotlight #{rang}] {produit['titre'][:50]}"
            )
            return True

        except TelegramError as e:
            logger.error(f"[Spotlight] TelegramError : {e}")
            return False

    async def publier_offre_async(
        self,
        produit: dict,
        lien_affilie: str,
        reduction_pct: float
    ) -> bool:
        """Publie une offre standard sur le canal."""
        try:
            texte   = self._formater_message(
                produit, reduction_pct, lien_affilie
            )
            boutons = self._creer_boutons(lien_affilie, produit["source"])
            clavier = InlineKeyboardMarkup(boutons)

            if produit.get("image_url"):
                await self.bot.send_photo(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    photo=produit["image_url"],
                    caption=texte,
                    parse_mode=ParseMode.HTML,
                    reply_markup=clavier,
                )
            else:
                await self.bot.send_message(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    text=texte,
                    parse_mode=ParseMode.HTML,
                    reply_markup=clavier,
                    disable_web_page_preview=False,
                )

            logger.info(f"[Telegram] Publie : {produit['titre'][:50]}")
            return True

        except TelegramError as e:
            logger.error(f"[Telegram] TelegramError : {e}")
            return False
        except Exception as e:
            logger.error(f"[Telegram] Erreur inattendue : {e}")
            return False

    @staticmethod
    def _extraire_marchand(description: str, source: str) -> str:
        """Extrait le nom du marchand. Retourne 'Offre Speciale' si inconnu."""
        marchands = [
            "Amazon", "Fnac", "Cdiscount", "Boulanger", "Darty",
            "eBay", "Rakuten", "Ldlc", "Rue du Commerce",
            "Micromania", "Cultura", "Grosbill", "Leclerc", "Auchan",
        ]
        for m in marchands:
            if m.lower() in description.lower():
                return m
        map_source = {
            "amazon":     "Amazon",
            "amazon_api": "Amazon",
            "cdiscount":  "Cdiscount",
            "rakuten":    "Rakuten",
            "dealabs":    "Dealabs",
        }
        return map_source.get(source, "Offre Speciale")

    @staticmethod
    def _generer_slogan(titre: str, valeur_remise: int, condition: str) -> str:
        """Genere un slogan dynamique selon la categorie produit."""
        t = titre.lower()

        if "ps5" in t or "playstation" in t:
            slogans = [
                "La console next-gen au meilleur prix !",
                "Pour les gamers exigeants !",
                "Stock tres limite, fonce !",
            ]
        elif "iphone" in t:
            slogans = [
                "L'iPhone a prix casse, c'est rare !",
                "Le premium Apple accessible !",
                "L'Apple Store ne fait pas ce prix !",
            ]
        elif "xbox" in t:
            slogans = [
                "Xbox au meilleur prix du moment !",
                "Le deal gaming Microsoft qu'on attendait !",
            ]
        elif "nintendo" in t or "switch" in t:
            slogans = [
                "Nintendo au meilleur prix !",
                "Le deal Switch a ne pas rater !",
            ]
        elif any(m in t for m in ["casque", "ecouteurs", "airpods"]):
            slogans = [
                "Le son premium a prix mini !",
                "L'audio de qualite sans se ruiner !",
            ]
        elif any(m in t for m in ["tv", "oled", "qled", "4k"]):
            slogans = [
                "L'image 4K a prix imbattable !",
                "La TV de tes reves enfin accessible !",
            ]
        elif any(m in t for m in ["laptop", "pc portable", "macbook"]):
            slogans = [
                "La puissance informatique au meilleur prix !",
                "Le PC pro a prix accessible !",
            ]
        elif any(m in t for m in ["samsung", "xiaomi", "redmi"]):
            slogans = [
                "Le smartphone Android au meilleur prix !",
                "Performances max, prix mini !",
            ]
        else:
            slogans = [
                "Offre limitee, fonce avant rupture !",
                "Le meilleur rapport qualite/prix du moment !",
                "Deal verifie et approuve !",
            ]

        slogan_base = slogans[datetime.now().minute % len(slogans)]

        if valeur_remise >= 50:
            prefixe = "\U0001f3c6 EXCEPTIONNEL ! "
        elif valeur_remise >= 30:
            prefixe = "\u2b50 SUPER DEAL ! "
        elif valeur_remise >= 15:
            prefixe = "\U0001f4a1 BONNE AFFAIRE ! "
        else:
            prefixe = "\u26a1\ufe0f "

        suffixe = (
            " (Reconditionne certifie Amazon)"
            if condition == "Reconditionne" else ""
        )
        return f"{prefixe}{slogan_base}{suffixe}"

    @staticmethod
    def _formater_spotlight(produit: dict, rang: int) -> str:
        """
        Formate un message Spotlight pour les meilleures offres Amazon.
        Format plus visible et premium que les offres standards.
        Affiche le rang (#1, #2) pour mettre en valeur les meilleures offres.
        """
        titre       = produit.get("titre", "Offre Speciale")
        prix_actuel = produit.get("prix_actuel")
        prix_orig   = produit.get("prix_original")
        description = produit.get("description", "")
        condition   = produit.get("condition", "Neuf")
        lien        = produit.get("lien_court", produit.get("url", ""))
        score       = produit.get("score", 0)

        sep = "\u2501" * 20

        match = re.search(r"-?\s*(\d+)\s*%", description)
        valeur_remise = int(match.group(1)) if match else 0
        if not valeur_remise and prix_actuel and prix_orig and prix_orig > prix_actuel:
            valeur_remise = int(
                ((prix_orig - prix_actuel) / prix_orig) * 100
            )

        barre = Filtre.generer_barre(valeur_remise)

        # Medaille selon le rang
        medailles = {1: "\U0001f947", 2: "\U0001f948", 3: "\U0001f949"}
        medaille = medailles.get(rang, f"#{rang}")

        badge_condition = ""
        if condition == "Reconditionne":
            badge_condition = "\u267b\ufe0f <b>RECONDITIONNE CERTIFIE</b>\n"

        if prix_actuel and prix_orig and prix_orig > prix_actuel:
            economie  = prix_orig - prix_actuel
            ligne_prix = (
                f"\u274c <s>{prix_orig:.2f} EUR</s>  "
                f"\u2705 <b>{prix_actuel:.2f} EUR</b> "
                f"(-{valeur_remise}%  \u2022  -{economie:.2f} EUR)"
            )
        elif prix_actuel:
            pct = f" (-{valeur_remise}%)" if valeur_remise > 0 else ""
            ligne_prix = f"\U0001f4b0 <b>{prix_actuel:.2f} EUR{pct}</b>"
        else:
            ligne_prix = "\U0001f4b0 <b>Prix brade !</b>"

        lien_html = (
            f'\n\U0001f517 <a href="{lien}">Voir l\'offre</a>'
            if lien else ""
        )

        heure = time.strftime("%H:%M")

        return (
            f"{medaille} <b>MEILLEURE OFFRE AMAZON #{rang}</b>\n"
            + badge_condition
            + barre + "\n"
            + sep + "\n"
            + f"\U0001f4e6 <b>Produit :</b> {titre}\n"
            + f"{ligne_prix}\n"
            + f"\U0001f3ea <b>Marchand :</b> Amazon\n"
            + sep + "\n"
            + f"\U0001f3c6 Score qualite : {score} pts\n"
            + f"\U0001f554 <i>Detecte a {heure}</i>"
            + lien_html
        )

    @staticmethod
    def _formater_message(
        produit: dict,
        reduction_pct: float,
        lien_affilie: str = ""
    ) -> str:
        """
        Genere le texte HTML du message Telegram standard.

        sep defini en variable une seule fois — evite le bug
        de multiplication (f-string * 20 qui repetait le message).
        Assemblage avec + explicites.
        """
        titre       = produit.get("titre", "Offre Speciale")
        prix_actuel = produit.get("prix_actuel")
        prix_orig   = produit.get("prix_original")
        source      = produit.get("source", "")
        description = produit.get("description", "")
        est_vitrine = produit.get("vitrine", False)
        condition   = produit.get("condition", "Neuf")

        sep = "\u2501" * 20

        match = re.search(r"-?\s*(\d+)\s*%", description)
        valeur_remise = int(match.group(1)) if match else int(reduction_pct)

        hashtags = Filtre.generer_hashtags(titre, source)
        marchand = ExpediteurTelegram._extraire_marchand(description, source)

        labels_source = {
            "amazon":     "AMAZON",
            "amazon_api": "AMAZON",
            "cdiscount":  "CDISCOUNT",
            "rakuten":    "RAKUTEN",
            "dealabs":    "DEALABS",
        }
        label_source = labels_source.get(source, "BON PLAN")

        badge_vitrine = (
            "\u2b50 <b>PRODUIT PHARE DU CANAL</b> \u2b50\n"
            if est_vitrine else ""
        )

        if condition == "Reconditionne":
            badge_condition = (
                "\u267b\ufe0f <b>RECONDITIONNE CERTIFIE AMAZON</b>\n"
            )
        elif est_vitrine:
            badge_condition = "\U0001f195 <b>NEUF</b>\n"
        else:
            badge_condition = ""

        if prix_actuel and prix_orig and prix_orig > prix_actuel:
            economie  = prix_orig - prix_actuel
            ligne_prix = (
                f"\u274c <s>{prix_orig:.2f} EUR</s>  "
                f"\u2705 <b>{prix_actuel:.2f} EUR</b> "
                f"(-{valeur_remise}%  \u2022  -{economie:.2f} EUR)"
            )
        elif prix_actuel:
            pct = f" (-{valeur_remise}%)" if valeur_remise > 0 else ""
            ligne_prix = f"\U0001f4b0 <b>{prix_actuel:.2f} EUR{pct}</b>"
        elif valeur_remise > 0:
            ligne_prix = f"\U0001f4b0 Remise : <b>-{valeur_remise}%</b>"
        else:
            ligne_prix = "\U0001f4b0 <b>Prix brade !</b>"

        slogan = ExpediteurTelegram._generer_slogan(
            titre, valeur_remise, condition
        )
        lien_html = (
            f'\n\U0001f517 <a href="{lien_affilie}">Voir l\'offre</a>'
            if lien_affilie else ""
        )
        heure = time.strftime("%H:%M")

        return (
            badge_vitrine
            + badge_condition
            + f"\U0001f525 <b>BON PLAN {label_source}</b> \U0001f525\n"
            + sep + "\n"
            + f"\U0001f4e6 <b>Produit :</b> {titre}\n"
            + f"{ligne_prix}\n"
            + f"\U0001f3ea <b>Marchand :</b> {marchand}\n"
            + sep + "\n"
            + f"{slogan}"
            + lien_html + "\n\n"
            + f"\U0001f554 <i>Detecte a {heure}</i>\n"
            + hashtags
        )

    @staticmethod
    def _creer_boutons(lien_affilie: str, source: str) -> list:
        """
        Cree les boutons inline Telegram.
        Rangee 1 : bouton principal "Voir sur X"
        Rangee 2 : bouton "Partager a un ami"
        """
        labels = {
            "amazon":     "Voir sur Amazon",
            "amazon_api": "Voir sur Amazon",
            "cdiscount":  "Voir sur Cdiscount",
            "rakuten":    "Voir sur Rakuten",
            "dealabs":    "Voir le deal",
        }
        label = labels.get(source, "Voir l'offre")
        bouton_offre = InlineKeyboardButton(text=label, url=lien_affilie)

        url_partage = (
            f"https://t.me/share/url?url={quote_plus(lien_affilie)}"
        )
        bouton_partager = InlineKeyboardButton(
            text="Partager a un ami",
            url=url_partage
        )

        return [[bouton_offre], [bouton_partager]]


# =====================================================================
#  MODULE 5 — LE NETTOYEUR
# =====================================================================

class Nettoyeur:

    def nettoyer(self):
        nettoyer_db()
        self._nettoyer_historique_prix()

    def _nettoyer_historique_prix(self):
        if not os.path.exists(FICHIER_PRIX_HISTORIQUE):
            return
        limite = datetime.now() - timedelta(days=MAX_HISTORIQUE_JOURS)
        with open(FICHIER_PRIX_HISTORIQUE, "r", encoding="utf-8") as f:
            historique = json.load(f)
        historique_nettoye = {
            id_prod: [
                e for e in entrees
                if datetime.fromisoformat(e["date"]) >= limite
            ]
            for id_prod, entrees in historique.items()
        }
        historique_nettoye = {
            k: v for k, v in historique_nettoye.items() if v
        }
        with open(FICHIER_PRIX_HISTORIQUE, "w", encoding="utf-8") as f:
            json.dump(historique_nettoye, f, ensure_ascii=False, indent=2)
        logger.info(
            f"[Nettoyeur] {len(historique_nettoye)} produits conserves"
        )


# =====================================================================
#  ORCHESTRATEUR PRINCIPAL (VERSION ASYNC)
# =====================================================================

class BotAffiliation:

    def __init__(self, bot: Bot):
        self.scraper    = Scraper()
        self.generateur = GenerateurAffiliation()
        self.filtre     = Filtre()
        self.expediteur = ExpediteurTelegram(bot)
        self.nettoyeur  = Nettoyeur()
        
    def _scraper_synchrone(self) -> list:
        """Methode synchrone executee dans un thread separe."""
        tous_produits = []
        tous_produits += self.scraper.scraper_vitrine_amazon()
        tous_produits += self.scraper.scraper_amazon_ventes_flash()
        tous_produits += self.scraper.scraper_amazon_api()
        tous_produits += self.scraper.scraper_cdiscount_promos()
        tous_produits += self.scraper.scraper_rakuten()
        tous_produits += self.scraper.scraper_dealabs(
            "https://www.dealabs.com/rss/groupe/tech"
        )
        tous_produits += self.scraper.scraper_flux_rss(
            "https://www.frandroid.com/bons-plans/feed", "frandroid_promos"
        )
        tous_produits += self.scraper.scraper_flux_rss(
            "https://www.lesnumeriques.com/bons-plans/rss.xml",
            "lesnumeriques_promos"
        )
        tous_produits += self.scraper.scraper_flux_rss(
            "https://www.01net.com/bons-plans/feed/", "01net_promos"
        )
        tous_produits += self.scraper.scraper_flux_rss(
            "https://www.blogdumoderateur.com/deals/feed/", "bdm_deals"
        )
        tous_produits += self.scraper.scraper_flux_rss(
            "https://korben.info/feed", "korben"
        )
        return tous_produits    
    
    async def collecter_et_filtrer(self) -> list:
        """
        Collecte dans un thread separe pour ne pas bloquer Telegram.
        """
        # ← UNIQUEMENT ceci, plus rien d'autre pour le scraping
        tous_produits = await asyncio.to_thread(self._scraper_synchrone)

        logger.info(f"Total brut collecte : {len(tous_produits)}")

        candidats = []

        for produit in tous_produits:
            url         = produit.get("url", "")
            titre       = produit.get("titre", "")
            description = produit.get("description", "")
            source      = produit.get("source", "")
            est_vitrine = produit.get("vitrine", False)

            if not url or not titre:
                continue

            if not est_vitrine and est_deja_poste_db(url):
                continue

            if not est_vitrine and est_titre_deja_poste(titre):
                logger.info(f"Ignore (doublon cross-site) : {titre[:50]}")
                continue

            # ... reste du filtrage inchange 

            # ── FILTRE VOITURES/AUTO (v5.3) ─────────────────────────
            # Aucune promotion liee a l'automobile n'est publiee
            if self.filtre.est_produit_auto(titre, description):
                logger.info(f"Ignore (auto) : {titre[:50]}")
                continue

            sources_rss = (
                "frandroid_promos", "lesnumeriques_promos",
                "01net_promos", "bdm_deals", "korben",
                "journaldugeek_promos", "dealabs"
            )

            reduction = None

            if source in sources_rss:
                # Filtre articles inutiles
                if self.filtre.est_article_inutile(titre):
                    continue

                # Filtre prix obligatoire
                if not self.filtre.contient_prix(titre, description):
                    continue

                if source == "dealabs":
                    est_promo, valeur_remise = (
                        self.filtre.est_une_grosse_promo(
                            description, seuil=15
                        )
                    )
                    if not est_promo:
                        continue
                    reduction = float(valeur_remise)
                else:
                    if not self.filtre.est_produit_tech(titre):
                        continue
                    reduction = 0.0

            else:
                # Amazon / Cdiscount / Rakuten / Vitrine
                if not est_vitrine and not self.filtre.est_produit_tech(titre):
                    continue

                reduction = self.filtre.calculer_reduction(
                    produit.get("prix_actuel"),
                    produit.get("prix_original"),
                    url
                )

                if not est_vitrine and not self.filtre.est_bonne_affaire(reduction):
                    continue

            # Calcul du score de qualite
            score = self.filtre.calculer_score(produit, reduction)
            produit["score"]     = score
            produit["reduction"] = reduction or 0.0

            candidats.append(produit)

        logger.info(f"Candidats eligibles apres filtrage : {len(candidats)}")
        return candidats

    async def session_publication(self, heure_session: int):
        """
        Execute une session de publication complete.
        Selectionne le top MAX_OFFRES_PAR_SESSION selon le score,
        met en avant NB_SPOTLIGHT_AMAZON offres Amazon en tete.
        """
        logger.info("=" * 60)
        logger.info(
            f"SESSION {heure_session}h -- "
            f"{datetime.now().strftime('%d/%m/%Y %H:%M')}"
        )
        logger.info("=" * 60)

        # 1. Collecte et filtrage
        candidats = await self.collecter_et_filtrer()

        if not candidats:
            logger.warning("[Session] Aucun candidat eligible.")
            return

        # 2. Tri par score decroissant -> top MAX_OFFRES_PAR_SESSION
        candidats.sort(key=lambda p: p.get("score", 0), reverse=True)
        top_offres = candidats[:MAX_OFFRES_PAR_SESSION]

        logger.info(
            f"[Session] {len(top_offres)} offres selectionnees "
            f"sur {len(candidats)} candidats"
        )

        # 3. Extraction des meilleures offres Amazon pour le Spotlight
        top_amazon = [
            p for p in top_offres
            if "amazon" in p.get("source", "")
        ][:NB_SPOTLIGHT_AMAZON]

        # 4. Message d'introduction de la session
        await self.expediteur.publier_intro_session(heure_session)

        produits_publies = 0

        # 5. Publication des Spotlight Amazon en tete (les meilleures)
        for rang, produit in enumerate(top_amazon, start=1):
            url = produit.get("url", "")
            lien_affilie = self.generateur.generer_lien(url, produit["source"])
            lien_court   = self.generateur.raccourcir_lien(lien_affilie)
            produit["lien_court"] = lien_court

            succes = await self.expediteur.publier_spotlight_amazon(
                produit, rang
            )
            if succes:
                if not produit.get("vitrine"):
                    marquer_comme_poste_db(url)
                    marquer_titre_comme_poste(produit.get("titre", ""))
                produits_publies += 1
                logger.info(
                    f"[Spotlight #{rang}] Publie : {produit['titre'][:50]}"
                )
                await asyncio.sleep(5)

        # 6. Publication des offres restantes (sans spotlight)
        ids_spotlight = {id(p) for p in top_amazon}
        offres_standard = [
            p for p in top_offres if id(p) not in ids_spotlight
        ]

        for produit in offres_standard:
            url = produit.get("url", "")
            lien_affilie = self.generateur.generer_lien(url, produit["source"])
            lien_court   = self.generateur.raccourcir_lien(lien_affilie)

            succes = await self.expediteur.publier_offre_async(
                produit, lien_court, produit.get("reduction", 0)
            )
            if succes:
                if not produit.get("vitrine"):
                    marquer_comme_poste_db(url)
                    marquer_titre_comme_poste(produit.get("titre", ""))
                produits_publies += 1
                logger.info(f"[Offre] Publiee : {produit['titre'][:50]}")
                await asyncio.sleep(5)

        # 7. Nettoyage
        self.nettoyeur.nettoyer()
        logger.info(
            f"[Session {heure_session}h] Termine -- "
            f"{produits_publies} offre(s) publiee(s)\n"
        )


# =====================================================================
#  POINT D'ENTREE — LANCEMENT ASYNC
#
#  ARCHITECTURE v5.3 :
#    - Application PTB v20+ gere les commandes (/start)
#    - La boucle de scan surveille l'heure et publie a 13h et 20h
#    - Les deux tournent en parallele via asyncio
#
#  SECURITE :
#    - Toutes les donnees sensibles dans .env
#    - SQLite avec requetes parametrees (protection injection SQL)
#    - URLs encodees via quote_plus dans les boutons inline
#    - parse_mode=HTML (pas de MARKDOWN — plus sur)
#    - Tokens jamais loggues
# =====================================================================

async def lancer_scan(bot_affiliation: BotAffiliation):
    """
    Boucle de surveillance qui attend les heures de publication.
    Tourne en tache de fond independante du polling Telegram.
    En cas d'erreur dans une session, la boucle continue quand meme.
    """
    await bot_affiliation.expediteur.tester_connexion()

    sessions_executees = set()

    # Rattrapage si le bot demarre apres l'heure programmee
    maintenant = datetime.now()
    for heure in HEURES_PUBLICATION:
        if maintenant.hour == heure and maintenant.minute > 5:
            logger.info(
                f"[Planificateur] Demarrage tardif a "
                f"{maintenant.hour}:{maintenant.minute:02d} — "
                f"lancement immediat session {heure}h"
            )
            cle = f"{maintenant.date()}-{heure}"
            sessions_executees.add(cle)
            try:
                await bot_affiliation.session_publication(heure)
            except Exception as e:
                logger.error(f"[Session] Erreur session rattrapage : {e}")
            break

    while True:
        try:
            maintenant  = datetime.now()
            heure       = maintenant.hour
            minute      = maintenant.minute
            cle_session = f"{maintenant.date()}-{heure}"

            if (
                heure in HEURES_PUBLICATION
                and minute <= 30
                and cle_session not in sessions_executees
            ):
                logger.info(
                    f"[Planificateur] Lancement session {heure}h00"
                )
                sessions_executees.add(cle_session)
                try:
                    await bot_affiliation.session_publication(heure)
                except Exception as e:
                    # Une erreur dans la session ne tue pas la boucle
                    logger.error(
                        f"[Session] Erreur session {heure}h : {e}"
                    )

                if len(sessions_executees) > 100:
                    sessions_executees = set(
                        list(sessions_executees)[-50:]
                    )

        except Exception as e:
            logger.error(f"[Planificateur] Erreur inattendue : {e}")

        await asyncio.sleep(30)


async def main():
    logger.info("Demarrage du Bot d'Affiliation Telegram v5.3")
    logger.info(
        f"Sessions : {HEURES_PUBLICATION[0]}h00 et "
        f"{HEURES_PUBLICATION[1]}h00"
    )
    logger.info(f"Top {MAX_OFFRES_PAR_SESSION} offres par session")
    logger.info(
        f"Spotlight : {NB_SPOTLIGHT_AMAZON} meilleures offres Amazon"
    )

    init_db()

    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .build()
    )
    application.add_handler(CommandHandler("start", start_command))

    bot_affiliation = BotAffiliation(application.bot)

    await application.initialize()
    await application.start()
    await application.updater.start_polling(
        allowed_updates=["message"]
    )
    logger.info("[Bot] Ecoute des commandes active (/start)")

    # CORRECTION CLE : lancer_scan tourne comme tache independante
    # asyncio.create_task evite qu'une erreur de scan bloque le polling
    # et evite que le polling bloque le scan
    scan_task = asyncio.create_task(lancer_scan(bot_affiliation))

    try:
        # Attend indefiniment — le bot tourne jusqu'a KeyboardInterrupt
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Signal d'arret recu.")
    finally:
        scan_task.cancel()
        try:
            await scan_task
        except asyncio.CancelledError:
            pass
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        logger.info("Bot arrete proprement.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot arrete manuellement.")
    except ValueError as e:
        logger.critical(f"Erreur de configuration : {e}")
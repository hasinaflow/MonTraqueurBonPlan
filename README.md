# 🤖 OptiDeals - Bot Traqueur de Bons Plans Tech

![Version](https://img.shields.io/badge/Version-1.0.0-orange)
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![License](https://img.shields.io/badge/License-MIT-green)

**OptiDeals** est un bot automatisé conçu pour scanner les flux RSS et les sites de e-commerce (Amazon, Cdiscount, Fnac...) afin de détecter les meilleures offres High-Tech en temps réel et les poster instantanément sur un canal Telegram.

---

## ✨ Fonctionnalités

* **Scan 24h/24 :** Analyse automatique des flux via `feedparser`.
* **Filtre Intelligent :** Ignore les articles d'actualité, les tests et les avis pour ne garder que les vraies promos.
* **Anti-Doublon :** Utilisation d'une base de données **SQLite** pour éviter de reposter le même deal.
* **Affiliation Automatique :** Injection automatique de ton tag partenaire dans les liens.
* **Interface Visuelle :** Messages formatés en Markdown avec boutons d'action (Acheter / Partager).

---

## 🛠️ Installation & Configuration

### 1. Prérequis
* Python 3.10 ou supérieur.
* Un token de bot Telegram (obtenu via [@BotFather](https://t.me/botfather)).
* Un ID de canal Telegram.

### 2. Installation
Clonez le dépôt et installez les dépendances :
```bash
git clone [https://github.com/hasinaflow/OptiDeals.git](https://github.com/hasinaflow/OptiDeals.git)
cd OptiDeals
pip install -r requirements.txt

3. Configuration (.env)
Créez un fichier .env à la racine du projet et ajoutez vos accès 

TELEGRAM_BOT_TOKEN=votre_token_ici
TELEGRAM_CHANNEL_ID=-100*******
TELEGRAM_CANAL_USERNAME=@votre_canal_ici
AFFILIATE_TAG=votre_tag_ici

🚀 Utilisation
Pour lancer le bot en local :

affiliate_bot.py

Le bot va créer automatiquement un fichier database.db pour gérer l'historique des posts.
📈 Stratégie Marketing
Le projet inclut une stratégie de croissance via TikTok/Reels :
Template CapCut : Structure de 7 secondes optimisée pour la rétention.
Hooks : Accroches psychologiques pour transformer les vues en abonnés.
Filtre de Qualité : Uniquement les remises > 15% pour garantir l'intérêt de l'audience.
🛡️ Sécurité
Le fichier .env et la base de données database.db sont exclus du suivi Git via le fichier .gitignore pour protéger vos clés API et vos données locales.
📝 Licence
Ce projet est sous licence MIT. Libre à vous de l'utiliser et de le modifier !
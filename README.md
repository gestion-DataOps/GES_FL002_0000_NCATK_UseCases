# NCA Toolkit

Plateforme de use cases automatisés autour du NCA Toolkit.

`n8n` `Python` `FastAPI` `MinIO` `Docker`

---

## Contenu du repo

Ce dépôt regroupe les premiers use cases construits autour du **NCA Toolkit**. Deux use cases sont actuellement implémentés :

1. **Démo n8n** : un workflow permettant de tester l'ensemble des outils disponibles dans le NCA Toolkit, directement depuis l'interface n8n.
2. **Transcription vidéo** : un pipeline complet qui prend en entrée un fichier vidéo local, le stocke via MinIO, puis le transcrit grâce à un service Python exposé en FastAPI.

L'ensemble est encapsulé dans **Docker**, avec un fichier `.env` à configurer pour personnaliser l'arborescence et les accès. Les fichiers transcrits sont rangés dans une arborescence de sortie dédiée, prête à être exploitée.

Crédits pour la démo n8n : Benjamin Keating : https://www.youtube.com/watch?v=RRqdDC43kSE

---

## Objectif

L'objectif est de fournir une base modulaire et facilement déployable pour explorer les capacités du NCA Toolkit à travers des cas d'usage concrets. Chaque use case est indépendant, documenté et prêt à être étendu.

L'installation se veut rapide : cloner le repo, renseigner le fichier `.env`, lancer `docker compose up`. L'arborescence des sorties (transcriptions, logs) est générée automatiquement à partir des variables d'environnement, ce qui facilite l'intégration dans n'importe quel workflow existant.

---

## MinIO

**MinIO** est utilisé comme couche de stockage objet compatible S3. Il sert de point de dépôt intermédiaire pour les fichiers vidéo avant traitement.

Le flux est simple : l'utilisateur dépose un fichier vidéo depuis son poste local, MinIO le stocke dans un bucket dédié, puis la FastAPI vient le récupérer pour lancer la transcription.

MinIO tourne en tant que service Docker dans le `docker-compose.yml`. Les variables d'accès (`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_BUCKET`) sont à définir dans le `.env`. Une interface web d'administration est accessible sur le port `9001` pour visualiser et gérer les fichiers uploadés.

---

## NCA Toolkit

Le **NCA Toolkit** est le cœur du projet. Il expose un ensemble d'outils (traitement média, transcription, manipulation de fichiers, etc.) orchestrés via n8n ou appelés directement depuis la FastAPI.

Le use case de démo n8n permet de parcourir tous les outils disponibles via un workflow structuré, idéal pour onboarder rapidement de nouveaux utilisateurs ou valider l'installation. Chaque outil est appelé avec des données d'exemple et les résultats sont loggés dans le workflow pour inspection.

---

## FastAPI

Le service **FastAPI** constitue la couche applicative Python du pipeline de transcription. Il expose un endpoint qui reçoit une référence vers un fichier stocké dans MinIO, orchestre le téléchargement, lance la transcription via le NCA Toolkit, puis écrit le résultat dans l'arborescence de sortie configurée.

Le service est conteneurisé et démarre automatiquement avec `docker compose`. La documentation interactive (Swagger UI) est disponible sur `/docs` une fois le conteneur lancé. Les paramètres (URL MinIO, bucket cible, dossier de sortie) sont injectés via les variables d'environnement du `.env`.

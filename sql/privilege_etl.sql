-- setup_privileges.sql
-- Configuration des privilèges selon le principe du moindre privilège.

-- Rôle pour la pipeline ETL (lecture, écriture/mise à jour)
-- REMPLACER 'votre_mdp_etl' par un mot de passe sécurisé.
CREATE USER etl_user WITH PASSWORD '12345'; 

--  Accès au rôle ETL (etl_user)
-- Nécessite SELECT (pour l'UPSERT), INSERT et UPDATE sur la table cible.
GRANT SELECT, INSERT, UPDATE ON consolidated_measurements TO etl_user;


/*=============================================================================
  DistrictPilot AI - Database Rename
  00_rename_database.sql

  Run this ONCE before deploying the rebranded code.
  All v7 code references DISTRICTPILOT_AI.ANALYTICS.*
=============================================================================*/

USE ROLE ACCOUNTADMIN;

-- Step 1: Rename the database
ALTER DATABASE MOVESIGNAL_AI RENAME TO DISTRICTPILOT_AI;

-- Step 2: Verify
SHOW DATABASES LIKE 'DISTRICTPILOT_AI';

-- Step 3: Update connection defaults (run in worksheet)
-- ALTER USER EHDJS0836 SET DEFAULT_DATABASE = 'DISTRICTPILOT_AI';

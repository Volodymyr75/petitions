"""
Validator module for petition data integrity checks.
Used by cloud_sync.py to ensure data quality before and after sync.
"""

import duckdb
import random
from scraper_detail import fetch_petition_detail


class ValidationResult:
    def __init__(self):
        self.passed = True
        self.errors = []
        self.warnings = []
    
    def add_error(self, msg):
        self.errors.append(msg)
        self.passed = False
    
    def add_warning(self, msg):
        self.warnings.append(msg)
    
    def summary(self):
        status = "✅ PASSED" if self.passed else "❌ FAILED"
        lines = [status]
        if self.errors:
            lines.append(f"Errors ({len(self.errors)}):")
            lines.extend([f"  - {e}" for e in self.errors])
        if self.warnings:
            lines.append(f"Warnings ({len(self.warnings)}):")
            lines.extend([f"  - {w}" for w in self.warnings])
        return "\n".join(lines)


def get_dynamic_markers(con):
    """
    Select 5 sample petitions from the database for verification:
    - 2 top active (high traffic)
    - 2 random archived
    - 1 with answer
    """
    markers = []
    
    # 1. 2 active petitions with most votes (likely most stable/known)
    active = con.execute("""
        SELECT external_id, votes, status 
        FROM petitions 
        WHERE status = 'Триває збір підписів' AND source = 'president'
        ORDER BY votes DESC LIMIT 2
    """).fetchall()
    
    # 2. 2 random archived petitions
    archived = con.execute("""
        SELECT external_id, votes, status 
        FROM petitions 
        WHERE status = 'Архів' AND source = 'president'
        USING SAMPLE 2
    """).fetchall()
    
    # 3. 1 answered petition
    answered = con.execute("""
        SELECT external_id, votes, status 
        FROM petitions 
        WHERE (status = 'З відповіддю' OR status = 'Розглянуто') AND source = 'president'
        LIMIT 1
    """).fetchone()

    # Combine
    for row in active: markers.append({"id": row[0], "db_votes": row[1], "db_status": row[2], "type": "active"})
    for row in archived: markers.append({"id": row[0], "db_votes": row[1], "db_status": row[2], "type": "archive"})
    if answered: markers.append({"id": answered[0], "db_votes": answered[1], "db_status": answered[2], "type": "answered"})
    
    return markers


def run_preflight_check(con, verbose=True):
    """
    Test a few petitions from DB to verify the scraper is still alive and compatible.
    Returns ValidationResult.
    """
    result = ValidationResult()
    
    if verbose:
        print("🔍 Running dynamic pre-flight check...")
    
    markers = get_dynamic_markers(con)
    if not markers:
        result.add_error("Pre-flight: No petitions found in database to use as markers")
        return result

    failed_count = 0
    passed_count = 0
    
    for marker in markers:
        pet_id = marker["id"]
        data = fetch_petition_detail(pet_id)
        
        if not data or "error" in data:
            result.add_warning(f"Petition {pet_id} ({marker['type']}): Failed to fetch (404/Timeout)")
            failed_count += 1
            continue
        
        # Validation Logic:
        # 1. Status must be recognized
        status = data.get("status", "Unknown")
        if status == "Unknown":
            result.add_error(f"Petition {pet_id}: Scraper returned 'Unknown' status")
            failed_count += 1
            continue
            
        # 2. Votes should not drop significantly (unless it's some rare site error)
        new_votes = data.get("votes", 0)
        db_votes = marker["db_votes"]
        if new_votes < db_votes * 0.95: # Allow 5% margin for rare edge cases / re-counts
            result.add_error(f"Petition {pet_id}: Votes dropped significantly! DB: {db_votes}, Web: {new_votes}")
            failed_count += 1
            continue
            
        # 3. Text length should be > 0
        if data.get("text_length", 0) == 0:
            result.add_error(f"Petition {pet_id}: Scraper returned text_length = 0")
            failed_count += 1
            continue
            
        if verbose:
            print(f"   ✓ Petition {pet_id} ({marker['type']}): OK (status={status}, votes={new_votes})")
        passed_count += 1
    
    # Pass threshold: at least 3 out of 5 markers must pass for the sync to proceed. 
    # This allows for some 404s/temporary site issues but catches complete scraper failure.
    if passed_count < 3:
        result.add_error(f"Critical failure: only {passed_count}/{len(markers)} marker(s) passed validation")
    elif failed_count > 0:
        result.add_warning(f"Validation partial: {passed_count}/{len(markers)} markers passed, {failed_count} skipped/failed")
    
    if verbose:
        print(result.summary())
    
    return result


def run_postsync_validation(con, stats, verbose=True):
    """
    Validate sync results after execution.
    """
    result = ValidationResult()
    
    if verbose:
        print("🔍 Running post-sync validation...")
    
    # Check 1: No new Unknown statuses
    unknown_count = con.execute(
        "SELECT COUNT(*) FROM petitions WHERE status = 'Unknown'"
    ).fetchone()[0]
    
    if unknown_count > 0:
        result.add_error(f"Found {unknown_count} petitions with status='Unknown'")
    
    # Check 2: No active petitions with 0 votes
    zero_votes = con.execute("""
        SELECT COUNT(*) FROM petitions 
        WHERE status = 'Триває збір підписів' AND votes = 0
    """).fetchone()[0]
    
    if zero_votes > 0:
        result.add_error(f"Found {zero_votes} active petitions with votes=0")
    
    # Check 3: Error rate
    total_checked = stats.get("total_checked", 0)
    errors = stats.get("errors", 0)
    if total_checked > 0:
        error_rate = errors / total_checked
        if error_rate > 0.4: # Increased threshold for post-sync, keep warnings tighter
            result.add_error(f"Error rate too high: {error_rate:.1%} (>{40}%)")
        elif error_rate > 0.15:
            result.add_warning(f"Error rate elevated: {error_rate:.1%}")
    
    if verbose:
        print(result.summary())
    
    return result

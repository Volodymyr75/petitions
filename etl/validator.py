"""
Validator module for petition data integrity checks.
Used by cloud_sync.py to ensure data quality before and after sync.
"""

import duckdb
from scraper_detail import fetch_petition_detail

# Marker petitions for pre-flight validation
# These are known, stable petitions with expected values
MARKER_PETITIONS = [
    {"id": 257906, "min_votes": 1, "expected_status_contains": ["Триває", "На розгляді", "Архів"]},
    {"id": 257890, "min_votes": 1, "expected_status_contains": ["Триває", "На розгляді", "Архів"]},
    {"id": 254188, "min_votes": 25000, "expected_status_contains": ["На розгляді", "З відповіддю"]},
    {"id": 253774, "min_votes": 25000, "expected_status_contains": ["На розгляді", "З відповіддю"]},
    {"id": 250000, "min_votes": 1, "expected_status_contains": ["Архів", "З відповіддю"]},
]


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


def run_preflight_check(verbose=True):
    """
    Test a few known petitions to verify the scraper is working correctly.
    Returns ValidationResult.
    """
    result = ValidationResult()
    failed_count = 0
    
    if verbose:
        print("🔍 Running pre-flight check...")
    
    for marker in MARKER_PETITIONS:
        pet_id = marker["id"]
        data = fetch_petition_detail(pet_id)
        
        if not data or "error" in data:
            result.add_error(f"Petition {pet_id}: Failed to fetch (404 or timeout)")
            failed_count += 1
            continue
        
        # Check votes
        if data.get("votes", 0) < marker["min_votes"]:
            result.add_error(f"Petition {pet_id}: votes={data.get('votes')} (expected >= {marker['min_votes']})")
            failed_count += 1
            continue
        
        # Check status
        status = data.get("status", "")
        if status == "Unknown":
            result.add_error(f"Petition {pet_id}: status is 'Unknown'")
            failed_count += 1
            continue
        
        # Check text length
        if data.get("text_length", 0) == 0:
            result.add_error(f"Petition {pet_id}: text_length is 0")
            failed_count += 1
            continue
        
        if verbose:
            print(f"   ✓ Petition {pet_id}: OK (votes={data['votes']}, status={status})")
    
    # If more than 2 markers failed, consider it a critical failure
    if failed_count >= 2:
        result.add_error(f"Pre-flight check failed: {failed_count}/{len(MARKER_PETITIONS)} markers failed")
    
    if verbose:
        print(result.summary())
    
    return result


def run_postsync_validation(con, stats, verbose=True):
    """
    Validate sync results after execution.
    
    Args:
        con: DuckDB connection
        stats: Dictionary with sync statistics
        verbose: Print details
    
    Returns:
        ValidationResult
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
    
    # Check 3: Error rate (if available in stats)
    total_checked = stats.get("total_checked", 0)
    errors = stats.get("errors", 0)
    if total_checked > 0:
        error_rate = errors / total_checked
        if error_rate > 0.2:
            result.add_error(f"Error rate too high: {error_rate:.1%} (>{20}%)")
        elif error_rate > 0.1:
            result.add_warning(f"Error rate elevated: {error_rate:.1%}")
    
    # Check 4: Vote delta sanity (should not be massively negative)
    vote_delta = stats.get("vote_delta", 0)
    if vote_delta < -10000:
        result.add_error(f"Suspicious vote delta: {vote_delta} (too negative)")
    
    if verbose:
        print(result.summary())
    
    return result


if __name__ == "__main__":
    # Test pre-flight check
    print("=" * 50)
    print("Testing Pre-flight Check")
    print("=" * 50)
    result = run_preflight_check(verbose=True)
    print(f"\nFinal result: {'PASS' if result.passed else 'FAIL'}")

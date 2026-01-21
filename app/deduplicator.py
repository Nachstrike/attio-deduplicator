"""
CSV Deduplication Engine
- Fuzzy name matching (80%+ similarity)
- Email username matching (ignores domain)
- Company-aware grouping with flagging
"""

import csv
import io
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
from rapidfuzz import fuzz


@dataclass
class DuplicateGroup:
    """A group of records that are potential duplicates."""
    master_record: dict
    duplicates: list[dict]
    merge_type: str  # 'auto' or 'flagged'
    reason: str
    merged_data: dict = field(default_factory=dict)


def normalize_name(name: str) -> str:
    """Normalize a name for comparison."""
    if not name:
        return ""
    return " ".join(name.lower().strip().split())


def extract_email_username(email: str) -> str:
    """Extract username from email, ignoring domain."""
    if not email or "@" not in email:
        return ""
    return email.split("@")[0].lower().strip()


def get_all_emails(record: dict) -> list[str]:
    """Extract all email addresses from a record."""
    emails = []
    for key, value in record.items():
        if not value:
            continue
        key_lower = key.lower()
        if 'email' in key_lower:
            if isinstance(value, str) and "@" in value:
                emails.append(value.lower().strip())
            elif isinstance(value, list):
                for v in value:
                    if isinstance(v, str) and "@" in v:
                        emails.append(v.lower().strip())
    return emails


def get_email_usernames(record: dict) -> set[str]:
    """Get all email usernames from a record."""
    emails = get_all_emails(record)
    usernames = set()
    for email in emails:
        username = extract_email_username(email)
        if username:
            usernames.add(username)
    return usernames


def get_name_field(record: dict) -> str:
    """Find and return the name field from a record."""
    name_keys = ['name', 'full_name', 'fullname', 'contact_name', 'person_name']
    for key in record.keys():
        if key.lower() in name_keys:
            return str(record[key] or "")
    # Try to combine first + last name
    first = ""
    last = ""
    for key in record.keys():
        key_lower = key.lower()
        if 'first' in key_lower and 'name' in key_lower:
            first = str(record[key] or "")
        elif 'last' in key_lower and 'name' in key_lower:
            last = str(record[key] or "")
    if first or last:
        return f"{first} {last}".strip()
    return ""


def get_company_field(record: dict) -> str:
    """Find and return the company field from a record."""
    company_keys = ['company', 'company_name', 'organization', 'org', 'employer']
    for key in record.keys():
        if key.lower() in company_keys:
            return str(record[key] or "").strip()
    return ""


def score_record_completeness(record: dict) -> int:
    """Score a record based on data completeness. Higher = more complete."""
    score = 0
    for key, value in record.items():
        if not value:
            continue
        key_lower = key.lower()
        if 'email' in key_lower:
            score += 10
        elif 'phone' in key_lower or 'mobile' in key_lower:
            score += 5
        elif 'company' in key_lower or 'organization' in key_lower:
            score += 5
        elif 'title' in key_lower or 'job' in key_lower or 'position' in key_lower:
            score += 3
        elif 'linkedin' in key_lower:
            score += 2
        elif 'address' in key_lower or 'location' in key_lower:
            score += 2
        elif value:
            score += 1
    return score


def names_match(name1: str, name2: str, threshold: int = 80) -> bool:
    """Check if two names match using fuzzy matching."""
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    if not n1 or not n2:
        return False
    # Exact match
    if n1 == n2:
        return True
    # Fuzzy match
    ratio = fuzz.ratio(n1, n2)
    return ratio >= threshold


def emails_match(record1: dict, record2: dict) -> bool:
    """Check if two records share an email username."""
    usernames1 = get_email_usernames(record1)
    usernames2 = get_email_usernames(record2)
    if not usernames1 or not usernames2:
        return False
    return bool(usernames1 & usernames2)


def merge_records(master: dict, duplicates: list[dict]) -> dict:
    """Merge data from duplicates into master record."""
    merged = {}
    for key in master.keys():
        master_value = master.get(key)
        if master_value:
            merged[key] = master_value
            continue
        # Try to fill from duplicates
        for dup in duplicates:
            dup_value = dup.get(key)
            if dup_value:
                merged[key] = dup_value
                break
        if key not in merged:
            merged[key] = master_value

    # Merge email addresses (combine unique ones)
    master_emails = set(get_all_emails(master))
    for dup in duplicates:
        master_emails.update(get_all_emails(dup))

    # Find the email column and update if we have more emails
    for key in master.keys():
        if 'email' in key.lower() and master_emails:
            # If it's a list-like field, add all emails
            merged[key] = list(master_emails)[0] if len(master_emails) == 1 else ", ".join(sorted(master_emails))

    return merged


def find_duplicates(records: list[dict]) -> tuple[list[DuplicateGroup], list[dict]]:
    """
    Find duplicate records in the dataset.

    Returns:
        - List of DuplicateGroup objects
        - List of records with no duplicates (clean records)
    """
    if not records:
        return [], []

    # Index records for efficient lookup
    processed = set()
    duplicate_groups = []
    clean_records = []

    # Build candidate pairs based on name similarity and email username matching
    candidates = defaultdict(list)

    for i, record in enumerate(records):
        name = normalize_name(get_name_field(record))
        if name:
            # Use first 3 chars of name as bucket key for efficiency
            bucket_key = name[:3] if len(name) >= 3 else name
            candidates[bucket_key].append(i)

        # Also bucket by email username
        for username in get_email_usernames(record):
            if username:
                candidates[f"email:{username}"].append(i)

    # Find groups of duplicates
    for i, record in enumerate(records):
        if i in processed:
            continue

        record_name = get_name_field(record)
        record_company = get_company_field(record)

        # Find all potential matches
        potential_matches = set()
        name_normalized = normalize_name(record_name)

        if name_normalized:
            bucket_key = name_normalized[:3] if len(name_normalized) >= 3 else name_normalized
            for j in candidates.get(bucket_key, []):
                if j != i and j not in processed:
                    potential_matches.add(j)

        for username in get_email_usernames(record):
            for j in candidates.get(f"email:{username}", []):
                if j != i and j not in processed:
                    potential_matches.add(j)

        # Check each potential match
        auto_merge = []
        flagged = []

        for j in potential_matches:
            other = records[j]
            other_name = get_name_field(other)
            other_company = get_company_field(other)

            # Check if they match by name or email
            name_matches = names_match(record_name, other_name)
            email_matches = emails_match(record, other)

            if not name_matches and not email_matches:
                continue

            # Determine merge type based on company
            if not record_company or not other_company:
                # One or both have no company - auto merge
                auto_merge.append((j, "No company on one/both records"))
            elif record_company.lower() == other_company.lower():
                # Same company - auto merge
                auto_merge.append((j, "Same company"))
            else:
                # Different companies - flag for user decision
                flagged.append((j, f"Different companies: '{record_company}' vs '{other_company}'"))

        # Create duplicate groups
        if auto_merge or flagged:
            # Process auto-merge group
            if auto_merge:
                all_in_group = [record] + [records[j] for j, _ in auto_merge]
                # Find master (highest completeness score)
                scored = [(score_record_completeness(r), idx, r) for idx, r in enumerate(all_in_group)]
                scored.sort(reverse=True)
                master = scored[0][2]
                dups = [r for _, idx, r in scored[1:]]

                merged_data = merge_records(master, dups)

                duplicate_groups.append(DuplicateGroup(
                    master_record=master,
                    duplicates=dups,
                    merge_type='auto',
                    reason=auto_merge[0][1],
                    merged_data=merged_data
                ))

                processed.add(i)
                for j, _ in auto_merge:
                    processed.add(j)

            # Process flagged group separately
            if flagged:
                for j, reason in flagged:
                    if j not in processed:
                        other = records[j]
                        # Determine master by score
                        if score_record_completeness(record) >= score_record_completeness(other):
                            master, dup = record, other
                        else:
                            master, dup = other, record

                        duplicate_groups.append(DuplicateGroup(
                            master_record=master,
                            duplicates=[dup],
                            merge_type='flagged',
                            reason=reason,
                            merged_data={}  # Don't merge flagged records
                        ))

                        if i not in processed:
                            processed.add(i)
                        processed.add(j)

        if i not in processed:
            clean_records.append(record)
            processed.add(i)

    return duplicate_groups, clean_records


def process_csv(csv_content: str) -> dict:
    """
    Process a CSV file and find duplicates.

    Returns a dict with:
        - total_records: int
        - duplicate_groups: list of DuplicateGroup info
        - auto_merge_count: int
        - flagged_count: int
        - clean_count: int
        - master_csv: str (CSV content of master records with merged data)
        - duplicates_csv: str (CSV content of records to delete)
    """
    # Parse CSV
    reader = csv.DictReader(io.StringIO(csv_content))
    records = list(reader)
    fieldnames = reader.fieldnames or []

    if not records:
        return {
            'total_records': 0,
            'duplicate_groups': [],
            'auto_merge_count': 0,
            'flagged_count': 0,
            'clean_count': 0,
            'master_csv': '',
            'duplicates_csv': ''
        }

    # Find duplicates
    duplicate_groups, clean_records = find_duplicates(records)

    # Separate auto-merge and flagged
    auto_merge_groups = [g for g in duplicate_groups if g.merge_type == 'auto']
    flagged_groups = [g for g in duplicate_groups if g.merge_type == 'flagged']

    # Build master CSV (clean records + merged masters)
    master_records = []
    for record in clean_records:
        master_records.append(record)

    for group in auto_merge_groups:
        master_records.append(group.merged_data)

    # For flagged groups, include both records with a flag
    flagged_fieldnames = fieldnames + ['_duplicate_flag', '_duplicate_reason']
    flagged_records = []

    for group in flagged_groups:
        master_with_flag = dict(group.master_record)
        master_with_flag['_duplicate_flag'] = 'POTENTIAL_DUPLICATE'
        master_with_flag['_duplicate_reason'] = group.reason
        flagged_records.append(master_with_flag)

        for dup in group.duplicates:
            dup_with_flag = dict(dup)
            dup_with_flag['_duplicate_flag'] = 'POTENTIAL_DUPLICATE'
            dup_with_flag['_duplicate_reason'] = group.reason
            flagged_records.append(dup_with_flag)

    # Build duplicates CSV (records to delete)
    duplicates_to_delete = []
    for group in auto_merge_groups:
        for dup in group.duplicates:
            dup_with_master = dict(dup)
            dup_with_master['_merged_into'] = get_name_field(group.master_record)
            duplicates_to_delete.append(dup_with_master)

    # Generate CSV strings
    master_output = io.StringIO()
    if master_records:
        writer = csv.DictWriter(master_output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(master_records)
    master_csv = master_output.getvalue()

    duplicates_output = io.StringIO()
    if duplicates_to_delete:
        dup_fieldnames = fieldnames + ['_merged_into']
        writer = csv.DictWriter(duplicates_output, fieldnames=dup_fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(duplicates_to_delete)
    duplicates_csv = duplicates_output.getvalue()

    flagged_output = io.StringIO()
    if flagged_records:
        writer = csv.DictWriter(flagged_output, fieldnames=flagged_fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(flagged_records)
    flagged_csv = flagged_output.getvalue()

    # Build summary of duplicate groups for preview
    groups_summary = []
    for group in duplicate_groups:
        groups_summary.append({
            'master_name': get_name_field(group.master_record),
            'master_company': get_company_field(group.master_record),
            'master_email': get_all_emails(group.master_record)[0] if get_all_emails(group.master_record) else '',
            'duplicate_count': len(group.duplicates),
            'merge_type': group.merge_type,
            'reason': group.reason,
            'duplicates': [
                {
                    'name': get_name_field(d),
                    'company': get_company_field(d),
                    'email': get_all_emails(d)[0] if get_all_emails(d) else ''
                }
                for d in group.duplicates
            ]
        })

    return {
        'total_records': len(records),
        'duplicate_groups': groups_summary,
        'auto_merge_count': sum(len(g.duplicates) for g in auto_merge_groups),
        'flagged_count': len(flagged_records),
        'clean_count': len(clean_records),
        'master_csv': master_csv,
        'duplicates_csv': duplicates_csv,
        'flagged_csv': flagged_csv
    }

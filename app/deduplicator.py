"""
CSV Deduplication Engine
- Fuzzy name matching (80%+ similarity)
- Email matching (same username + same domain, ignoring TLD like .com/.es/.uk)
- Company-aware grouping with flagging
"""

import csv
import io
import re
from collections import defaultdict
from dataclasses import dataclass, field
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


def extract_email_parts(email: str) -> tuple[str, str]:
    """
    Extract username and domain base from email.
    nacho@google.com -> ('nacho', 'google')
    nacho@google.es -> ('nacho', 'google')
    nacho@mail.google.co.uk -> ('nacho', 'google')
    """
    if not email or "@" not in email:
        return ("", "")

    parts = email.lower().strip().split("@")
    if len(parts) != 2:
        return ("", "")

    username = parts[0]
    domain = parts[1]

    # Remove TLD and extract main domain
    # google.com -> google
    # google.co.uk -> google
    # mail.google.com -> google
    domain_parts = domain.split(".")

    # Filter out common TLDs and subdomains
    tlds = {'com', 'es', 'uk', 'de', 'fr', 'it', 'nl', 'be', 'org', 'net', 'io', 'co', 'ai', 'app'}
    subdomains = {'mail', 'email', 'smtp', 'www'}

    # Get the main domain part
    main_parts = [p for p in domain_parts if p not in tlds and p not in subdomains]
    domain_base = main_parts[0] if main_parts else domain_parts[0]

    return (username, domain_base)


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


def get_email_signatures(record: dict) -> set[str]:
    """
    Get email signatures (username+domain) for matching.
    Returns set of 'username:domain' strings.
    """
    emails = get_all_emails(record)
    signatures = set()
    for email in emails:
        username, domain = extract_email_parts(email)
        if username and domain:
            signatures.add(f"{username}:{domain}")
    return signatures


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


def names_match(name1: str, name2: str, threshold: int = 85) -> bool:
    """Check if two names match using fuzzy matching."""
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    if not n1 or not n2:
        return False
    if n1 == n2:
        return True
    # For short names, require higher similarity to avoid false positives
    # "Person 1" vs "Person 10" would wrongly match at 80%
    min_len = min(len(n1), len(n2))
    if min_len < 10:
        threshold = 95  # Very strict for short names
    ratio = fuzz.ratio(n1, n2)
    return ratio >= threshold


def emails_match(record1: dict, record2: dict) -> bool:
    """
    Check if two records have matching emails.
    Matches if same username AND same domain base (ignoring TLD).
    nacho@google.com matches nacho@google.es
    nacho@google.com does NOT match nacho@microsoft.com
    """
    sigs1 = get_email_signatures(record1)
    sigs2 = get_email_signatures(record2)
    if not sigs1 or not sigs2:
        return False
    return bool(sigs1 & sigs2)


def merge_records(master: dict, duplicates: list[dict]) -> dict:
    """Merge data from duplicates into master record."""
    merged = {}
    for key in master.keys():
        master_value = master.get(key)
        if master_value:
            merged[key] = master_value
            continue
        for dup in duplicates:
            dup_value = dup.get(key)
            if dup_value:
                merged[key] = dup_value
                break
        if key not in merged:
            merged[key] = master_value

    # Merge email addresses
    master_emails = set(get_all_emails(master))
    for dup in duplicates:
        master_emails.update(get_all_emails(dup))

    for key in master.keys():
        if 'email' in key.lower() and master_emails:
            merged[key] = list(master_emails)[0] if len(master_emails) == 1 else ", ".join(sorted(master_emails))

    return merged


def find_duplicates(records: list[dict]) -> tuple[list[DuplicateGroup], list[dict]]:
    """Find duplicate records in the dataset."""
    if not records:
        return [], []

    processed = set()
    duplicate_groups = []
    clean_records = []

    # Build candidate buckets
    candidates = defaultdict(list)

    for i, record in enumerate(records):
        name = normalize_name(get_name_field(record))
        if name:
            bucket_key = name[:3] if len(name) >= 3 else name
            candidates[bucket_key].append(i)

        for sig in get_email_signatures(record):
            candidates[f"email:{sig}"].append(i)

    for i, record in enumerate(records):
        if i in processed:
            continue

        record_name = get_name_field(record)
        record_company = get_company_field(record)

        potential_matches = set()
        name_normalized = normalize_name(record_name)

        if name_normalized:
            bucket_key = name_normalized[:3] if len(name_normalized) >= 3 else name_normalized
            for j in candidates.get(bucket_key, []):
                if j != i and j not in processed:
                    potential_matches.add(j)

        for sig in get_email_signatures(record):
            for j in candidates.get(f"email:{sig}", []):
                if j != i and j not in processed:
                    potential_matches.add(j)

        auto_merge = []
        flagged = []

        for j in potential_matches:
            other = records[j]
            other_name = get_name_field(other)
            other_company = get_company_field(other)

            name_matches = names_match(record_name, other_name)
            email_matches = emails_match(record, other)

            if not name_matches and not email_matches:
                continue

            if not record_company or not other_company:
                auto_merge.append((j, "No company on one/both"))
            elif record_company.lower() == other_company.lower():
                auto_merge.append((j, "Same company"))
            else:
                flagged.append((j, f"Different companies: {record_company} vs {other_company}"))

        if auto_merge or flagged:
            if auto_merge:
                all_in_group = [record] + [records[j] for j, _ in auto_merge]
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

            if flagged:
                for j, reason in flagged:
                    if j not in processed:
                        other = records[j]
                        if score_record_completeness(record) >= score_record_completeness(other):
                            master, dup = record, other
                        else:
                            master, dup = other, record

                        duplicate_groups.append(DuplicateGroup(
                            master_record=master,
                            duplicates=[dup],
                            merge_type='flagged',
                            reason=reason,
                            merged_data={}
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

    Returns 2 CSVs:
    - master_csv: Clean records + merged masters + flagged records (with _status column)
    - duplicates_csv: Records to delete from Attio
    """
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

    duplicate_groups, clean_records = find_duplicates(records)

    auto_merge_groups = [g for g in duplicate_groups if g.merge_type == 'auto']
    flagged_groups = [g for g in duplicate_groups if g.merge_type == 'flagged']

    # Build master CSV with status column
    output_fieldnames = fieldnames + ['_status', '_note']
    master_records = []

    # Add clean records
    for record in clean_records:
        r = dict(record)
        r['_status'] = 'clean'
        r['_note'] = ''
        master_records.append(r)

    # Add merged masters
    for group in auto_merge_groups:
        r = dict(group.merged_data)
        r['_status'] = 'merged'
        r['_note'] = f'Merged {len(group.duplicates)} duplicate(s)'
        master_records.append(r)

    # Add flagged records (both sides, for user to review)
    for group in flagged_groups:
        r = dict(group.master_record)
        r['_status'] = 'review'
        r['_note'] = group.reason
        master_records.append(r)

        for dup in group.duplicates:
            r = dict(dup)
            r['_status'] = 'review'
            r['_note'] = group.reason
            master_records.append(r)

    # Build duplicates CSV (records to delete)
    dup_fieldnames = fieldnames + ['_merged_into']
    duplicates_to_delete = []

    for group in auto_merge_groups:
        master_name = get_name_field(group.master_record)
        for dup in group.duplicates:
            r = dict(dup)
            r['_merged_into'] = master_name
            duplicates_to_delete.append(r)

    # Generate CSV strings
    master_output = io.StringIO()
    if master_records:
        writer = csv.DictWriter(master_output, fieldnames=output_fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(master_records)
    master_csv = master_output.getvalue()

    duplicates_output = io.StringIO()
    if duplicates_to_delete:
        writer = csv.DictWriter(duplicates_output, fieldnames=dup_fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(duplicates_to_delete)
    duplicates_csv = duplicates_output.getvalue()

    # Build summary
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
        'flagged_count': len(flagged_groups),  # Number of groups needing review
        'clean_count': len(clean_records),
        'master_csv': master_csv,
        'duplicates_csv': duplicates_csv
    }

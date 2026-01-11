# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pymongo",
# ]
# ///
"""
CDC Test Script - Insert/Delete test records in MongoDB for CDC testing.

Usage:
    uv run scripts/test_cdc_changes.py --insert      # Insert test records
    uv run scripts/test_cdc_changes.py --delete      # Delete test records (only those with CDC_TEST marker)
    uv run scripts/test_cdc_changes.py --both        # Insert first, wait, then delete
    uv run scripts/test_cdc_changes.py --count       # Count existing test records

All test records are marked with source="CDC_TEST" for easy identification and cleanup.
"""

import argparse
import time
from datetime import datetime, timezone
from pymongo import MongoClient
from bson import ObjectId

# Configuration
MONGO_URI = "mongodb://admin:mongoadd99@localhost:27017/carbonLens?replicaSet=rs0&authSource=admin"
DATABASE = "carbonLens"
COLLECTION = "stationarycombustions"  # Using this for testing - well-structured and has data
TEST_MARKER = "CDC_TEST"  # Marker in 'source' field to identify test records

# Test records template - matches existing schema
TEST_RECORDS = [
    {
        "companyId": ObjectId("6891bb1896163182f48b7e6f"),
        "siteId": ObjectId("6891c63196163182f48b7f5b"),
        "userId": ObjectId("6891bceb96163182f48b7ea2"),
        "frequency": "Monthly",
        "year": 2026,
        "month": "January",
        "equipment": "Boiler",
        "fuelState": "Gaseous Fuels",
        "fuelType": "Natural Gas",
        "unit": "scf",
        "activity": 12345.67,
        "spend": 500.00,
        "currency": "USD",
        "emissionFactor": 0.05444,
        "emissionFactorUnit": "kgCO2/scf",
        "calculatedEmission": 672.00,
        "name": "CDC Test User",
        "notes": "Test record for CDC verification - safe to delete",
        "source": TEST_MARKER,  # Key marker for identification
        "approved": "false",
        "approvedBy": None,
        "createdAt": None,  # Will be set at insert time
        "updatedAt": None,
    },
    {
        "companyId": ObjectId("6891bb1896163182f48b7e6f"),
        "siteId": ObjectId("6891c63196163182f48b7f5b"),
        "userId": ObjectId("6891bceb96163182f48b7ea2"),
        "frequency": "Monthly",
        "year": 2026,
        "month": "February",
        "equipment": "Furnaces & Ovens",
        "fuelState": "Liquid Fuels",
        "fuelType": "Diesel Fuel",
        "unit": "gallon",
        "activity": 987.65,
        "spend": 3500.00,
        "currency": "USD",
        "emissionFactor": 10.21,
        "emissionFactorUnit": "kgCO2/gallon",
        "calculatedEmission": 10082.97,
        "name": "CDC Test User",
        "notes": "Test record for CDC verification - safe to delete",
        "source": TEST_MARKER,
        "approved": "false",
        "approvedBy": None,
        "createdAt": None,
        "updatedAt": None,
    },
    {
        "companyId": ObjectId("6891bb1896163182f48b7e6f"),
        "siteId": ObjectId("6891c63196163182f48b7f5b"),
        "userId": ObjectId("6891bceb96163182f48b7ea2"),
        "frequency": "Monthly",
        "year": 2026,
        "month": "March",
        "equipment": "Turbines",
        "fuelState": "Gaseous Fuels",
        "fuelType": "Propane",
        "unit": "gallon",
        "activity": 450.00,
        "spend": 1200.00,
        "currency": "USD",
        "emissionFactor": 5.72,
        "emissionFactorUnit": "kgCO2/gallon",
        "calculatedEmission": 2574.00,
        "name": "CDC Test User",
        "notes": "Test record for CDC verification - safe to delete",
        "source": TEST_MARKER,
        "approved": "false",
        "approvedBy": None,
        "createdAt": None,
        "updatedAt": None,
    },
]


def get_collection():
    """Connect to MongoDB and return the collection."""
    client = MongoClient(MONGO_URI)
    db = client[DATABASE]
    return db[COLLECTION], client


def insert_test_records():
    """Insert test records into MongoDB."""
    collection, client = get_collection()
    
    now = datetime.now(timezone.utc).isoformat()
    records_to_insert = []
    
    for record in TEST_RECORDS:
        rec = record.copy()
        rec["createdAt"] = now
        rec["updatedAt"] = now
        records_to_insert.append(rec)
    
    result = collection.insert_many(records_to_insert)
    print(f"✅ Inserted {len(result.inserted_ids)} test records:")
    for _id in result.inserted_ids:
        print(f"   - {_id}")
    
    client.close()
    return result.inserted_ids


def delete_test_records():
    """Delete all test records (those with source=CDC_TEST)."""
    collection, client = get_collection()
    
    # Find test records first
    test_records = list(collection.find({"source": TEST_MARKER}))
    
    if not test_records:
        print("⚠️  No test records found to delete.")
        client.close()
        return 0
    
    print(f"🔍 Found {len(test_records)} test records to delete:")
    for rec in test_records:
        print(f"   - {rec['_id']} (year={rec.get('year')}, month={rec.get('month')})")
    
    # Delete them
    result = collection.delete_many({"source": TEST_MARKER})
    print(f"🗑️  Deleted {result.deleted_count} test records.")
    
    client.close()
    return result.deleted_count


def count_test_records():
    """Count existing test records."""
    collection, client = get_collection()
    
    count = collection.count_documents({"source": TEST_MARKER})
    total = collection.count_documents({})
    
    print(f"📊 Test records (source='{TEST_MARKER}'): {count}")
    print(f"📊 Total records in {COLLECTION}: {total}")
    
    if count > 0:
        test_records = list(collection.find({"source": TEST_MARKER}))
        print("\n   Test records:")
        for rec in test_records:
            print(f"   - {rec['_id']} | {rec.get('year')}/{rec.get('month')} | {rec.get('equipment')}")
    
    client.close()
    return count


def update_test_records():
    """Update existing test records (to test UPDATE CDC events)."""
    collection, client = get_collection()
    
    test_records = list(collection.find({"source": TEST_MARKER}))
    
    if not test_records:
        print("⚠️  No test records found to update.")
        client.close()
        return 0
    
    now = datetime.now(timezone.utc).isoformat()
    updated_count = 0
    
    for rec in test_records:
        new_activity = (rec.get("activity", 0) or 0) + 100
        result = collection.update_one(
            {"_id": rec["_id"]},
            {"$set": {
                "activity": new_activity,
                "notes": f"Updated at {now} - CDC update test",
                "updatedAt": now
            }}
        )
        if result.modified_count > 0:
            updated_count += 1
            print(f"   ✏️  Updated {rec['_id']} - activity: {rec.get('activity')} → {new_activity}")
    
    print(f"✅ Updated {updated_count} test records.")
    client.close()
    return updated_count


def main():
    parser = argparse.ArgumentParser(
        description="CDC Test Script - Insert/Delete/Update test records in MongoDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run scripts/test_cdc_changes.py --insert      # Insert 3 test records
    uv run scripts/test_cdc_changes.py --update      # Update existing test records  
    uv run scripts/test_cdc_changes.py --delete      # Delete all test records
    uv run scripts/test_cdc_changes.py --both        # Insert, wait 5s, then delete
    uv run scripts/test_cdc_changes.py --count       # Count test records
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--insert", action="store_true", help="Insert test records")
    group.add_argument("--update", action="store_true", help="Update existing test records")
    group.add_argument("--delete", action="store_true", help="Delete test records (source=CDC_TEST only)")
    group.add_argument("--both", action="store_true", help="Insert, wait 5 seconds, then delete")
    group.add_argument("--count", action="store_true", help="Count test records")
    
    parser.add_argument("--wait", type=int, default=5, help="Seconds to wait between insert/delete in --both mode")
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"  CDC Test Script - {COLLECTION}")
    print(f"  Marker: source='{TEST_MARKER}'")
    print(f"{'='*60}\n")
    
    if args.insert:
        insert_test_records()
    elif args.update:
        update_test_records()
    elif args.delete:
        delete_test_records()
    elif args.count:
        count_test_records()
    elif args.both:
        insert_test_records()
        print(f"\n⏳ Waiting {args.wait} seconds before delete...")
        time.sleep(args.wait)
        print()
        delete_test_records()
    
    print()


if __name__ == "__main__":
    main()

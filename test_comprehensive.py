"""
Comprehensive test suite for Backend Challenge requirements
Tests all APIs, performance metrics, and edge cases
"""
import os
import sys
import time
import requests
import json
from pathlib import Path

# Configuration
BASE_URL = "http://localhost:8000"
TEST_DATA_DIR = Path(__file__).parent / "data"

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def print_test(name, passed, details=""):
    status = f"{Colors.GREEN}✓ PASS{Colors.END}" if passed else f"{Colors.RED}✗ FAIL{Colors.END}"
    print(f"{status} | {name}")
    if details:
        print(f"      {details}")

def print_section(title):
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}{title}{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}")

class TestResults:
    def __init__(self):
        self.total = 0
        self.passed = 0
        self.failed = 0
    
    def add(self, passed):
        self.total += 1
        if passed:
            self.passed += 1
        else:
            self.failed += 1
    
    def summary(self):
        print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
        print(f"{Colors.BLUE}TEST SUMMARY{Colors.END}")
        print(f"{Colors.BLUE}{'='*60}{Colors.END}")
        print(f"Total Tests: {self.total}")
        print(f"{Colors.GREEN}Passed: {self.passed}{Colors.END}")
        print(f"{Colors.RED}Failed: {self.failed}{Colors.END}")
        percentage = (self.passed / self.total * 100) if self.total > 0 else 0
        print(f"Success Rate: {percentage:.1f}%")

results = TestResults()

def test_upload_api():
    """Test Challenge 1: File Upload & Performance Tracking"""
    print_section("Challenge 1: File Upload API")
    
    # Test 1.1: Valid CSV upload
    test_file = TEST_DATA_DIR / "test_small.csv"
    if test_file.exists():
        start_time = time.time()
        with open(test_file, 'rb') as f:
            response = requests.post(
                f"{BASE_URL}/api/uploads/",
                files={'file': ('test_small.csv', f, 'text/csv')}
            )
        upload_time = (time.time() - start_time) * 1000
        
        passed = response.status_code == 200 and upload_time < 200
        data = response.json() if response.status_code == 200 else {}
        print_test(
            "Valid CSV upload (< 200ms)",
            passed,
            f"Time: {upload_time:.0f}ms, Status: {response.status_code}"
        )
        results.add(passed)
        
        # Store task_id for later tests
        global task_id
        task_id = data.get('task_id')
        
        # Test 1.2: Response structure
        required_fields = ['success', 'task_id', 'file_name', 'file_size_mb', 'estimated_rows', 'status', 'message']
        has_all_fields = all(field in data for field in required_fields)
        print_test(
            "Response contains all required fields",
            has_all_fields,
            f"Fields: {list(data.keys())}"
        )
        results.add(has_all_fields)
    else:
        print_test("Valid CSV upload", False, "Test file not found")
        results.add(False)
    
    # Test 1.3: Invalid file type
    try:
        response = requests.post(
            f"{BASE_URL}/api/uploads/",
            files={'file': ('test.txt', b'not a csv', 'text/plain')}
        )
        passed = response.status_code == 400
        print_test("Reject non-CSV files", passed, f"Status: {response.status_code}")
        results.add(passed)
    except Exception as e:
        print_test("Reject non-CSV files", False, str(e))
        results.add(False)
    
    # Test 1.4: Missing file
    try:
        response = requests.post(f"{BASE_URL}/api/uploads/")
        passed = response.status_code in [400, 422]
        print_test("Handle missing file", passed, f"Status: {response.status_code}")
        results.add(passed)
    except Exception as e:
        print_test("Handle missing file", False, str(e))
        results.add(False)
    
    # Test 1.5: Empty file
    try:
        response = requests.post(
            f"{BASE_URL}/api/uploads/",
            files={'file': ('empty.csv', b'', 'text/csv')}
        )
        passed = response.status_code == 400
        print_test("Reject empty files", passed, f"Status: {response.status_code}")
        results.add(passed)
    except Exception as e:
        print_test("Reject empty files", False, str(e))
        results.add(False)

def test_performance_stats():
    """Test performance metrics API"""
    print_section("Performance Metrics API")
    
    if 'task_id' not in globals():
        print_test("Performance stats", False, "No task_id available")
        results.add(False)
        return
    
    # Wait for processing to complete
    print("Waiting for processing to complete...")
    max_wait = 60
    start_wait = time.time()
    
    while time.time() - start_wait < max_wait:
        response = requests.get(f"{BASE_URL}/api/uploads/performance-stats/{task_id}")
        if response.status_code == 200:
            data = response.json()
            if data.get('status') in ['COMPLETED', 'FAILED']:
                break
        time.sleep(2)
    
    # Test 2.1: Performance stats response
    response = requests.get(f"{BASE_URL}/api/uploads/performance-stats/{task_id}")
    passed = response.status_code == 200
    print_test("Get performance stats", passed, f"Status: {response.status_code}")
    results.add(passed)
    
    if passed:
        data = response.json()
        
        # Test 2.2: Required metrics fields
        required_metrics = [
            'execution_time_ms', 'peak_memory_mb', 'rows_processed',
            'rows_rejected', 'db_query_count', 'cache_hit_rate',
            'processing_rate_rows_per_sec'
        ]
        metrics = data.get('metrics', {})
        has_all_metrics = all(metric in metrics for metric in required_metrics)
        print_test(
            "All performance metrics present",
            has_all_metrics,
            f"Metrics: {list(metrics.keys())}"
        )
        results.add(has_all_metrics)
        
        # Test 2.3: Processing completed successfully
        status_ok = data.get('status') == 'COMPLETED'
        print_test(
            "Processing completed successfully",
            status_ok,
            f"Status: {data.get('status')}, Rows: {metrics.get('rows_processed', 0)}"
        )
        results.add(status_ok)
        
        # Display metrics
        if status_ok:
            print(f"\n{Colors.YELLOW}Performance Metrics:{Colors.END}")
            print(f"  Execution Time: {metrics.get('execution_time_ms', 0)}ms")
            print(f"  Peak Memory: {metrics.get('peak_memory_mb', 0)}MB")
            print(f"  Rows Processed: {metrics.get('rows_processed', 0)}")
            print(f"  Rows Rejected: {metrics.get('rows_rejected', 0)}")
            print(f"  DB Query Count: {metrics.get('db_query_count', 0)}")
            print(f"  Cache Hit Rate: {metrics.get('cache_hit_rate', 0)}")
            print(f"  Processing Rate: {metrics.get('processing_rate_rows_per_sec', 0)} rows/sec")

def test_analytics_apis():
    """Test Challenge 2: Analytics APIs"""
    print_section("Challenge 2: Analytics APIs")
    
    # Test 2.1: Zone Leaderboard
    response = requests.get(f"{BASE_URL}/api/analytics/zone-leaderboard/")
    passed = response.status_code == 200
    data = response.json() if passed else {}
    has_data = 'data' in data and 'query_time_ms' in data
    print_test(
        "Zone Leaderboard API",
        passed and has_data,
        f"Status: {response.status_code}, Zones: {len(data.get('data', []))}, Query time: {data.get('query_time_ms', 0)}ms"
    )
    results.add(passed and has_data)
    
    # Validate zone leaderboard structure
    if passed and data.get('data'):
        zone = data['data'][0]
        required_fields = ['rank', 'zone', 'total_amount', 'transaction_count', 'average_amount']
        has_fields = all(field in zone for field in required_fields)
        print_test(
            "Zone leaderboard structure",
            has_fields,
            f"Fields: {list(zone.keys())}"
        )
        results.add(has_fields)
    
    # Test 2.2: Category Distribution
    response = requests.get(f"{BASE_URL}/api/analytics/category-distribution/")
    passed = response.status_code == 200
    data = response.json() if passed else {}
    has_data = 'data' in data and 'total_transactions' in data
    print_test(
        "Category Distribution API",
        passed and has_data,
        f"Status: {response.status_code}, Categories: {len(data.get('data', []))}, Query time: {data.get('query_time_ms', 0)}ms"
    )
    results.add(passed and has_data)
    
    # Test 2.3: Dormant Merchants (with pagination)
    response = requests.get(f"{BASE_URL}/api/analytics/dormant-merchants/?page=1&page_size=100")
    passed = response.status_code == 200
    data = response.json() if passed else {}
    has_pagination = 'pagination' in data
    print_test(
        "Dormant Merchants API (paginated)",
        passed and has_pagination,
        f"Status: {response.status_code}, Dormant: {data.get('pagination', {}).get('total_dormant_merchants', 0)}, Query time: {data.get('query_time_ms', 0)}ms"
    )
    results.add(passed and has_pagination)
    
    # Test 2.4: Hourly Pattern
    response = requests.get(f"{BASE_URL}/api/analytics/hourly-pattern/")
    passed = response.status_code == 200
    data = response.json() if passed else {}
    has_24_hours = len(data.get('data', [])) == 24
    print_test(
        "Hourly Pattern API (24 hours)",
        passed and has_24_hours,
        f"Status: {response.status_code}, Hours: {len(data.get('data', []))}, Query time: {data.get('query_time_ms', 0)}ms"
    )
    results.add(passed and has_24_hours)
    
    # Test 2.5: Anomalies (with pagination)
    response = requests.get(f"{BASE_URL}/api/analytics/anomalies/?page=1&page_size=50")
    passed = response.status_code == 200
    data = response.json() if passed else {}
    has_pagination = 'pagination' in data
    print_test(
        "Anomalies API (paginated)",
        passed and has_pagination,
        f"Status: {response.status_code}, Anomalies: {data.get('pagination', {}).get('total_anomalies', 0)}, Query time: {data.get('query_time_ms', 0)}ms"
    )
    results.add(passed and has_pagination)
    
    # Test 2.6: Customer Retention
    response = requests.get(f"{BASE_URL}/api/analytics/customer-retention/")
    passed = response.status_code == 200
    data = response.json() if passed else {}
    required_fields = ['total_unique_customers', 'repeat_customers', 'repeat_customer_percentage']
    has_fields = all(field in data for field in required_fields)
    print_test(
        "Customer Retention API",
        passed and has_fields,
        f"Status: {response.status_code}, Repeat rate: {data.get('repeat_customer_percentage', 0)}%, Query time: {data.get('query_time_ms', 0)}ms"
    )
    results.add(passed and has_fields)
    
    # Test 2.7: Full Report
    response = requests.get(f"{BASE_URL}/api/analytics/full-report/")
    passed = response.status_code == 200
    data = response.json() if passed else {}
    required_sections = [
        'zone_leaderboard', 'category_distribution', 'dormant_merchants_count',
        'anomalies_count', 'customer_retention', 'total_query_time_ms'
    ]
    has_all_sections = all(section in data for section in required_sections)
    print_test(
        "Full Report API (combined)",
        passed and has_all_sections,
        f"Status: {response.status_code}, Total query time: {data.get('total_query_time_ms', 0)}ms"
    )
    results.add(passed and has_all_sections)

def test_edge_cases():
    """Test data quality and edge case handling"""
    print_section("Edge Cases & Data Quality")
    
    # Test pagination edge cases
    response = requests.get(f"{BASE_URL}/api/analytics/dormant-merchants/?page=0&page_size=100")
    passed = response.status_code in [200, 400]
    print_test("Handle invalid page number", passed, f"Status: {response.status_code}")
    results.add(passed)
    
    response = requests.get(f"{BASE_URL}/api/analytics/dormant-merchants/?page=1&page_size=10000")
    passed = response.status_code in [200, 400]
    print_test("Handle excessive page size", passed, f"Status: {response.status_code}")
    results.add(passed)
    
    # Test invalid task_id
    response = requests.get(f"{BASE_URL}/api/uploads/performance-stats/invalid-uuid")
    passed = response.status_code == 404
    print_test("Handle invalid task_id", passed, f"Status: {response.status_code}")
    results.add(passed)

def test_concurrent_uploads():
    """Test concurrent upload handling"""
    print_section("Concurrency Test")
    
    test_file = TEST_DATA_DIR / "test_small.csv"
    if not test_file.exists():
        print_test("Concurrent uploads", False, "Test file not found")
        results.add(False)
        return
    
    import concurrent.futures
    
    def upload_file(file_path):
        try:
            with open(file_path, 'rb') as f:
                response = requests.post(
                    f"{BASE_URL}/api/ingest/",
                    files={'file': (file_path.name, f, 'text/csv')},
                    timeout=10
                )
            return response.status_code == 200
        except Exception:
            return False
    
    # Test 5 concurrent uploads
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(upload_file, test_file) for _ in range(5)]
        results_list = [f.result() for f in concurrent.futures.as_completed(futures)]
    
    elapsed = time.time() - start_time
    success_count = sum(results_list)
    
    passed = success_count >= 4  # At least 4 out of 5 should succeed
    print_test(
        "Handle 5 concurrent uploads",
        passed,
        f"Successful: {success_count}/5, Time: {elapsed:.1f}s"
    )
    results.add(passed)

def main():
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}Backend Challenge - Comprehensive Test Suite{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"Testing against: {BASE_URL}")
    
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/api/analytics/zone-leaderboard/", timeout=5)
        print(f"{Colors.GREEN}✓ Server is running{Colors.END}\n")
    except requests.exceptions.RequestException:
        print(f"{Colors.RED}✗ Server is not running at {BASE_URL}{Colors.END}")
        print(f"Please start the server with: docker-compose up")
        return
    
    # Run all tests
    test_upload_api()
    test_performance_stats()
    test_analytics_apis()
    test_edge_cases()
    test_concurrent_uploads()
    
    # Print summary
    results.summary()
    
    # Exit with appropriate code
    sys.exit(0 if results.failed == 0 else 1)

if __name__ == "__main__":
    main()

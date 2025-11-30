from celery import shared_task
from django.utils import timezone
from apps.core.models import UploadTask, Transaction, Merchant
import csv
import os
import time
import psutil
from decimal import Decimal, InvalidOperation
from datetime import datetime

@shared_task
def process_csv_file(task_id, file_path):
    import tracemalloc
    import redis
    from django.db import connection
    
    # Start memory tracking
    tracemalloc.start()
    start_time = time.time()
    initial_queries = len(connection.queries)
    
    # Redis connection for cache stats
    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        initial_redis_stats = redis_client.info('stats')
    except:
        initial_redis_stats = None
    
    try:
        task = UploadTask.objects.get(task_id=task_id)
        task.status = 'PROCESSING'
        task.save()
        
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        rows_processed = 0
        rows_failed = 0
        batch_size = 1000
        batch_data = []
        
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                try:
                    # Clean and validate data
                    transaction_data = clean_transaction_data(row)
                    if transaction_data:
                        batch_data.append(Transaction(**transaction_data))
                        
                        # Bulk insert when batch is full
                        if len(batch_data) >= batch_size:
                            Transaction.objects.bulk_create(batch_data, ignore_conflicts=True)
                            rows_processed += len(batch_data)
                            batch_data = []
                            
                            # Update progress
                            task.rows_processed = rows_processed
                            task.save()
                    else:
                        rows_failed += 1
                        
                except Exception as e:
                    rows_failed += 1
                    continue
            
            # Insert remaining batch
            if batch_data:
                Transaction.objects.bulk_create(batch_data, ignore_conflicts=True)
                rows_processed += len(batch_data)
        
        # Calculate advanced metrics
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Get peak memory usage from tracemalloc
        current, peak = tracemalloc.get_traced_memory()
        peak_memory_mb = round(peak / 1024 / 1024, 1)
        tracemalloc.stop()
        
        # Calculate processing rate
        processing_rate = rows_processed / (execution_time_ms / 1000) if execution_time_ms > 0 else 0
        
        # Calculate database query count
        final_queries = len(connection.queries)
        db_query_count = final_queries - initial_queries
        
        # Calculate cache hit rate from Redis stats
        cache_hit_rate = 0.0
        if initial_redis_stats:
            try:
                final_redis_stats = redis_client.info('stats')
                hits = final_redis_stats.get('keyspace_hits', 0) - initial_redis_stats.get('keyspace_hits', 0)
                misses = final_redis_stats.get('keyspace_misses', 0) - initial_redis_stats.get('keyspace_misses', 0)
                total = hits + misses
                cache_hit_rate = round(hits / total, 2) if total > 0 else 0.0
            except:
                cache_hit_rate = 0.0
        
        task.status = 'COMPLETED'
        task.rows_processed = rows_processed
        task.rows_rejected = rows_failed
        task.execution_time_ms = execution_time_ms
        task.peak_memory_mb = peak_memory_mb
        task.db_query_count = db_query_count
        task.cache_hit_rate = cache_hit_rate
        task.processing_rate_rows_per_sec = round(processing_rate, 0)
        task.completed_at = timezone.now()
        task.save()
        
        # Clean up temp file
        if os.path.exists(file_path):
            os.remove(file_path)
            
    except Exception as e:
        try:
            task = UploadTask.objects.get(task_id=task_id)
            task.status = 'FAILED'
            task.error_message = str(e)
            task.completed_at = timezone.now()
            task.save()
        except:
            pass  # Task might not exist

def clean_transaction_data(row):
    """Clean and validate transaction data with comprehensive data quality handling"""
    try:
        from django.utils import timezone as django_timezone
        import re
        
        # Required fields with null handling
        transaction_id = str(row.get('TRANSACTION_ID', '') or '').strip()
        merchant_id = str(row.get('MERCHANT_ID', '') or '').strip()
        zone = str(row.get('ZONE', '') or '').strip().upper()
        category = str(row.get('CATEGORY', '') or '').strip().title()
        amount_str = str(row.get('AMOUNT', '') or '').strip()
        timestamp_str = str(row.get('TIMESTAMP', '') or '').strip()
        customer_phone = str(row.get('CUSTOMER_PHONE', '') or '').strip()
        
        # Handle missing fields
        if not all([transaction_id, merchant_id, zone, category, amount_str, timestamp_str]):
            return None
        
        # Clean and normalize category names
        category_mapping = {
            'GROCERY': 'Grocery', 'GROCERIES': 'Grocery', 'FOOD': 'Food',
            'ELECTRONICS': 'Electronics', 'ELECTRONIC': 'Electronics',
            'FASHION': 'Fashion', 'CLOTHING': 'Fashion', 'CLOTHES': 'Fashion',
            'TRANSPORT': 'Transport', 'TRANSPORTATION': 'Transport',
            'UTILITIES': 'Utilities', 'UTILITY': 'Utilities',
            'HEALTHCARE': 'Healthcare', 'HEALTH': 'Healthcare',
            'EDUCATION': 'Education', 'EDU': 'Education'
        }
        category = category_mapping.get(category.upper(), category)
        
        # Parse and validate amount
        try:
            # Remove any non-numeric characters except decimal point
            amount_clean = re.sub(r'[^\d.-]', '', amount_str)
            amount = Decimal(amount_clean)
            
            # Reject negative amounts
            if amount < 0:
                return None
            
            # Reject unreasonably large amounts (> 1M)
            if amount > 1000000:
                return None
                
        except (InvalidOperation, ValueError, TypeError):
            return None
        
        # Parse and validate timestamp
        try:
            # Handle various timestamp formats
            timestamp_str = timestamp_str.replace('\n', ' ').strip()
            
            # Try ISO format first
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except ValueError:
                # Try common formats
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H:%M',
                    '%Y/%m/%d %H:%M:%S',
                    '%d/%m/%Y %H:%M:%S',
                    '%Y-%m-%d'
                ]
                
                timestamp = None
                for fmt in formats:
                    try:
                        timestamp = datetime.strptime(timestamp_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if not timestamp:
                    return None
                
                # Make timezone aware
                timestamp = django_timezone.make_aware(timestamp)
            
            # Reject future timestamps
            if timestamp > django_timezone.now():
                return None
                
        except (ValueError, TypeError):
            return None
        
        # Validate and clean phone number
        if customer_phone:
            # Remove non-numeric characters
            phone_clean = re.sub(r'[^\d+]', '', customer_phone)
            # Basic validation - should have 10-15 digits
            if len(re.sub(r'[^\d]', '', phone_clean)) < 10 or len(re.sub(r'[^\d]', '', phone_clean)) > 15:
                customer_phone = 'INVALID'
            else:
                customer_phone = phone_clean
        
        # Create merchant if not exists
        try:
            Merchant.objects.get_or_create(merchant_id=merchant_id)
        except:
            pass  # Continue even if merchant creation fails
        
        return {
            'transaction_id': transaction_id,
            'merchant_id': merchant_id,
            'zone': zone,
            'category': category,
            'amount': amount,
            'timestamp': timestamp,
            'customer_phone': customer_phone
        }
        
    except Exception:
        return None
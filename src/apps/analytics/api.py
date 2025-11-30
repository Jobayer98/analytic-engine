from ninja import Router, Query
from django.http import JsonResponse
from django.db.models import Sum, Count, Avg, StdDev
from django.db.models.functions import Extract
from django.core.paginator import Paginator
from apps.core.models import Transaction, Merchant
from decimal import Decimal
from typing import Optional

router = Router()

@router.get("/zone-leaderboard/")
def zone_leaderboard(request):
    import time
    start_time = time.time()
    
    try:
        zones = Transaction.objects.values('zone').annotate(
            total_amount=Sum('amount'),
            transaction_count=Count('id'),
            average_amount=Avg('amount')
        ).order_by('-total_amount')[:20]
        
        query_time_ms = int((time.time() - start_time) * 1000)
        
        return JsonResponse({
            "data": [
                {
                    "rank": idx + 1,
                    "zone": zone['zone'],
                    "total_amount": float(zone['total_amount'] or 0),
                    "transaction_count": zone['transaction_count'],
                    "average_amount": float(zone['average_amount'] or 0)
                }
                for idx, zone in enumerate(zones)
            ],
            "query_time_ms": query_time_ms
        })
    except Exception as e:
        return JsonResponse({"success": False, "error": "Query failed"}, status=500)

@router.get("/category-distribution/")
def category_distribution(request):
    import time
    start_time = time.time()
    
    try:
        total_transactions = Transaction.objects.count()
        if total_transactions == 0:
            return JsonResponse({"data": [], "total_transactions": 0, "query_time_ms": 0})
        
        categories = Transaction.objects.values('category').annotate(
            transaction_count=Count('id')
        ).order_by('-transaction_count')
        
        query_time_ms = int((time.time() - start_time) * 1000)
        
        return JsonResponse({
            "data": [
                {
                    "category": cat['category'],
                    "percentage": round((cat['transaction_count'] / total_transactions) * 100, 1),
                    "transaction_count": cat['transaction_count']
                }
                for cat in categories
            ],
            "total_transactions": total_transactions,
            "query_time_ms": query_time_ms
        })
    except Exception as e:
        return JsonResponse({"success": False, "error": "Query failed"}, status=500)

@router.get("/dormant-merchants/")
def dormant_merchants(request, page: int = Query(1, description="Page number"), page_size: int = Query(100, description="Number of items per page (max 1000)")):
    import time
    start_time = time.time()
    
    try:
        # Parameters are now passed directly from function signature
        
        # Validate pagination parameters
        if page < 1 or page_size < 1 or page_size > 1000:
            return JsonResponse({"success": False, "error": "Invalid pagination parameters"}, status=400)
        
        # Get merchants with no transactions (optimized query)
        active_merchants = Transaction.objects.values_list('merchant_id', flat=True).distinct()
        dormant_merchants = Merchant.objects.exclude(merchant_id__in=active_merchants)
        
        paginator = Paginator(dormant_merchants, page_size)
        page_obj = paginator.get_page(page)
        
        query_time_ms = int((time.time() - start_time) * 1000)
        
        return JsonResponse({
            "data": [
                {
                    "merchant_id": merchant.merchant_id, 
                    "merchant_name": merchant.name or "Unknown",
                    "zone": "Unknown"  # Would need zone info in Merchant model
                }
                for merchant in page_obj
            ],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_dormant_merchants": paginator.count,
                "total_pages": paginator.num_pages
            },
            "query_time_ms": query_time_ms
        })
    except Exception as e:
        return JsonResponse({"success": False, "error": "Query failed"}, status=500)

@router.get("/hourly-pattern/")
def hourly_pattern(request):
    import time
    start_time = time.time()
    
    try:
        hourly_data = Transaction.objects.annotate(
            hour=Extract('timestamp', 'hour')
        ).values('hour').annotate(
            transaction_count=Count('id'),
            average_amount=Avg('amount')
        ).order_by('hour')
        
        # Fill missing hours with zeros
        hours_dict = {data['hour']: data for data in hourly_data}
        result = []
        
        for hour in range(24):
            if hour in hours_dict:
                result.append({
                    "hour": hour,
                    "transaction_count": hours_dict[hour]['transaction_count'],
                    "average_amount": round(float(hours_dict[hour]['average_amount'] or 0), 2)
                })
            else:
                result.append({
                    "hour": hour,
                    "transaction_count": 0,
                    "average_amount": 0.0
                })
        
        query_time_ms = int((time.time() - start_time) * 1000)
        
        return JsonResponse({
            "data": result,
            "query_time_ms": query_time_ms
        })
    except Exception as e:
        return JsonResponse({"success": False, "error": "Query failed"}, status=500)

@router.get("/anomalies/")
def anomalies(request, page: int = Query(1, description="Page number"), page_size: int = Query(50, description="Number of items per page (max 1000)")):
    import time
    start_time = time.time()
    
    try:
        # Parameters are now passed directly from function signature
        
        # Validate pagination parameters
        if page < 1 or page_size < 1 or page_size > 1000:
            return JsonResponse({"success": False, "error": "Invalid pagination parameters"}, status=400)
        
        # Calculate category statistics
        category_stats = Transaction.objects.values('category').annotate(
            category_mean=Avg('amount'),
            category_std_dev=StdDev('amount')
        )
        
        anomalies = []
        for stat in category_stats:
            if stat['category_std_dev'] and stat['category_std_dev'] > 0:
                threshold = stat['category_mean'] + (3 * stat['category_std_dev'])
                category_anomalies = Transaction.objects.filter(
                    category=stat['category'],
                    amount__gt=threshold
                ).values('transaction_id', 'amount')
                
                for anomaly in category_anomalies:
                    std_dev_from_mean = (float(anomaly['amount']) - float(stat['category_mean'])) / float(stat['category_std_dev'])
                    anomalies.append({
                        "transaction_id": anomaly['transaction_id'],
                        "amount": float(anomaly['amount']),
                        "category": stat['category'],
                        "category_mean": round(float(stat['category_mean']), 2),
                        "category_std_dev": round(float(stat['category_std_dev']), 2),
                        "std_dev_from_mean": round(std_dev_from_mean, 1)
                    })
        
        # Sort by amount descending
        anomalies.sort(key=lambda x: x['amount'], reverse=True)
        
        paginator = Paginator(anomalies, page_size)
        page_obj = paginator.get_page(page)
        
        query_time_ms = int((time.time() - start_time) * 1000)
        
        return JsonResponse({
            "data": list(page_obj),
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_anomalies": len(anomalies),
                "total_pages": paginator.num_pages
            },
            "query_time_ms": query_time_ms
        })
    except Exception as e:
        return JsonResponse({"success": False, "error": "Query failed"}, status=500)

@router.get("/customer-retention/")
def customer_retention(request):
    import time
    start_time = time.time()
    
    try:
        # Count unique customers and repeat customers
        customer_counts = Transaction.objects.values('customer_phone').annotate(
            transaction_count=Count('id')
        )
        
        total_customers = customer_counts.count()
        repeat_customers = customer_counts.filter(transaction_count__gt=1).count()
        single_transaction_customers = total_customers - repeat_customers
        
        # Calculate average transactions per customer
        total_transactions = Transaction.objects.count()
        avg_transactions_per_customer = round(total_transactions / total_customers, 2) if total_customers > 0 else 0
        
        query_time_ms = int((time.time() - start_time) * 1000)
        
        return JsonResponse({
            "total_unique_customers": total_customers,
            "repeat_customers": repeat_customers,
            "repeat_customer_percentage": round((repeat_customers / total_customers * 100), 2) if total_customers > 0 else 0,
            "single_transaction_customers": single_transaction_customers,
            "average_transactions_per_customer": avg_transactions_per_customer,
            "query_time_ms": query_time_ms
        })
    except Exception as e:
        return JsonResponse({"success": False, "error": "Query failed"}, status=500)

@router.get("/full-report/")
def full_report(request):
    import time
    from concurrent.futures import ThreadPoolExecutor
    import threading
    
    start_time = time.time()
    
    try:
        # Use parallel execution for better performance
        results = {}
        
        def get_zone_leaderboard():
            zones = Transaction.objects.values('zone').annotate(
                total_amount=Sum('amount'),
                transaction_count=Count('id'),
                average_amount=Avg('amount')
            ).order_by('-total_amount')[:20]
            return [{
                "rank": idx + 1,
                "zone": zone['zone'],
                "total_amount": float(zone['total_amount'] or 0),
                "transaction_count": zone['transaction_count'],
                "average_amount": float(zone['average_amount'] or 0)
            } for idx, zone in enumerate(zones)]
        
        def get_category_distribution():
            total_transactions = Transaction.objects.count()
            if total_transactions == 0:
                return []
            categories = Transaction.objects.values('category').annotate(
                transaction_count=Count('id')
            ).order_by('-transaction_count')
            return [{
                "category": cat['category'],
                "percentage": round((cat['transaction_count'] / total_transactions) * 100, 1),
                "transaction_count": cat['transaction_count']
            } for cat in categories]
        
        def get_customer_retention():
            customer_counts = Transaction.objects.values('customer_phone').annotate(
                transaction_count=Count('id')
            )
            total_customers = customer_counts.count()
            repeat_customers = customer_counts.filter(transaction_count__gt=1).count()
            single_transaction_customers = total_customers - repeat_customers
            total_transactions = Transaction.objects.count()
            avg_transactions_per_customer = round(total_transactions / total_customers, 2) if total_customers > 0 else 0
            
            return {
                "total_unique_customers": total_customers,
                "repeat_customers": repeat_customers,
                "repeat_customer_percentage": round((repeat_customers / total_customers * 100), 2) if total_customers > 0 else 0,
                "single_transaction_customers": single_transaction_customers,
                "average_transactions_per_customer": avg_transactions_per_customer
            }
        
        def get_counts():
            # Get dormant merchants count
            active_merchants = Transaction.objects.values_list('merchant_id', flat=True).distinct()
            dormant_count = Merchant.objects.exclude(merchant_id__in=active_merchants).count()
            
            # Get anomalies count
            category_stats = Transaction.objects.values('category').annotate(
                avg_amount=Avg('amount'),
                std_amount=StdDev('amount')
            )
            anomalies_count = 0
            for stat in category_stats:
                if stat['std_amount'] and stat['std_amount'] > 0:
                    threshold = stat['avg_amount'] + (3 * stat['std_amount'])
                    anomalies_count += Transaction.objects.filter(
                        category=stat['category'],
                        amount__gt=threshold
                    ).count()
            
            return dormant_count, anomalies_count
        
        # Execute queries in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            zone_future = executor.submit(get_zone_leaderboard)
            category_future = executor.submit(get_category_distribution)
            retention_future = executor.submit(get_customer_retention)
            counts_future = executor.submit(get_counts)
            
            zone_leaderboard = zone_future.result()
            category_distribution = category_future.result()
            customer_retention = retention_future.result()
            dormant_count, anomalies_count = counts_future.result()
        
        query_time_ms = int((time.time() - start_time) * 1000)
        
        return JsonResponse({
            "zone_leaderboard": zone_leaderboard,
            "category_distribution": category_distribution,
            "dormant_merchants_count": dormant_count,
            "hourly_pattern": [],  # Would need separate query
            "anomalies_count": anomalies_count,
            "customer_retention": customer_retention,
            "total_query_time_ms": query_time_ms
        })
    except Exception as e:
        return JsonResponse({"success": False, "error": "Query failed"}, status=500)
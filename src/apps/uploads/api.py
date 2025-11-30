from ninja import Router, File, UploadedFile, Query
from ninja.responses import Response
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from apps.core.models import UploadTask
from .tasks import process_csv_file
import time
import uuid
import os
from typing import Optional

router = Router()

@router.post("/")
def ingest_csv(request, file: UploadedFile = File(...)):
    start_time = time.time()
    
    try:
        # Handle missing file
        if not file:
            return JsonResponse({"success": False, "message": "No file provided"}, status=400)
        
        # Handle empty file
        if file.size == 0:
            return JsonResponse({"success": False, "message": "Empty file not allowed"}, status=400)
        # Validate file
        if not file.name.endswith('.csv'):
            return JsonResponse({"success": False, "message": "Only CSV files allowed"}, status=400)
        
        if file.size > 3 * 1024 * 1024 * 1024:  # 3GB limit
            return JsonResponse({"success": False, "message": "File too large (max 3GB)"}, status=400)
        
        # Validate CSV headers with robust error handling
        try:
            import csv
            import io
            # Read first few bytes to check headers
            file.seek(0)
            try:
                sample = file.read(2048).decode('utf-8', errors='ignore')
            except UnicodeDecodeError:
                return JsonResponse({"success": False, "message": "File encoding not supported"}, status=400)
            
            file.seek(0)
            
            if not sample.strip():
                return JsonResponse({"success": False, "message": "File appears to be empty or corrupted"}, status=400)
            
            try:
                reader = csv.DictReader(io.StringIO(sample))
                headers = reader.fieldnames
            except csv.Error:
                return JsonResponse({"success": False, "message": "Malformed CSV file"}, status=400)
            
            required_headers = ['TRANSACTION_ID', 'MERCHANT_ID', 'ZONE', 'CATEGORY', 'AMOUNT', 'TIMESTAMP', 'CUSTOMER_PHONE']
            
            if not headers:
                return JsonResponse({"success": False, "message": "No CSV headers found"}, status=400)
            
            missing_headers = [h for h in required_headers if h not in headers]
            if missing_headers:
                return JsonResponse({
                    "success": False, 
                    "message": f"Missing required headers: {', '.join(missing_headers)}"
                }, status=400)
                
        except Exception as e:
            return JsonResponse({"success": False, "message": "Failed to validate CSV format"}, status=400)
        
        # Create upload task with validation
        task_id = uuid.uuid4()
        
        # Sanitize filename
        safe_filename = file.name[:255] if file.name else f"upload_{task_id}.csv"
        
        try:
            upload_task = UploadTask.objects.create(
                task_id=task_id,
                filename=safe_filename,
                file_size=file.size,
                status='QUEUED'
            )
        except Exception as db_error:
            import logging
            logging.error(f"Database error: {str(db_error)}")
            return JsonResponse({"success": False, "message": f"Database error: {str(db_error)}"}, status=500)
        
        # Ensure temp directory exists
        import os
        temp_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Save file temporarily and queue processing
        safe_file_path = os.path.join(temp_dir, f"{task_id}_{safe_filename}")
        
        try:
            with open(safe_file_path, 'wb+') as destination:
                for chunk in file.chunks():
                    if chunk:  # Skip empty chunks
                        destination.write(chunk)
        except IOError as io_error:
            return JsonResponse({"success": False, "message": "Failed to save file"}, status=500)
        except Exception as file_error:
            return JsonResponse({"success": False, "message": "File processing error"}, status=500)
        
        # Try to queue Celery task, fallback to direct processing
        try:
            process_csv_file.delay(str(task_id), safe_file_path)
        except Exception as celery_error:
            # Fallback: process directly (for testing without Celery)
            try:
                from .tasks import process_csv_file
                import threading
                thread = threading.Thread(target=process_csv_file, args=(str(task_id), safe_file_path))
                thread.start()
            except Exception as fallback_error:
                # Clean up file and task if both methods fail
                try:
                    os.remove(safe_file_path)
                    upload_task.delete()
                except:
                    pass
                return JsonResponse({"success": False, "message": "Processing queue unavailable"}, status=503)
        
        upload_time = int((time.time() - start_time) * 1000)
        
        # Estimate rows based on file size (rough estimate: ~100 bytes per row)
        estimated_rows = max(1, int(file.size / 100))
        file_size_mb = round(file.size / (1024 * 1024), 1)
        
        return JsonResponse({
            "success": True,
            "task_id": str(task_id),
            "file_name": safe_filename,
            "file_size_mb": file_size_mb,
            "estimated_rows": estimated_rows,
            "status": "QUEUED",
            "message": "File accepted and queued for processing."
        })
        
    except Exception as e:
        # Generic error handler - don't expose internal details
        return JsonResponse({
            "success": False,
            "message": "Upload failed due to server error"
        }, status=500)

@router.get("/performance-stats/{task_id}")
def get_performance_stats(request, task_id: str):
    try:
        # Validate UUID format
        try:
            import uuid
            uuid.UUID(task_id)
        except (ValueError, AttributeError):
            return JsonResponse({"success": False, "error": "Invalid task ID format"}, status=404)
        
        task = UploadTask.objects.get(task_id=task_id)
        file_size_mb = round(task.file_size / (1024 * 1024), 1)
        
        return JsonResponse({
            "task_id": str(task.task_id),
            "status": task.status,
            "file_size_mb": file_size_mb,
            "metrics": {
                "execution_time_ms": task.execution_time_ms or 0,
                "peak_memory_mb": task.peak_memory_mb or 0.0,
                "rows_processed": task.rows_processed,
                "rows_rejected": task.rows_rejected,
                "db_query_count": task.db_query_count,
                "cache_hit_rate": task.cache_hit_rate or 0.0,
                "processing_rate_rows_per_sec": task.processing_rate_rows_per_sec or 0.0
            }
        })
    except UploadTask.DoesNotExist:
        return JsonResponse({"success": False, "error": "Task not found"}, status=404)
    except Exception as e:
        return JsonResponse({"success": False, "error": "Failed to get stats"}, status=500)
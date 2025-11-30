from django.db import models
import uuid


class Transaction(models.Model):
    transaction_id = models.CharField(max_length=100, unique=True, db_index=True)
    merchant_id = models.CharField(max_length=100, db_index=True)
    zone = models.CharField(max_length=100, db_index=True)
    category = models.CharField(max_length=100, db_index=True)
    amount = models.DecimalField(max_digits=12, decimal_places=2, db_index=True)
    timestamp = models.DateTimeField(db_index=True)
    customer_phone = models.CharField(max_length=20, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "transactions"
        indexes = [
            models.Index(fields=["zone", "amount"]),
            models.Index(fields=["category", "amount"]),
            models.Index(fields=["timestamp"]),
            models.Index(fields=["customer_phone"]),
        ]


class UploadTask(models.Model):
    STATUS_CHOICES = [
        ("QUEUED", "Queued"),
        ("PROCESSING", "Processing"),
        ("COMPLETED", "Completed"),
        ("FAILED", "Failed"),
    ]

    task_id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="QUEUED")
    filename = models.CharField(max_length=255)
    file_size = models.BigIntegerField()
    rows_processed = models.BigIntegerField(default=0)
    rows_rejected = models.BigIntegerField(default=0)
    execution_time_ms = models.BigIntegerField(null=True, blank=True)
    peak_memory_mb = models.FloatField(null=True, blank=True)
    db_query_count = models.IntegerField(default=0)
    cache_hit_rate = models.FloatField(null=True, blank=True)
    processing_rate_rows_per_sec = models.FloatField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "upload_tasks"


class Merchant(models.Model):
    merchant_id = models.CharField(max_length=100, unique=True, primary_key=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "merchants"

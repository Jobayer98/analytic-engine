#!/usr/bin/env python
import os
import sys
import django

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from django.db import connection
from apps.core.models import UploadTask, Transaction, Merchant

def test_database():
    try:
        # Test database connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            print(f"✅ Database connection successful: {result}")
        
        # Test table creation
        print("📋 Testing table creation...")
        
        # Create tables if they don't exist
        from django.core.management import execute_from_command_line
        execute_from_command_line(['manage.py', 'migrate', '--run-syncdb'])
        
        print("✅ Tables created successfully")
        
        # Test model operations
        print("🧪 Testing model operations...")
        
        # Test UploadTask creation
        import uuid
        task = UploadTask.objects.create(
            task_id=uuid.uuid4(),
            filename="test.csv",
            file_size=1024,
            status="QUEUED"
        )
        print(f"✅ UploadTask created: {task.task_id}")
        
        # Clean up
        task.delete()
        print("✅ Test cleanup completed")
        
    except Exception as e:
        print(f"❌ Database error: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        
        # Try to create tables manually
        print("🔧 Attempting to create tables manually...")
        try:
            with connection.cursor() as cursor:
                with open('create_tables.sql', 'r') as f:
                    sql_commands = f.read().split(';')
                    for command in sql_commands:
                        if command.strip():
                            cursor.execute(command)
            print("✅ Tables created manually")
        except Exception as manual_error:
            print(f"❌ Manual table creation failed: {str(manual_error)}")

if __name__ == "__main__":
    test_database()
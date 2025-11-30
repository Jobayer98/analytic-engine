from django.db import connection
from django.utils.deprecation import MiddlewareMixin
import threading

class QueryCountMiddleware(MiddlewareMixin):
    """Middleware to track database query count per request/task"""
    
    def __init__(self, get_response):
        self.get_response = get_response
        self.local = threading.local()
        super().__init__(get_response)
    
    def process_request(self, request):
        self.local.query_count = len(connection.queries)
        return None
    
    def process_response(self, request, response):
        if hasattr(self.local, 'query_count'):
            current_queries = len(connection.queries)
            request.db_query_count = current_queries - self.local.query_count
        return response
    
    @classmethod
    def get_query_count(cls):
        """Get current query count for background tasks"""
        return len(connection.queries)
from django.contrib import admin
from django.urls import path, include
from ninja import NinjaAPI
from apps.uploads.api import router as uploads_router
from apps.analytics.api import router as analytics_router

api = NinjaAPI(title="Analytics Engine API", version="1.0.0")

# Include app APIs
api.add_router("/ingest", uploads_router)
api.add_router("/analytics", analytics_router)

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/", api.urls),
]


from input.serializers import AnaSummarySerializer
from rest_framework import viewsets, filters,routers
from .models import AnaSummary


class AnaSummaryViewSet(viewsets.ModelViewSet):
    queryset = AnaSummary.objects.all()
    serializer_class = AnaSummarySerializer


router = routers.DefaultRouter()
router.register(r'articles', AnaSummaryViewSet)

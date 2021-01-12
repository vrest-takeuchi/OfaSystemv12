"""djangoProject2 URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from input.views import AnaSummaryViewSet
from input import views
from input import cal
from rest_framework import routers
from django.conf.urls import url, include


router = routers.DefaultRouter()
router.register(r'anasummary', AnaSummaryViewSet)

urlpatterns = [
    path('admin/', admin.site.urls),
    url(r'^api/', include(router.urls)),
    path('ana_data/<int:param_equip_id>/<int:year>/<int:month>/<int:day>/<int:hour>/<int:minute>/<int:second>/',views.ana_data,name='ana_data'),
]

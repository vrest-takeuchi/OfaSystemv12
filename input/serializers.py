from rest_framework import serializers
from output.models import AnaSummary


class AnaSummarySerializer(serializers.ModelSerializer):

    class Meta:
        model = AnaSummary
        fields = '__all__'

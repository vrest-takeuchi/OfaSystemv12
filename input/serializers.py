from rest_framework import serializers
from output.models import AnaSummary,OffpointDetail,CategoryDetail

class CategoryDetailSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(
        required=False,
    )
    name = serializers.CharField(
        required=False,
    )

    class Meta(object):
        model = CategoryDetail
        fields = (
            'id',
            'name',
            'detail',
        )


class OffpointDetailSerializer(serializers.ModelSerializer):

    category_name =serializers.SerializerMethodField()
    category_detail=serializers.SerializerMethodField()
    offpoint_category_name = serializers.SerializerMethodField()
    class Meta:
        model = OffpointDetail
        fields = ['measurement_date','offpoint','offpoint_category','offpoint_category_name','category','category_name','category_detail','evaluation_place','block_no']
    def get_category_name(self, obj):
        try:
            comment_abstruct_contents = CategoryDetail.objects.values_list('name', flat=True).get(id=obj.category_id)
            return comment_abstruct_contents
        except:
            comment_abstruct_contents = None
            return comment_abstruct_contents
    def get_category_detail(self, obj):
        try:
            comment_abstruct_contents = CategoryDetail.objects.values_list('detail', flat=True).get(id=obj.category_id)
            return comment_abstruct_contents
        except:
            comment_abstruct_contents = None
            return comment_abstruct_contents
    def get_offpoint_category_name(self, obj):
        try:
            comment_abstruct_contents = CategoryDetail.objects.values_list('name', flat=True).get(id=obj.offpoint_category)
            return comment_abstruct_contents
        except:
            comment_abstruct_contents = None
            return comment_abstruct_contents

class AnaSummarySerializer(serializers.ModelSerializer):
    # employee_set = EmployeeSerializer(many=True)
    detail = OffpointDetailSerializer(many=True)
    class Meta:
        model = AnaSummary
        fields = ['id', 'equip_id','run_start_date', 'result', 'total_offpoint','comment','detail',]

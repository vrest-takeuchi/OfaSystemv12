from rest_framework import serializers
from output.models import AnaSummary,OffpointDetail,CategoryDetail,DrivingEvaluationItemTbl,DrivingEvaluationByBlockTbl

class CategoryDetailSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required=False)
    name = serializers.CharField(required=False)

    class Meta(object):
        model = CategoryDetail
        fields = (
            'id',
            'name',
            'detail',
        )
class DrivingEvaluationItemTblSerializer(serializers.ModelSerializer):
    evaluation_cd = serializers.IntegerField(required=False)
    evaluation_item_name = serializers.IntegerField(required=False)
    evaluation_message = serializers.IntegerField(required=False)

    class Meta(object):
        model = CategoryDetail
        fields = (
            'evaluation_cd',
            'evaluation_item_name',
            'evaluation_message',
        )

class OffpointDetailSerializer(serializers.ModelSerializer):

    category_name =serializers.SerializerMethodField()
    category_detail=serializers.SerializerMethodField()
    offpoint_category_name = serializers.SerializerMethodField()
    evaluation_place = serializers.SerializerMethodField()

    class Meta:
        model = OffpointDetail
        fields = ['measurement_date','offpoint','offpoint_category','offpoint_category_name','category','category_name','category_detail','evaluation_place','block_no']

    def get_category_name(self, obj):
        try:
            comment_abstruct_contents = DrivingEvaluationItemTbl.objects.values_list('evaluation_item_name',flat=True).get(evaluation_cd=obj.category_id)
            return comment_abstruct_contents
        except:
            comment_abstruct_contents = None
            return comment_abstruct_contents

    def get_category_detail(self, obj):
        try:
            comment_abstruct_contents = DrivingEvaluationItemTbl.objects.values_list('evaluation_message', flat=True).get(evaluation_cd=obj.category_id)
            return comment_abstruct_contents
        except:
            comment_abstruct_contents = None
            return comment_abstruct_contents
    def get_offpoint_category_name(self, obj):
        try:

            comment_abstruct_contents = CategoryDetail.objects.values_list('name', flat=True).get(id=obj.category_id)
            return comment_abstruct_contents
        except:
            comment_abstruct_contents = None
            return comment_abstruct_contents
    def get_evaluation_place(self, obj):
        try:
            comment_abstruct_contents = DrivingEvaluationByBlockTbl.objects.values_list('block_cd', flat=True).filter(evaluation_cd=obj.category_id)
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

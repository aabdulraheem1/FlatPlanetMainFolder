from rest_framework import serializers

class ImportSerializers(serializers.Serializer):
    file = serializers.FileField()
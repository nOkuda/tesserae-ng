from haystack import indexes
from models import SourceText

class NoteIndex(indexes.SearchIndex, indexes.Indexable):
    text = indexes.CharField(document=True, use_template=False)
    author = indexes.CharField(model_attr='author')
    title = indexes.CharField(model_attr='title')
    online_source_name = indexes.CharField(model_attr='online_source_name')
    online_source_url = indexes.CharField(model_attr='online_source_link')
    print_source_name = indexes.CharField(model_attr='print_source_name')
    print_source_url = indexes.CharField(model_attr='print_source_link')
    enabled = indexes.BooleanField(model_attr='enabled')

    def get_model(self):
        return SourceText

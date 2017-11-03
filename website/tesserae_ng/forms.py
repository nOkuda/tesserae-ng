from django import forms
import logging
from website.tesserae_ng.models import SourceTextVolume
from website.tesserae_ng.parse_tess import TESS_MODES


logger = logging.getLogger(__name__)


class SourceTextSubmitForm(forms.Form):

    LANGUAGE_CHOICES = (
        ('latin', 'Latin'),
        ('greek', 'Greek'),
        ('english', 'English')
    )

    def _boundText(auto_source, auto_query, auto_value, input_value, source_value):
        """
        Example:
            _boundText('myPeople', 'getPeople', 'mySelectedGuid', 'name', 'guid')
        """
        bind_text = "jqAuto: { autoFocus: true }, jqAutoSource: " + auto_source + ", jqAutoQuery: " + \
            auto_query + ", jqAutoValue: " + auto_value + ", jqAutoSourceLabel: 'displayName', " + \
            "jqAutoSourceInputValue: '" + input_value + "', jqAutoSourceValue: '" + source_value + "'"

        return forms.TextInput(attrs={'data-bind':bind_text})

    enabled = forms.BooleanField(label='Indexed', required=True, initial=True)
    language = forms.ChoiceField(label='Text language', choices=LANGUAGE_CHOICES, required=True)
    author = forms.CharField(label='Work author', max_length=255, required=True,
                             widget=_boundText('authors', 'getAuthors', 'selectedAuthor', 'name', 'name'))
    title = forms.CharField(label='Work name', max_length=255, required=True,
                            widget=_boundText('titles', 'getTitles', 'selectedTitle', 'title', 'title'))
    volume = forms.CharField(label='Volume name', max_length=255, required=False)

    online_source_name = forms.CharField(label='Online source name', max_length=255, required=False)
    online_source_link = forms.URLField(label='Online source URL', required=False)
    print_source_name = forms.CharField(label='Print source name', max_length=255, required=False)
    print_source_link = forms.URLField(label='Print source URL', required=False)

    source_file = forms.FileField(allow_empty_file=False, required=True, label='Source file')


class STVChoiceField(forms.ModelChoiceField):

    def label_from_instance(self, obj):
        return obj.source.title + " (" + obj.volume + ")"


class SimpleSearchForm(forms.Form):

    source = STVChoiceField(queryset=SourceTextVolume.objects, empty_label="Choose a source text")
    target = STVChoiceField(queryset=SourceTextVolume.objects, empty_label="Choose a target text")
    start = forms.IntegerField(initial=0, min_value=0, widget=forms.widgets.HiddenInput())
    rows = forms.IntegerField(initial=50, min_value=1, widget=forms.widgets.HiddenInput())
    sw = forms.CharField(min_length=0, max_length=10000, required=False, widget=forms.widgets.HiddenInput())


PARSE_TYPES = tuple([(k, k) for k in sorted(TESS_MODES.keys())])


class AdvancedSearchForm(forms.Form):
    """Template for advanced search form"""
    source = STVChoiceField(queryset=SourceTextVolume.objects, empty_label="Choose a source text")
    source_parse_unit = forms.ChoiceField(PARSE_TYPES)
    target = STVChoiceField(queryset=SourceTextVolume.objects, empty_label="Choose a target text")
    target_parse_unit = forms.ChoiceField(PARSE_TYPES)
    start = forms.IntegerField(initial=0, min_value=0, widget=forms.widgets.HiddenInput())
    rows = forms.IntegerField(initial=50, min_value=1, widget=forms.widgets.HiddenInput())
    sw = forms.CharField(min_length=0, max_length=10000, required=False, widget=forms.widgets.HiddenInput())
    stopwords_count = forms.IntegerField(initial=10, min_value=0)
    cutoff = forms.FloatField(initial=0.0)

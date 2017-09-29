# -*- coding: utf-8 -*-
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Adding field 'SourceTextSentence.parse_type'
        db.add_column(u'tesserae_ng_sourcetextsentence', 'parse_type',
                      self.gf('django.db.models.fields.CharField')(default='line', max_length=255),
                      keep_default=False)


    def backwards(self, orm):
        # Deleting field 'SourceTextSentence.parse_type'
        db.delete_column(u'tesserae_ng_sourcetextsentence', 'parse_type')


    models = {
        u'tesserae_ng.sourcetext': {
            'Meta': {'object_name': 'SourceText'},
            'author': ('django.db.models.fields.CharField', [], {'max_length': '255'}),
            'enabled': ('django.db.models.fields.BooleanField', [], {'default': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'language': ('django.db.models.fields.CharField', [], {'max_length': '20'}),
            'online_source_link': ('django.db.models.fields.URLField', [], {'max_length': '4096', 'null': 'True', 'blank': 'True'}),
            'online_source_name': ('django.db.models.fields.CharField', [], {'max_length': '4096', 'null': 'True', 'blank': 'True'}),
            'print_source_link': ('django.db.models.fields.URLField', [], {'max_length': '4096', 'null': 'True', 'blank': 'True'}),
            'print_source_name': ('django.db.models.fields.CharField', [], {'max_length': '4096', 'null': 'True', 'blank': 'True'}),
            'title': ('django.db.models.fields.CharField', [], {'max_length': '255'})
        },
        u'tesserae_ng.sourcetextsentence': {
            'Meta': {'object_name': 'SourceTextSentence'},
            'end_line': ('django.db.models.fields.CharField', [], {'max_length': '255'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'parse_type': ('django.db.models.fields.CharField', [], {'max_length': '255'}),
            'sentence': ('django.db.models.fields.TextField', [], {}),
            'start_line': ('django.db.models.fields.CharField', [], {'max_length': '255'}),
            'volume': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['tesserae_ng.SourceTextVolume']", 'on_delete': 'models.PROTECT'})
        },
        u'tesserae_ng.sourcetextvolume': {
            'Meta': {'object_name': 'SourceTextVolume'},
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'source': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['tesserae_ng.SourceText']", 'on_delete': 'models.PROTECT'}),
            'text': ('django.db.models.fields.TextField', [], {}),
            'volume': ('django.db.models.fields.CharField', [], {'max_length': '255'})
        }
    }

    complete_apps = ['tesserae_ng']